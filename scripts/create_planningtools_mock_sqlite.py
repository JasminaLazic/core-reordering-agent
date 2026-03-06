"""
Create a local PlanningTools mock SQLite DB with schema-aligned tables.

The script reads table DDL from ProductTools/PlanningToolsDB and creates a SQLite
database with the same column names for the core-ordering tables used by this app.
"""
from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass
class ColumnDef:
    name: str
    sqlite_type: str
    not_null: bool


TABLE_SPECS: List[Tuple[str, str, str]] = [
    ("bicache", "tbl_Item", "bicache/Tables/tbl_Item.sql"),
    ("bicache", "tbl_CentralWarehouse", "bicache/Tables/tbl_CentralWarehouse.sql"),
    ("model", "tbl_StockWarehouseOnHand", "model/Tables/tbl_StockWarehouseOnHand.sql"),
    ("model", "tbl_CoreAssortment", "model/Tables/tbl_CoreAssortment.sql"),
    ("model", "tbl_StoreWarehouseRelationship", "model/Tables/tbl_StoreWarehouseRelationship.sql"),
    ("fpo", "tbl_ForecastStoreSales", "fpo/Tables/tbl_ForecastStoreSales.sql"),
    ("fpo", "tbl_ItemWarehouse", "fpo/Tables/tbl_ItemWarehouse.sql"),
    ("fpo", "tbl_ItemWarehouseOrderQty", "fpo/Tables/tbl_ItemWarehouseOrderQty.sql"),
    ("fpo", "tbl_ImportCoverConfig", "fpo/Tables/tbl_ImportCoverConfig.sql"),
    ("am", "tbl_JobControl", "am/Tables/tbl_JobControl.sql"),
    ("am", "tbl_JobControlHistory", "am/Tables/tbl_JobControlHistory.sql"),
]


def _map_type_to_sqlite(raw_type: str) -> str:
    t = raw_type.upper()
    if "INT" in t or "BIT" in t:
        return "INTEGER"
    if any(k in t for k in ["DECIMAL", "NUMERIC", "REAL", "FLOAT", "MONEY"]):
        return "REAL"
    if any(k in t for k in ["DATE", "TIME", "DATETIME"]):
        return "TEXT"
    if any(k in t for k in ["CHAR", "TEXT", "VARCHAR", "NCHAR", "NVARCHAR", "UNIQUEIDENTIFIER"]):
        return "TEXT"
    return "TEXT"


def _extract_create_block(sql_text: str, schema: str, table: str) -> str:
    pattern = rf"CREATE TABLE \[{re.escape(schema)}\]\.\[{re.escape(table)}\]\s*\((.*?)\);\s*GO"
    m = re.search(pattern, sql_text, re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError(f"Could not parse CREATE TABLE block for {schema}.{table}")
    return m.group(1)


def _extract_columns(create_block: str) -> List[ColumnDef]:
    cols: List[ColumnDef] = []
    for raw_line in create_block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line.startswith("["):
            continue
        m = re.match(r"^\[([^\]]+)\]\s+(.+)$", line)
        if not m:
            continue
        col_name = m.group(1)
        remainder = m.group(2).strip()

        # Computed columns (SQL Server AS (...)) are materialized as TEXT in sqlite.
        if remainder.upper().startswith("AS"):
            cols.append(ColumnDef(name=col_name, sqlite_type="TEXT", not_null=False))
            continue

        type_match = re.match(r"^([A-Z]+(?:\s+[A-Z]+)?(?:\s*\([^)]*\))?)", remainder, re.IGNORECASE)
        raw_type = type_match.group(1) if type_match else "TEXT"
        not_null = "NOT NULL" in remainder.upper()
        cols.append(ColumnDef(name=col_name, sqlite_type=_map_type_to_sqlite(raw_type), not_null=not_null))
    return cols


def _extract_primary_key(sql_text: str, schema: str, table: str) -> List[str]:
    pattern = (
        rf"(?:ALTER TABLE \[{re.escape(schema)}\]\.\[{re.escape(table)}\].*?PRIMARY KEY CLUSTERED"
        rf"\s*\((.*?)\)|CREATE TABLE \[{re.escape(schema)}\]\.\[{re.escape(table)}\].*?PRIMARY KEY CLUSTERED"
        rf"\s*\((.*?)\))"
    )
    m = re.search(pattern, sql_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    cols_blob = m.group(1) or m.group(2) or ""
    return re.findall(r"\[([^\]]+)\]", cols_blob)


def _sqlite_table_name(schema: str, table: str) -> str:
    return f"{schema}_{table}"


def _default_value(col: ColumnDef):
    if not col.not_null:
        return None
    if col.sqlite_type == "INTEGER":
        return 0
    if col.sqlite_type == "REAL":
        return 0.0
    return ""


def _create_sqlite_table(conn: sqlite3.Connection, table_name: str, cols: List[ColumnDef], pk_cols: List[str]) -> None:
    col_defs = []
    for c in cols:
        nn = " NOT NULL" if c.not_null else ""
        col_defs.append(f"\"{c.name}\" {c.sqlite_type}{nn}")
    if pk_cols:
        pk = ", ".join([f"\"{c}\"" for c in pk_cols])
        col_defs.append(f"PRIMARY KEY ({pk})")
    ddl = f"CREATE TABLE \"{table_name}\" ({', '.join(col_defs)})"
    conn.execute(ddl)


def _insert_rows(
    conn: sqlite3.Connection,
    table_name: str,
    cols: List[ColumnDef],
    overrides: List[Dict[str, object]],
) -> None:
    col_names = [c.name for c in cols]
    placeholders = ", ".join(["?"] * len(col_names))
    quoted_cols = ", ".join([f'"{n}"' for n in col_names])
    sql = f"INSERT INTO \"{table_name}\" ({quoted_cols}) VALUES ({placeholders})"

    for override in overrides:
        row = []
        for c in cols:
            row.append(override.get(c.name, _default_value(c)))
        conn.execute(sql, row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--planningtools-root",
        default=str(Path.home() / "ProductTools" / "PlanningToolsDB"),
        help="Path to ProductTools PlanningToolsDB folder",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "data" / "planningtools_mock.sqlite"),
        help="Output sqlite path",
    )
    args = parser.parse_args()

    planningtools_root = Path(args.planningtools_root).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    schema_defs: Dict[str, List[ColumnDef]] = {}
    with sqlite3.connect(output_path) as conn:
        for schema, table, rel_path in TABLE_SPECS:
            ddl_file = planningtools_root / rel_path
            sql_text = ddl_file.read_text(encoding="utf-8")
            create_block = _extract_create_block(sql_text, schema, table)
            cols = _extract_columns(create_block)
            pk_cols = _extract_primary_key(sql_text, schema, table)
            sqlite_table = _sqlite_table_name(schema, table)
            _create_sqlite_table(conn, sqlite_table, cols, pk_cols)
            schema_defs[f"{schema}.{table}"] = cols

        now = datetime.utcnow().replace(microsecond=0)
        this_week = int(now.strftime("%W")) + 1

        _insert_rows(
            conn,
            "bicache_tbl_CentralWarehouse",
            schema_defs["bicache.tbl_CentralWarehouse"],
            [
                {
                    "CentralWarehouseKey": 100,
                    "CentralWarehouseCode": "W01",
                    "CentralWarehouseName": "Main Warehouse",
                    "CountryKey": 1,
                    "DataTimestamp": now.isoformat(sep=" "),
                }
            ],
        )

        _insert_rows(
            conn,
            "bicache_tbl_Item",
            schema_defs["bicache.tbl_Item"],
            [
                {
                    "ItemKey": 1001,
                    "ItemNumber": "123456",
                    "ItemName": "Mock Toothbrush",
                    "SupplierKey": 501,
                    "SizeStoreParcelVolumeInCm": 1.0,
                    "CategoryMainLevel1": "Health",
                    "CategoryMainLevel2": "Dental",
                    "MOQ": 48,
                    "IsCalloff": 0,
                    "IsFood": 0,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
                {
                    "ItemKey": 1002,
                    "ItemNumber": "223344",
                    "ItemName": "Mock Shampoo",
                    "SupplierKey": 502,
                    "SizeStoreParcelVolumeInCm": 1.0,
                    "CategoryMainLevel1": "Beauty",
                    "CategoryMainLevel2": "Hair",
                    "MOQ": 24,
                    "IsCalloff": 0,
                    "IsFood": 0,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
            ],
        )

        _insert_rows(
            conn,
            "model_tbl_CoreAssortment",
            schema_defs["model.tbl_CoreAssortment"],
            [
                {
                    "ItemKey": 1001,
                    "ItemNumber": "123456",
                    "CategoryMainLevel1": "Health",
                    "IsFood": 0,
                    "CentralWarehouseCode": "W01",
                    "CentralWarehouseKey": 100,
                    "ToDate": (now + timedelta(days=180)).date().isoformat(),
                    "ReqFC": 1,
                    "ReqPO": 1,
                    "HasStock": 1,
                    "HasPO": 1,
                    "HasReplacement": 0,
                    "HasActiveReplacement": 0,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
                {
                    "ItemKey": 1002,
                    "ItemNumber": "223344",
                    "CategoryMainLevel1": "Beauty",
                    "IsFood": 0,
                    "CentralWarehouseCode": "W01",
                    "CentralWarehouseKey": 100,
                    "ToDate": (now + timedelta(days=180)).date().isoformat(),
                    "ReqFC": 1,
                    "ReqPO": 1,
                    "HasStock": 1,
                    "HasPO": 0,
                    "HasReplacement": 0,
                    "HasActiveReplacement": 0,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
            ],
        )

        _insert_rows(
            conn,
            "model_tbl_StockWarehouseOnHand",
            schema_defs["model.tbl_StockWarehouseOnHand"],
            [
                {
                    "ItemKeyNew": 1001,
                    "ItemKeyOld": 1001,
                    "CentralWarehouseKey": 100,
                    "Quantity": 120,
                    "QuantityOrdered": 0,
                    "QuantityReservedForCampaign": 0,
                    "QuntityInPurchase": 36,
                    "StockOnHand": 120,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
                {
                    "ItemKeyNew": 1002,
                    "ItemKeyOld": 1002,
                    "CentralWarehouseKey": 100,
                    "Quantity": 20,
                    "QuantityOrdered": 0,
                    "QuantityReservedForCampaign": 0,
                    "QuntityInPurchase": 0,
                    "StockOnHand": 20,
                    "DataTimestamp": now.isoformat(sep=" "),
                },
            ],
        )

        _insert_rows(
            conn,
            "model_tbl_StoreWarehouseRelationship",
            schema_defs["model.tbl_StoreWarehouseRelationship"],
            [
                {
                    "StoreKey": 2001,
                    "PartnerKey": 1,
                    "CountryKey": 1,
                    "FromDate": (now - timedelta(days=365)).date().isoformat(),
                    "ToDate": None,
                    "CentralWarehouseKey": 100,
                    "Channel": 1,
                    "DataTimestamp": now.isoformat(sep=" "),
                }
            ],
        )

        # Forecast table has many week columns; fill wk01..wk53 with defaults and override first 4.
        forecast_cols = schema_defs["fpo.tbl_ForecastStoreSales"]
        forecast_row_1: Dict[str, object] = {
            "ItemKey": 1001,
            "StoreKey": 2001,
            "CentralWarehouseKey": 100,
            "ForecastTotal": 180.0,
            "ForecastWk01": 40.0,
            "ForecastWk02": 42.0,
            "ForecastWk03": 46.0,
            "ForecastWk04": 52.0,
        }
        forecast_row_2: Dict[str, object] = {
            "ItemKey": 1002,
            "StoreKey": 2001,
            "CentralWarehouseKey": 100,
            "ForecastTotal": 96.0,
            "ForecastWk01": 24.0,
            "ForecastWk02": 24.0,
            "ForecastWk03": 24.0,
            "ForecastWk04": 24.0,
        }
        for c in forecast_cols:
            if c.name.startswith("ForecastWk") and c.name not in forecast_row_1:
                forecast_row_1[c.name] = 0.0
                forecast_row_2[c.name] = 0.0
            if c.name.startswith("CoverQtyWk"):
                forecast_row_1[c.name] = None
                forecast_row_2[c.name] = None
        _insert_rows(conn, "fpo_tbl_ForecastStoreSales", forecast_cols, [forecast_row_1, forecast_row_2])

        _insert_rows(
            conn,
            "fpo_tbl_ItemWarehouse",
            schema_defs["fpo.tbl_ItemWarehouse"],
            [
                {
                    "ItemKey": 1001,
                    "CentralWarehouseKey": 100,
                    "CategoryABC": "A1",
                    "SafetyStockQty": 30,
                    "ShipLT": 14,
                    "TotalLT": 21,
                },
                {
                    "ItemKey": 1002,
                    "CentralWarehouseKey": 100,
                    "CategoryABC": "B1",
                    "SafetyStockQty": 20,
                    "ShipLT": 14,
                    "TotalLT": 21,
                },
            ],
        )

        _insert_rows(
            conn,
            "fpo_tbl_ItemWarehouseOrderQty",
            schema_defs["fpo.tbl_ItemWarehouseOrderQty"],
            [
                {"ItemKey": 1001, "CentralWarehouseKey": 100, "OrderQtyType": "A", "AOQ": 12, "EOQ": 48, "LOQ": 48, "SOQ": 12},
                {"ItemKey": 1002, "CentralWarehouseKey": 100, "OrderQtyType": "A", "AOQ": 6, "EOQ": 24, "LOQ": 24, "SOQ": 6},
            ],
        )

        _insert_rows(
            conn,
            "fpo_tbl_ImportCoverConfig",
            schema_defs["fpo.tbl_ImportCoverConfig"],
            [
                {
                    "CentralWarehouseCode": "W01",
                    "CategoryABC": "A1",
                    "WeekOfYear": this_week,
                    "NoOfWeeksCoverWarehouseOrder": 4,
                    "NoOfWeeksCoverStore": 2,
                    "ModifiedBy": "mock",
                    "ModifiedDate": now.isoformat(sep=" "),
                },
                {
                    "CentralWarehouseCode": "W01",
                    "CategoryABC": "B1",
                    "WeekOfYear": this_week,
                    "NoOfWeeksCoverWarehouseOrder": 4,
                    "NoOfWeeksCoverStore": 2,
                    "ModifiedBy": "mock",
                    "ModifiedDate": now.isoformat(sep=" "),
                },
            ],
        )

        _insert_rows(
            conn,
            "am_tbl_JobControl",
            schema_defs["am.tbl_JobControl"],
            [
                {
                    "JobSequence": 1,
                    "JobName": "fpo.usp_ReorderPlanning",
                    "ProcParams": "",
                    "LastRunStart": (now - timedelta(minutes=15)).isoformat(sep=" "),
                    "LastRunEnd": (now - timedelta(minutes=12)).isoformat(sep=" "),
                    "LastRunErrorMessage": "",
                    "LastRunByUser": "mock_user",
                    "LastRunByServer": "mock_server",
                }
            ],
        )

        _insert_rows(
            conn,
            "am_tbl_JobControlHistory",
            schema_defs["am.tbl_JobControlHistory"],
            [
                {
                    "JobName": "fpo.usp_ReorderPlanning",
                    "ProcParams": "",
                    "JobStart": (now - timedelta(minutes=15)).isoformat(sep=" "),
                    "JobEnd": (now - timedelta(minutes=12)).isoformat(sep=" "),
                    "JobErrorMessage": "",
                    "JobRunByUser": "mock_user",
                    "JobRunByServer": "mock_server",
                }
            ],
        )

        conn.commit()

    print(f"Created mock sqlite DB: {output_path}")
    print("Set env var to use it:")
    print(f"PLANNING_TOOLS_SQLITE_PATH={output_path}")


if __name__ == "__main__":
    main()

