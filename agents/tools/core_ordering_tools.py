"""
Core re-ordering tools for PlanningToolsDB.

Agent tools (registered with the agent):
  get_item_ordering_data  — batch fetch of raw tables for a single item/warehouse
  get_fpo_source_table    — whitelist-based lookup of any FPO raw source table

API-only (used by api.py endpoints, not registered with the agent):
  validate_proposal
"""
import os
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def _get_pyodbc():
    try:
        import pyodbc  # type: ignore
        return pyodbc
    except Exception as e:
        raise RuntimeError(
            "pyodbc is required for PlanningToolsDB access. "
            "Install with: pip install pyodbc"
        ) from e


def _build_connection_string() -> str:
    server   = os.environ.get("PLANNING_TOOLS_SQL_SERVER")
    database = os.environ.get("PLANNING_TOOLS_SQL_DATABASE", "PlanningTools")
    username = os.environ.get("PLANNING_TOOLS_SQL_USERNAME")
    password = os.environ.get("PLANNING_TOOLS_SQL_PASSWORD")
    driver   = os.environ.get("PLANNING_TOOLS_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

    if not server:
        raise RuntimeError(
            "Missing PlanningToolsDB SQL config. Set "
            "PLANNING_TOOLS_SQL_SERVER (+ optional auth variables)."
        )
    if username and password:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )


def _query_sqlserver(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    pyodbc = _get_pyodbc()
    conn_str = _build_connection_string()
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        if cur.description is None:
            return []
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    result: List[Dict[str, Any]] = []
    for r in rows:
        row: Dict[str, Any] = {}
        for i, c in enumerate(cols):
            value = r[i]
            if isinstance(value, (datetime, date)):
                row[c] = value.isoformat()
            elif isinstance(value, Decimal):
                row[c] = float(value)
            elif isinstance(value, bytes):
                try:
                    row[c] = value.decode("utf-8")
                except Exception:
                    row[c] = value.hex()
            else:
                row[c] = value
        result.append(row)
    return result


def _query_safe(
    sql: str, params: Optional[List[Any]] = None
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        return _query_sqlserver(sql, params), None
    except Exception as e:
        return [], str(e)


def _query_item_wh(
    table: str,
    item_key: int,
    wh_key: Optional[int],
    select: str = "*",
) -> List[Dict[str, Any]]:
    """SELECT {select} FROM {table} WHERE ItemKey=? [AND CentralWarehouseKey=?]."""
    if wh_key is not None:
        rows, _ = _query_safe(
            f"SELECT {select} FROM {table} WHERE ItemKey = ? AND CentralWarehouseKey = ?",
            [item_key, wh_key],
        )
    else:
        rows, _ = _query_safe(
            f"SELECT {select} FROM {table} WHERE ItemKey = ?",
            [item_key],
        )
    return rows


def _normalize_top_n(top_n: int, max_n: int = 500) -> int:
    return max(1, min(int(top_n), max_n))


# ---------------------------------------------------------------------------
# Timeline helper
# ---------------------------------------------------------------------------

def _get_current_calc_week_no() -> int:
    """CalcWeekNo for the week containing today. Returns 1 if not found."""
    rows, err = _query_safe(
        "SELECT TOP 1 CalcWeekNo FROM fpo.tbl_CalcTimelineWeek "
        "WHERE CAST(GETDATE() AS DATE) BETWEEN WeekStartDate AND WeekEndDate"
    )
    if err or not rows:
        return 1
    return int(rows[0]["CalcWeekNo"])



# ---------------------------------------------------------------------------
# Weekly aggregation helper
# ---------------------------------------------------------------------------

def _aggregate_weekly_series(
    table: str,
    col_prefix: str,
    item_key: int,
    wh_key: Optional[int],
    value_key: str = "value",
    n_weeks: int = 53,
    positive_only: bool = False,
) -> List[Dict[str, Any]]:
    """
    SELECT SUM(<col_prefix>Wk01)..SUM(<col_prefix>Wk53) FROM <table>
    WHERE ItemKey=? [AND CentralWarehouseKey=?]

    positive_only=True mirrors tvf_StockProjection behaviour:
      SUM(CASE WHEN col > 0 THEN col ELSE 0 END)

    Returns a list of {week_index: n, <value_key>: total} for weeks 1..n_weeks,
    treating NULL as 0.
    """
    if positive_only:
        sums = ", ".join(
            f"COALESCE(SUM(CASE WHEN {col_prefix}Wk{n:02d} > 0 THEN {col_prefix}Wk{n:02d} ELSE 0 END), 0) AS Wk{n:02d}"
            for n in range(1, n_weeks + 1)
        )
    else:
        sums = ", ".join(
            f"COALESCE(SUM({col_prefix}Wk{n:02d}), 0) AS Wk{n:02d}"
            for n in range(1, n_weeks + 1)
        )
    if wh_key is not None:
        sql = f"SELECT {sums} FROM {table} WHERE ItemKey = ? AND CentralWarehouseKey = ?"
        params: List[Any] = [item_key, wh_key]
    else:
        sql = f"SELECT {sums} FROM {table} WHERE ItemKey = ?"
        params = [item_key]
    rows, err = _query_safe(sql, params)
    if err or not rows:
        return []
    row = rows[0]
    return [
        {"week_index": n, value_key: row.get(f"Wk{n:02d}", 0) or 0}
        for n in range(1, n_weeks + 1)
    ]


def _week_cols(prefix: str, start: int = 1, end: int = 53) -> str:
    return ", ".join(f"{prefix}Wk{n:02d}" for n in range(start, end + 1))


# ---------------------------------------------------------------------------
# AGENT TOOL 1: get_item_ordering_data
# ---------------------------------------------------------------------------

def get_item_ordering_data(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
    include_all_warehouses: bool = False,
) -> Dict[str, Any]:
    """
    Fetch raw tables needed for a core reorder decision in ONE call.

    Returns item master, warehouse config, raw forecast/demand/stock tables,
    cover configs, leadtime calendar, and a 53-week timeline.
    The agent is responsible for all aggregation and reorder logic.

    include_all_warehouses=True: also return a multi_warehouse_summary and warehouse
    master for every warehouse that carries the item (ignored when central_warehouse_code
    is supplied).
    """
    if not (item_number or "").strip():
        return {"status": "error", "message": "item_number required", "tables": {}}

    rows, err = _query_safe(
        "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
        [item_number.strip()],
    )
    if err:
        return {"status": "error", "message": f"item lookup: {err}", "tables": {}}
    if not rows:
        return {"status": "no_item", "message": f"Item '{item_number}' not found", "tables": {}}
    item_key = int(rows[0]["ItemKey"])

    tables: Dict[str, List[Dict[str, Any]]] = {}

    # Resolve warehouse key
    wh_key: Optional[int] = None
    if central_warehouse_code:
        wh_rows, _ = _query_safe(
            "SELECT CentralWarehouseKey FROM bicache.tbl_CentralWarehouse WHERE CentralWarehouseCode = ?",
            [central_warehouse_code.strip()],
        )
        if wh_rows:
            wh_key = int(wh_rows[0]["CentralWarehouseKey"])

    # Item master
    tables["bicache_tbl_Item"], _ = _query_safe(
        "SELECT ItemKey, ItemNumber, ItemName, "
        "COALESCE(NumberOfUnitsPerParcelForStore,0) AS StoreCartonSize, "
        "COALESCE(NumberOfUnitsPerParcelWhenPurchase,0) AS SupplierCartonSize, "
        "COALESCE(NumberOfUnitsPerPallet,0) AS PalletSize, COALESCE(MOQ,0) AS MOQ, "
        "CountryOriginCountryKey FROM bicache.tbl_Item WHERE ItemKey = ?",
        [item_key],
    )

    # Warehouse master
    if include_all_warehouses and not central_warehouse_code:
        tables["bicache_tbl_CentralWarehouse"], _ = _query_safe(
            "SELECT DISTINCT cw.CentralWarehouseKey, cw.CentralWarehouseCode, cw.CentralWarehouseName "
            "FROM bicache.tbl_CentralWarehouse cw "
            "INNER JOIN fpo.tbl_ItemWarehouse iw ON iw.CentralWarehouseKey = cw.CentralWarehouseKey "
            "WHERE iw.ItemKey = ?",
            [item_key],
        )
    elif wh_key is not None:
        tables["bicache_tbl_CentralWarehouse"], _ = _query_safe(
            "SELECT CentralWarehouseKey, CentralWarehouseCode, CentralWarehouseName "
            "FROM bicache.tbl_CentralWarehouse WHERE CentralWarehouseKey = ?",
            [wh_key],
        )

    # ItemWarehouse config (ABC class, SafetyStockQty, ReqPO, ShipLT, TotalLT)
    tables["fpo_tbl_ItemWarehouse"] = _query_item_wh(
        "fpo.tbl_ItemWarehouse", item_key, wh_key
    )

    # ItemWarehouseOrderQty (AOQ, EOQ, LOQ, SOQ, carton/pallet sizes)
    tables["fpo_tbl_ItemWarehouseOrderQty"] = _query_item_wh(
        "fpo.tbl_ItemWarehouseOrderQty", item_key, wh_key
    )

    # CalcWarehouseStock — keep warehouse opening state only; existing POs remain excluded
    tables["fpo_tbl_CalcWarehouseStock"] = _query_item_wh(
        "fpo.tbl_CalcWarehouseStock", item_key, wh_key,
        select="ItemKey, CentralWarehouseKey, ReqPO, SafetyStockQty, CloseStockWk00, "
               "CalcIterationNo, FinalisedCalcWeekNo",
    )

    # Expose per-store raw calculation inputs needed for ProductTools-style S2 logic.
    # Existing warehouse orders are still intentionally excluded from the agent's stock path,
    # but the agent can now inspect store-level CloseStock/Demand/StockIn/InTransit plus
    # Forecast/CoverQty when it needs to reason more faithfully about store allocation.
    calc_store_select = ", ".join([
        "ItemKey",
        "StoreKey",
        "CentralWarehouseKey",
        "CartonSize",
        "CloseStockWk00",
        _week_cols("CloseStock"),
        _week_cols("Demand"),
        _week_cols("StockIn"),
        _week_cols("InTransit"),
    ])
    forecast_store_select = ", ".join([
        "ItemKey",
        "StoreKey",
        "CentralWarehouseKey",
        "ForecastTotal",
        _week_cols("Forecast"),
        _week_cols("CoverQty"),
    ])
    tables["fpo_tbl_CalcStoreStock"] = _query_item_wh(
        "fpo.tbl_CalcStoreStock", item_key, wh_key, select=calc_store_select
    )
    tables["fpo_tbl_ForecastStoreSales"] = _query_item_wh(
        "fpo.tbl_ForecastStoreSales", item_key, wh_key, select=forecast_store_select
    )
    if wh_key is not None:
        tables["bicache_tbl_Store"], _ = _query_safe(
            "SELECT DISTINCT s.StoreKey, s.StoreNumber, s.CountryKey, s.PartnerKey, s.StoreStatus "
            "FROM bicache.tbl_Store s "
            "JOIN fpo.tbl_CalcStoreStock css ON css.StoreKey = s.StoreKey "
            "WHERE css.ItemKey = ? AND css.CentralWarehouseKey = ?",
            [item_key, wh_key],
        )

    # ItemWarehouseLeadtime (DeliveryDateWk01..53, BlockReasonWk01..53)
    tables["fpo_tbl_ItemWarehouseLeadtime"] = _query_item_wh(
        "fpo.tbl_ItemWarehouseLeadtime", item_key, wh_key
    )

    # 53-week calendar from current week
    start_week = _get_current_calc_week_no()
    tables["fpo_tbl_CalcTimelineWeek"], _ = _query_safe(
        "SELECT TOP 53 CalcWeekNo, YearAndWeek, WeekStartDate, WeekEndDate "
        "FROM fpo.tbl_CalcTimelineWeek WHERE CalcWeekNo >= ? ORDER BY CalcWeekNo",
        [start_week],
    )

    # Cover configs (current week-of-year)
    tables["fpo_tbl_ConfigStoreCover"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ConfigStoreCover WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
    )
    tables["fpo_tbl_ConfigWarehouseCover"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ConfigWarehouseCover WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
    )
    if central_warehouse_code:
        tables["fpo_tbl_ImportCoverConfig"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ImportCoverConfig "
            "WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE()) AND CentralWarehouseCode = ?",
            [central_warehouse_code.strip()],
        )
    else:
        tables["fpo_tbl_ImportCoverConfig"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ImportCoverConfig WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
        )

    # Multi-warehouse rollup summary
    multi_wh_summary: Optional[List[Dict[str, Any]]] = None
    if include_all_warehouses and not central_warehouse_code:
        mw_rows, _ = _query_safe(
            "SELECT cw.CentralWarehouseCode, cw.CentralWarehouseName, "
            "cws.CloseStockWk00, iw.CategoryABC, iw.ReqPO, iw.SafetyStockQty "
            "FROM fpo.tbl_CalcWarehouseStock cws "
            "JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey "
            "LEFT JOIN fpo.tbl_ItemWarehouse iw "
            "  ON iw.ItemKey = cws.ItemKey AND iw.CentralWarehouseKey = cws.CentralWarehouseKey "
            "WHERE cws.ItemKey = ?",
            [item_key],
        )
        multi_wh_summary = mw_rows or []

    # Pre-aggregated weekly series — sum across all stores for this warehouse
    # demand_by_week:        SUM(CalcStoreStock.DemandWkNN)    per week
    #                        = store-requested pulls, matches SQL RecCumulativeDemand source
    # store_stockin_by_week: SUM(CalcStoreStock.StockInWkNN)   per week
    #                        = store demand satisfied by warehouse, matches SQL W3 close-stock calc
    # forecast_by_week:      SUM(ForecastStoreSales.ForecastWkNN) per week (raw forecast units)
    # ststock_by_week:       SUM(CalcStoreStock.CloseStockWkNN) per week (closing store stock)
    demand_by_week = _aggregate_weekly_series(
        "fpo.tbl_CalcStoreStock", "Demand", item_key, wh_key, "demand"
    )
    store_stockin_by_week = _aggregate_weekly_series(
        "fpo.tbl_CalcStoreStock", "StockIn", item_key, wh_key, "stockin"
    )
    forecast_by_week = _aggregate_weekly_series(
        "fpo.tbl_ForecastStoreSales", "Forecast", item_key, wh_key, "forecast"
    )
    ststock_by_week = _aggregate_weekly_series(
        "fpo.tbl_CalcStoreStock", "CloseStock", item_key, wh_key, "ststock",
        positive_only=True,
    )

    result: Dict[str, Any] = {
        "status": "ok",
        "item_number": item_number,
        "item_key": item_key,
        "current_calc_week_no": start_week,
        "warehouse_code": central_warehouse_code,
        "orders_excluded": True,
        "demand_by_week": demand_by_week,
        "store_stockin_by_week": store_stockin_by_week,
        "forecast_by_week": forecast_by_week,
        "ststock_by_week": ststock_by_week,
        "tables": {
            k: v for k, v in tables.items()
            if v is not None
            and k not in ("fpo_tbl_CalcStoreStock", "fpo_tbl_ForecastStoreSales", "bicache_tbl_Store")
        },
        "counts": {k: len(v or []) for k, v in tables.items()},
    }
    if multi_wh_summary is not None:
        result["multi_warehouse_summary"] = multi_wh_summary
    return result


# ---------------------------------------------------------------------------
# AGENT TOOL 2: get_fpo_source_table
# ---------------------------------------------------------------------------

_FPO_SOURCE_TABLES: Dict[str, Dict[str, Any]] = {
    # bicache schema
    "bicache.tbl_Item":             {"purpose": "Item master data",                        "item_col": "ItemKey",    "wh_col": None,                  "wh_code_col": None},
    "bicache.tbl_CentralWarehouse": {"purpose": "Warehouse reference data",                "item_col": None,         "wh_col": "CentralWarehouseKey", "wh_code_col": "CentralWarehouseCode"},
    "bicache.tbl_Calendar":         {"purpose": "Calendar source for timeline building",   "item_col": None,         "wh_col": None,                  "wh_code_col": None},
    "bicache.tbl_Store":            {"purpose": "Store reference data",                    "item_col": None,         "wh_col": None,                  "wh_code_col": None},
    # model schema
    "model.tbl_CoreAssortment":          {"purpose": "Master item-warehouse assortment list",   "item_col": "ItemKey",    "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "model.tbl_Item":                    {"purpose": "Production lead time & compliance flags", "item_col": "ItemKey",    "wh_col": None,                  "wh_code_col": None},
    "model.tbl_StockStoreOnHand":        {"purpose": "Opening on-hand stock per store",         "item_col": "ItemKeyNew", "wh_col": None,                  "wh_code_col": None},
    "model.tbl_StockWarehouseOnHand":    {"purpose": "Opening on-hand stock per warehouse",     "item_col": "ItemKeyNew", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "model.tbl_StockOpenPurchaseOrders": {"purpose": "AX open purchase orders",                 "item_col": "ItemKey",    "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "model.tbl_StockInTransit":          {"purpose": "In-transit stock per store",              "item_col": "ItemKey",    "wh_col": None,                  "wh_code_col": None},
    # fpo schema — config/reference
    "fpo.tbl_ImportCoverConfig":  {"purpose": "Store and warehouse cover week config", "item_col": None,      "wh_col": None,                  "wh_code_col": "CentralWarehouseCode"},
    "fpo.tbl_ProductionLeadTime": {"purpose": "Production lead times",                "item_col": "ItemKey", "wh_col": None,                  "wh_code_col": None},
    "fpo.tbl_ShippingLeadTime":   {"purpose": "Shipping lead times",                  "item_col": None,      "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "fpo.tbl_ItemDataIssueType":  {"purpose": "Data issue type lookup",               "item_col": None,      "wh_col": None,                  "wh_code_col": None},
    "fpo.PO_CalcAOQ":             {"purpose": "Average order quantity source",        "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "fpo.PO_CalcEOQ":             {"purpose": "Economic order quantity source",       "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    "fpo.PO_CalcLOQ":             {"purpose": "Last order quantity source",           "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    # config schema
    "config.tbl_Country":             {"purpose": "Country pallet order flags", "item_col": None, "wh_col": None, "wh_code_col": None},
    "config.tbl_CountryDowntimeType": {"purpose": "CNY delay calculation",      "item_col": None, "wh_col": None, "wh_code_col": None},
    "config.tbl_DowntimeType":        {"purpose": "CNY delay type lookup",      "item_col": None, "wh_col": None, "wh_code_col": None},
    "config.tbl_DowntimeDates":       {"purpose": "CNY delay date ranges",      "item_col": None, "wh_col": None, "wh_code_col": None},
    # bet schema
    "bet.tbl_TOOL_VersionControl": {"purpose": "Active forecast version",      "item_col": None,      "wh_col": None,                  "wh_code_col": None},
    "bet.tbl_TOOL_ABC":            {"purpose": "ABC category classification",   "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    # bed schema
    "bed.DATA_WHItem_QtySS":       {"purpose": "Safety stock quantities",       "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
    # cam schema
    "cam.TOOL_CurrentAgreementWH": {"purpose": "Cost/rate data per item-warehouse", "item_col": "ItemKey", "wh_col": "CentralWarehouseKey", "wh_code_col": None},
}


def get_fpo_source_table(
    table_name: str,
    item_key: Optional[int] = None,
    item_number: Optional[str] = None,
    central_warehouse_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    top_n: int = 200,
) -> Dict[str, Any]:
    """
    Fetch rows from any FPO raw source table (read-only; no calculated/output tables).

    Pass table_name="list" to get the full catalogue of available tables and their purpose.

    Available tables (schema.table):
      bicache: tbl_Item, tbl_CentralWarehouse, tbl_Calendar, tbl_Store
      model:   tbl_CoreAssortment, tbl_Item, tbl_StockStoreOnHand,
               tbl_StockWarehouseOnHand, tbl_StockOpenPurchaseOrders, tbl_StockInTransit
      fpo:     tbl_ImportCoverConfig, tbl_ProductionLeadTime, tbl_ShippingLeadTime,
               tbl_ItemDataIssueType, PO_CalcAOQ, PO_CalcEOQ, PO_CalcLOQ
      config:  tbl_Country, tbl_CountryDowntimeType, tbl_DowntimeType, tbl_DowntimeDates
      bet:     tbl_TOOL_VersionControl, tbl_TOOL_ABC
      bed:     DATA_WHItem_QtySS
      cam:     TOOL_CurrentAgreementWH

    Filters applied when the table supports the column and the value is supplied:
      item_number / item_key          → ItemKey (or ItemKeyNew for model stock tables)
      central_warehouse_code / key   → CentralWarehouseKey or CentralWarehouseCode
    """
    if (table_name or "").strip().lower() == "list":
        return {"status": "ok", "tables": {k: v["purpose"] for k, v in _FPO_SOURCE_TABLES.items()}}

    normalised = (table_name or "").strip()
    meta = _FPO_SOURCE_TABLES.get(normalised)
    if meta is None:
        return {
            "status": "error",
            "message": f"Unknown table '{normalised}'. Call with table_name='list' for the full catalogue.",
            "allowed_tables": sorted(_FPO_SOURCE_TABLES.keys()),
        }

    limit = _normalize_top_n(top_n, 500)
    filters_sql: List[str] = []
    params: List[Any] = []

    # Resolve item_key from item_number
    resolved_item_key = item_key
    if resolved_item_key is None and item_number:
        ik_rows, _ = _query_safe(
            "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
            [item_number.strip()],
        )
        if ik_rows:
            resolved_item_key = int(ik_rows[0]["ItemKey"])

    item_col = meta.get("item_col")
    if item_col and resolved_item_key is not None:
        filters_sql.append(f"{item_col} = ?")
        params.append(resolved_item_key)

    # Resolve warehouse key from code
    resolved_wh_key = central_warehouse_key
    if resolved_wh_key is None and central_warehouse_code:
        wk_rows, _ = _query_safe(
            "SELECT TOP 1 CentralWarehouseKey FROM bicache.tbl_CentralWarehouse WHERE CentralWarehouseCode = ?",
            [central_warehouse_code.strip()],
        )
        if wk_rows:
            resolved_wh_key = int(wk_rows[0]["CentralWarehouseKey"])

    wh_col      = meta.get("wh_col")
    wh_code_col = meta.get("wh_code_col")
    if wh_col and resolved_wh_key is not None:
        filters_sql.append(f"{wh_col} = ?")
        params.append(resolved_wh_key)
    elif wh_code_col and central_warehouse_code:
        filters_sql.append(f"{wh_code_col} = ?")
        params.append(central_warehouse_code.strip())

    where = ("WHERE " + " AND ".join(filters_sql)) if filters_sql else ""
    sql = f"SELECT TOP ({limit}) * FROM {normalised} {where}"
    rows, err = _query_safe(sql, params if params else None)
    if err:
        return {"status": "error", "table": normalised, "message": err, "results": []}
    return {
        "status": "ok",
        "table": normalised,
        "purpose": meta["purpose"],
        "count": len(rows),
        "filters_applied": {
            "item_key": resolved_item_key if item_col and resolved_item_key is not None else None,
            "central_warehouse_key": resolved_wh_key if wh_col and resolved_wh_key is not None else None,
            "central_warehouse_code": central_warehouse_code if wh_code_col and central_warehouse_code else None,
        },
        "results": rows,
    }


# ---------------------------------------------------------------------------
# API-only: validate_proposal
# (used by api.py endpoints — not registered as agent tools)
# ---------------------------------------------------------------------------

def validate_proposal(
    quantity: int,
    reorderPoint: int,
    casePack: int,
    moq: int = 0,
    aoq: Optional[int] = None,
    eoq: Optional[int] = None,
    loq: Optional[int] = None,
    soq: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Validate and adjust a proposed order quantity against all reorder constraints.

    Constraint application order:
      1. MOQ / AOQ floor
      2. reorderPoint floor
      3. EOQ rounding (round up to multiple)
      4. casePack rounding (round up to multiple)
      5. LOQ cap (hard maximum)
      6. SOQ rounding (round up to multiple, applied last)
    """
    constraints = {
        "reorderPoint": reorderPoint, "casePack": casePack, "moq": moq,
        "aoq": aoq, "eoq": eoq, "loq": loq, "soq": soq,
    }
    if casePack <= 0:
        return {"valid": False, "input_quantity": int(quantity), "adjusted_quantity": 0,
                "adjustment_made": True, "reasoning": [], "issues": ["Invalid casePack; must be > 0"],
                "constraints": constraints}
    if reorderPoint < 0 or moq < 0:
        return {"valid": False, "input_quantity": int(quantity), "adjusted_quantity": 0,
                "adjustment_made": True, "reasoning": [], "issues": ["reorderPoint and moq must be >= 0"],
                "constraints": constraints}

    reasoning: List[str] = []
    original = max(0, int(quantity))
    q = original

    effective_moq = int(moq)
    if aoq is not None and int(aoq) > effective_moq:
        effective_moq = int(aoq)
        reasoning.append(f"AOQ={aoq} exceeds MOQ={moq}; using AOQ as minimum floor.")
    if q < effective_moq:
        reasoning.append(f"Raised from {q} to MOQ/AOQ floor {effective_moq}.")
        q = effective_moq
    elif effective_moq > 0:
        reasoning.append(f"MOQ/AOQ floor {effective_moq} satisfied (quantity {q} >= floor).")

    rp = int(reorderPoint)
    if q < rp:
        reasoning.append(f"Raised from {q} to reorderPoint floor {rp}.")
        q = rp
    elif rp > 0:
        reasoning.append(f"reorderPoint {rp} satisfied (quantity {q} >= reorderPoint).")

    if eoq is not None and int(eoq) > 0:
        eoq_int = int(eoq)
        r = int(math.ceil(q / eoq_int) * eoq_int) if q > 0 else 0
        if r != q:
            reasoning.append(f"Rounded up from {q} to EOQ multiple of {eoq_int}: {r}.")
        else:
            reasoning.append(f"EOQ={eoq_int}: quantity {q} is already a valid multiple.")
        q = r

    if q > 0:
        r = int(math.ceil(q / casePack) * casePack)
        if r != q:
            reasoning.append(f"Rounded up from {q} to casePack multiple of {casePack}: {r}.")
        else:
            reasoning.append(f"casePack={casePack}: quantity {q} is already a valid multiple.")
        q = r

    if loq is not None and int(loq) > 0 and q > int(loq):
        reasoning.append(f"Capped from {q} down to LOQ maximum {int(loq)}.")
        q = int(loq)

    if soq is not None and int(soq) > 0:
        soq_int = int(soq)
        r = int(math.ceil(q / soq_int) * soq_int) if q > 0 else 0
        if r != q:
            reasoning.append(f"Rounded up from {q} to SOQ multiple of {soq_int}: {r}.")
        else:
            reasoning.append(f"SOQ={soq_int}: quantity {q} is already a valid multiple.")
        q = r

    if not reasoning:
        reasoning.append("No adjustment needed; quantity satisfies all constraints as supplied.")

    issues: List[str] = []
    if q != original:
        issues.append(f"Quantity adjusted from {original} to {q} to satisfy constraints.")

    return {
        "valid": True,
        "input_quantity": original,
        "adjusted_quantity": q,
        "adjustment_made": q != original,
        "reasoning": reasoning,
        "issues": issues,
        "constraints": constraints,
    }
