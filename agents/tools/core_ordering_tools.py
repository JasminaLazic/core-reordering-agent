"""
Tools for core re-ordering analysis against PlanningToolsDB.

These tools are intentionally read-only and designed for agent use.
"""
import os
import math
import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

_SQLITE_CONN: Optional[sqlite3.Connection] = None
_SQLITE_LOCK = threading.Lock()


def _is_local_mode() -> bool:
    val = os.environ.get("IS_LOCAL") or os.environ.get("isLocal") or "false"
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_wk_cols(prefix: str, total_weeks: int = 53) -> List[str]:
    return [f"{prefix}{i:02d}" for i in range(1, total_weeks + 1)]


def _create_mock_tables(conn: sqlite3.Connection) -> None:
    forecast_cols = ", ".join([f"{c} REAL DEFAULT 0" for c in _build_wk_cols("ForecastWk", 53)])
    demand_cols = ", ".join([f"{c} REAL DEFAULT 0" for c in _build_wk_cols("DemandWk", 53)])
    close_store_cols = ", ".join([f"{c} REAL DEFAULT 0" for c in _build_wk_cols("CloseStockWk", 53)])
    close_wh_cols = ", ".join([f"{c} REAL DEFAULT 0" for c in _build_wk_cols("CloseStockWk", 53)])
    conn.executescript(
        f"""
CREATE TABLE bicache.tbl_Item (
    ItemKey INTEGER PRIMARY KEY,
    ItemNumber TEXT NOT NULL,
    ItemName TEXT
);
CREATE TABLE bicache.tbl_CentralWarehouse (
    CentralWarehouseKey INTEGER PRIMARY KEY,
    CentralWarehouseCode TEXT NOT NULL
);
CREATE TABLE model.tbl_StockWarehouseOnHand (
    ItemKeyNew INTEGER,
    CentralWarehouseKey INTEGER,
    StockOnHand REAL,
    QuantityOrdered REAL,
    QuntityInPurchase REAL
);
CREATE TABLE model.tbl_StockStoreOnHand (
    ItemKeyNew INTEGER,
    StoreKey INTEGER,
    StockOnHand REAL
);
CREATE TABLE model.tbl_StoreWarehouseRelationship (
    StoreKey INTEGER,
    CentralWarehouseKey INTEGER,
    ValidFromDate TEXT,
    ValidToDate TEXT
);
CREATE TABLE model.tbl_CoreAssortment (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    ReqFC INTEGER,
    ReqPO INTEGER,
    HasStock INTEGER,
    HasPO INTEGER,
    DataTimestamp TEXT
);
CREATE TABLE fpo.tbl_ForecastStoreSales (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    StoreKey INTEGER,
    {forecast_cols}
);
CREATE TABLE fpo.tbl_CalcTimelineDay (
    CalcWeekNo INTEGER,
    YearAndWeek INTEGER
);
CREATE TABLE fpo.tbl_CalcTimelineWeek (
    CalcWeekNo INTEGER,
    YearAndWeek INTEGER
);
CREATE TABLE fpo.tbl_CalcStoreStock (
    ItemKey INTEGER,
    StoreKey INTEGER,
    CentralWarehouseKey INTEGER,
    {demand_cols},
    {close_store_cols}
);
CREATE TABLE fpo.tbl_CalcWarehouseStock (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    {close_wh_cols}
);
CREATE TABLE fpo.tbl_ItemWarehouse (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    CentralWarehouseCode TEXT,
    CategoryABC TEXT,
    SafetyStockQty INTEGER
);
CREATE TABLE fpo.tbl_ItemWarehouseOrderQty (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    OrderQtyType TEXT,
    AOQ INTEGER,
    EOQ INTEGER,
    LOQ INTEGER,
    SOQ INTEGER
);
CREATE TABLE fpo.tbl_ItemWarehouseLeadtime (
    ItemKey INTEGER,
    CentralWarehouseKey INTEGER,
    CalcWeekNo INTEGER,
    LeadtimeDays INTEGER
);
CREATE TABLE fpo.tbl_ConfigStoreCover (
    CategoryABC TEXT,
    WeekOfYear INTEGER,
    NoOfWeeksCoverStoreOrder INTEGER
);
CREATE TABLE fpo.tbl_ConfigWarehouseCover (
    CategoryABC TEXT,
    WeekOfYear INTEGER,
    NoOfWeeksCoverWarehouseOrder INTEGER
);
CREATE TABLE fpo.tbl_ImportCoverConfig (
    CentralWarehouseCode TEXT,
    CategoryABC TEXT,
    WeekOfYear INTEGER,
    NoOfWeeksCoverWarehouseOrder INTEGER
);
CREATE TABLE am.tbl_JobControl (
    JobName TEXT,
    LastRunStart TEXT,
    LastRunEnd TEXT,
    LastRunErrorMessage TEXT
);
CREATE TABLE am.tbl_JobControlHistory (
    JobName TEXT,
    JobStart TEXT,
    JobEnd TEXT,
    JobErrorMessage TEXT
);
"""
    )


def _seed_mock_data(conn: sqlite3.Connection) -> None:
    # One deterministic item across four warehouses for local testing.
    item_key = 3000393
    item_number = "3000393"
    warehouses = [
        (1, "DK01WH", 1488, 9904, 40),
        (2, "ES01WH", 1416, 9812, 48),
        (3, "GB01WH", 1368, 8516, 64),
        (4, "CN01WH", 1200, 8189, 72),
    ]
    conn.execute(
        "INSERT INTO bicache.tbl_Item(ItemKey, ItemNumber, ItemName) VALUES (?, ?, ?)",
        (item_key, item_number, "Mock Core Item 3000393"),
    )
    for wh_key, wh_code, wh_on_hand, store_on_hand, store_key in warehouses:
        conn.execute(
            "INSERT INTO bicache.tbl_CentralWarehouse(CentralWarehouseKey, CentralWarehouseCode) VALUES (?, ?)",
            (wh_key, wh_code),
        )
        conn.execute(
            "INSERT INTO model.tbl_StockWarehouseOnHand(ItemKeyNew, CentralWarehouseKey, StockOnHand, QuantityOrdered, QuntityInPurchase) VALUES (?, ?, ?, ?, ?)",
            (item_key, wh_key, float(wh_on_hand), 0.0, 0.0),
        )
        conn.execute(
            "INSERT INTO model.tbl_StockStoreOnHand(ItemKeyNew, StoreKey, StockOnHand) VALUES (?, ?, ?)",
            (item_key, store_key, float(store_on_hand)),
        )
        conn.execute(
            "INSERT INTO model.tbl_StoreWarehouseRelationship(StoreKey, CentralWarehouseKey, ValidFromDate, ValidToDate) VALUES (?, ?, ?, ?)",
            (store_key, wh_key, "2026-01-01", "2027-12-31"),
        )
        conn.execute(
            "INSERT INTO model.tbl_CoreAssortment(ItemKey, CentralWarehouseKey, ReqFC, ReqPO, HasStock, HasPO, DataTimestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (item_key, wh_key, 1, 1, 1, 0, "2026-03-10T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO fpo.tbl_ItemWarehouse(ItemKey, CentralWarehouseKey, CentralWarehouseCode, CategoryABC, SafetyStockQty) VALUES (?, ?, ?, ?, ?)",
            (item_key, wh_key, wh_code, "A", 96),
        )
        conn.execute(
            "INSERT INTO fpo.tbl_ItemWarehouseOrderQty(ItemKey, CentralWarehouseKey, OrderQtyType, AOQ, EOQ, LOQ, SOQ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (item_key, wh_key, "SOQ", 0, 0, 96, 24),
        )
        conn.execute(
            "INSERT INTO fpo.tbl_ItemWarehouseLeadtime(ItemKey, CentralWarehouseKey, CalcWeekNo, LeadtimeDays) VALUES (?, ?, ?, ?)",
            (item_key, wh_key, 1, 14),
        )

    iso_week = datetime.now().isocalendar()[1]
    conn.execute(
        "INSERT INTO fpo.tbl_ConfigStoreCover(CategoryABC, WeekOfYear, NoOfWeeksCoverStoreOrder) VALUES (?, ?, ?)",
        ("A", iso_week, 2),
    )
    conn.execute(
        "INSERT INTO fpo.tbl_ConfigWarehouseCover(CategoryABC, WeekOfYear, NoOfWeeksCoverWarehouseOrder) VALUES (?, ?, ?)",
        ("A", iso_week, 4),
    )
    for _, wh_code, *_ in warehouses:
        conn.execute(
            "INSERT INTO fpo.tbl_ImportCoverConfig(CentralWarehouseCode, CategoryABC, WeekOfYear, NoOfWeeksCoverWarehouseOrder) VALUES (?, ?, ?, ?)",
            (wh_code, "A", iso_week, 4),
        )

    for wk in range(1, 54):
        year_week = 202600 + wk
        conn.execute(
            "INSERT INTO fpo.tbl_CalcTimelineWeek(CalcWeekNo, YearAndWeek) VALUES (?, ?)",
            (wk, year_week),
        )
        conn.execute(
            "INSERT INTO fpo.tbl_CalcTimelineDay(CalcWeekNo, YearAndWeek) VALUES (?, ?)",
            (wk, year_week),
        )

    for wh_key, _, _, _, store_key in warehouses:
        forecast_values = [max(0, 350 - (i * 4) + (wh_key * 3)) for i in range(53)]
        demand_values = [max(0, v - 8) for v in forecast_values]
        close_values = [max(0, 9000 - (i * 120) - (wh_key * 10)) for i in range(53)]
        wh_close_values = [max(0, 1500 - (i * 24) - (wh_key * 2)) for i in range(53)]

        forecast_cols = ", ".join(_build_wk_cols("ForecastWk", 53))
        forecast_q = ", ".join(["?"] * 53)
        conn.execute(
            f"INSERT INTO fpo.tbl_ForecastStoreSales(ItemKey, CentralWarehouseKey, StoreKey, {forecast_cols}) VALUES (?, ?, ?, {forecast_q})",
            [item_key, wh_key, store_key, *forecast_values],
        )

        demand_cols = ", ".join(_build_wk_cols("DemandWk", 53))
        close_cols = ", ".join(_build_wk_cols("CloseStockWk", 53))
        store_q = ", ".join(["?"] * 106)
        conn.execute(
            f"INSERT INTO fpo.tbl_CalcStoreStock(ItemKey, StoreKey, CentralWarehouseKey, {demand_cols}, {close_cols}) VALUES (?, ?, ?, {store_q})",
            [item_key, store_key, wh_key, *demand_values, *close_values],
        )

        wh_close_cols = ", ".join(_build_wk_cols("CloseStockWk", 53))
        wh_q = ", ".join(["?"] * 53)
        conn.execute(
            f"INSERT INTO fpo.tbl_CalcWarehouseStock(ItemKey, CentralWarehouseKey, {wh_close_cols}) VALUES (?, ?, {wh_q})",
            [item_key, wh_key, *wh_close_values],
        )

    conn.execute(
        "INSERT INTO am.tbl_JobControl(JobName, LastRunStart, LastRunEnd, LastRunErrorMessage) VALUES (?, ?, ?, ?)",
        ("CoreOrderingMock", "2026-03-10T09:00:00Z", "2026-03-10T09:05:00Z", None),
    )
    conn.execute(
        "INSERT INTO am.tbl_JobControlHistory(JobName, JobStart, JobEnd, JobErrorMessage) VALUES (?, ?, ?, ?)",
        ("CoreOrderingMock", "2026-03-09T09:00:00Z", "2026-03-09T09:04:00Z", None),
    )
    conn.commit()


def _get_sqlite_conn() -> sqlite3.Connection:
    global _SQLITE_CONN
    if _SQLITE_CONN is not None:
        return _SQLITE_CONN
    with _SQLITE_LOCK:
        if _SQLITE_CONN is None:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("ATTACH DATABASE ':memory:' AS model")
            conn.execute("ATTACH DATABASE ':memory:' AS fpo")
            conn.execute("ATTACH DATABASE ':memory:' AS bicache")
            conn.execute("ATTACH DATABASE ':memory:' AS am")
            _create_mock_tables(conn)
            _seed_mock_data(conn)
            _SQLITE_CONN = conn
    return _SQLITE_CONN


def _rewrite_sql_for_sqlite(sql: str) -> str:
    rewritten = sql
    top_match = re.search(r"select\s+top\s*\(\s*(\d+)\s*\)", rewritten, flags=re.IGNORECASE)
    limit_value: Optional[str] = None
    if top_match:
        limit_value = top_match.group(1)
        rewritten = re.sub(
            r"select\s+top\s*\(\s*\d+\s*\)",
            "SELECT",
            rewritten,
            count=1,
            flags=re.IGNORECASE,
        )
    iso_week = datetime.now().isocalendar()[1]
    rewritten = re.sub(
        r"datepart\s*\(\s*iso_week\s*,\s*getdate\s*\(\s*\)\s*\)",
        str(iso_week),
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"datepart\s*\(\s*week\s*,\s*getdate\s*\(\s*\)\s*\)",
        str(iso_week),
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"getdate\s*\(\s*\)",
        f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'",
        rewritten,
        flags=re.IGNORECASE,
    )
    if limit_value and not re.search(r"\blimit\s+\d+\b", rewritten, flags=re.IGNORECASE):
        rewritten = rewritten.rstrip() + f"\nLIMIT {limit_value}"
    return rewritten


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
    server = os.environ.get("PLANNING_TOOLS_SQL_SERVER")
    database = os.environ.get("PLANNING_TOOLS_SQL_DATABASE", "PlanningTools")
    username = os.environ.get("PLANNING_TOOLS_SQL_USERNAME")
    password = os.environ.get("PLANNING_TOOLS_SQL_PASSWORD")
    driver = os.environ.get("PLANNING_TOOLS_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

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

    # Managed identity / integrated scenarios
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
        row = {}
        for i, c in enumerate(cols):
            row[c] = r[i]
        result.append(row)
    return result


def _query_sqlite(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    conn = _get_sqlite_conn()
    query = _rewrite_sql_for_sqlite(sql)
    cur = conn.cursor()
    cur.execute(query, params or [])
    if cur.description is None:
        return []
    rows = cur.fetchall()
    result: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, sqlite3.Row):
            result.append({k: r[k] for k in r.keys()})
        else:
            # Defensive fallback if row_factory gets changed.
            cols = [c[0] for c in cur.description]
            result.append({cols[i]: r[i] for i in range(len(cols))})
    return result


def _query(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    if _is_local_mode():
        return _query_sqlite(sql, params)
    return _query_sqlserver(sql, params)


def _query_safe(sql: str, params: Optional[List[Any]] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        return _query(sql, params), None
    except Exception as e:
        return [], str(e)


def _ensure_read_only_sql(sql: str) -> None:
    normalized = sql.strip().lower()
    if ";" in normalized:
        # Keep this strict for safety in agent-exposed tool.
        raise ValueError("Only a single SELECT/CTE query without semicolons is allowed.")
    if not (normalized.startswith("select") or normalized.startswith("with")):
        raise ValueError("Only SELECT/CTE read-only queries are allowed.")
    forbidden = [
        " insert ", " update ", " delete ", " merge ", " alter ", " create ",
        " drop ", " truncate ", " execute ", " exec ", " grant ", " revoke ",
    ]
    wrapped = f" {normalized} "
    if any(tok in wrapped for tok in forbidden):
        raise ValueError("Query contains non-read-only statements.")


def _normalize_top_n(top_n: int, max_n: int = 500) -> int:
    return max(1, min(int(top_n), max_n))


def _get_table_rows(
    schema_table: str,
    top_n: int = 100,
    filters: Optional[List[Tuple[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Generic read-only table fetcher for canonical PlanningToolsDB tables.
    """
    limit = _normalize_top_n(top_n, 500)
    active_filters = [(col, val) for col, val in (filters or []) if val is not None]
    where_clause = ""
    params: List[Any] = []
    if active_filters:
        where_clause = "WHERE " + " AND ".join([f"{col} = ?" for col, _ in active_filters])
        params = [v for _, v in active_filters]

    sql = f"""
SELECT TOP ({limit}) *
FROM {schema_table}
{where_clause}
"""
    rows = _query(sql, params if params else None)

    return {
        "table": schema_table,
        "count": len(rows),
        "filters": {k: v for k, v in active_filters},
        "results": rows,
    }


def get_stock_warehouse_on_hand(
    item_key_new: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "model.tbl_StockWarehouseOnHand",
        top_n=top_n,
        filters=[
            ("ItemKeyNew", item_key_new),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_stock_store_on_hand(
    item_key_new: Optional[int] = None,
    store_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "model.tbl_StockStoreOnHand",
        top_n=top_n,
        filters=[
            ("ItemKeyNew", item_key_new),
            ("StoreKey", store_key),
        ],
    )


def get_store_warehouse_relationship(
    store_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "model.tbl_StoreWarehouseRelationship",
        top_n=top_n,
        filters=[
            ("StoreKey", store_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_core_assortment(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "model.tbl_CoreAssortment",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_forecast_store_sales(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ForecastStoreSales",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_calc_timeline_day(
    calc_week_no: Optional[int] = None,
    year_and_week: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_CalcTimelineDay",
        top_n=top_n,
        filters=[
            ("CalcWeekNo", calc_week_no),
            ("YearAndWeek", year_and_week),
        ],
    )


def get_calc_timeline_week(
    calc_week_no: Optional[int] = None,
    year_and_week: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_CalcTimelineWeek",
        top_n=top_n,
        filters=[
            ("CalcWeekNo", calc_week_no),
            ("YearAndWeek", year_and_week),
        ],
    )


def get_calc_store_stock(
    item_key: Optional[int] = None,
    store_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_CalcStoreStock",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("StoreKey", store_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_calc_warehouse_stock(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_CalcWarehouseStock",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_item_warehouse(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ItemWarehouse",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
            ("CentralWarehouseCode", central_warehouse_code),
        ],
    )


def get_item_warehouse_order_qty(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ItemWarehouseOrderQty",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_item_warehouse_leadtime(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    calc_week_no: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ItemWarehouseLeadtime",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
            ("CalcWeekNo", calc_week_no),
        ],
    )


def get_config_store_cover(
    category_abc: Optional[str] = None,
    week_of_year: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ConfigStoreCover",
        top_n=top_n,
        filters=[
            ("CategoryABC", category_abc),
            ("WeekOfYear", week_of_year),
        ],
    )


def get_config_warehouse_cover(
    category_abc: Optional[str] = None,
    week_of_year: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ConfigWarehouseCover",
        top_n=top_n,
        filters=[
            ("CategoryABC", category_abc),
            ("WeekOfYear", week_of_year),
        ],
    )


def get_import_cover_config(
    central_warehouse_code: Optional[str] = None,
    category_abc: Optional[str] = None,
    week_of_year: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "fpo.tbl_ImportCoverConfig",
        top_n=top_n,
        filters=[
            ("CentralWarehouseCode", central_warehouse_code),
            ("CategoryABC", category_abc),
            ("WeekOfYear", week_of_year),
        ],
    )


def get_job_control(
    job_name: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "am.tbl_JobControl",
        top_n=top_n,
        filters=[("JobName", job_name)],
    )


def get_job_control_history(
    job_name: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    return _get_table_rows(
        "am.tbl_JobControlHistory",
        top_n=top_n,
        filters=[("JobName", job_name)],
    )


def _build_forecast_week_columns(total_weeks: int = 53) -> List[str]:
    return [f"ForecastWk{i:02d}" for i in range(1, total_weeks + 1)]


def _build_week_columns(prefix: str, total_weeks: int = 53) -> List[str]:
    return [f"{prefix}{i:02d}" for i in range(1, total_weeks + 1)]


def _to_iso8601_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_core_ordering_schema_reference() -> Dict[str, Any]:
    """
    Returns canonical table references used in core re-ordering.
    """
    return {
        "database": "PlanningToolsDb",
        "core_tables": {
            "model.tbl_StockWarehouseOnHand": "Warehouse stock on hand by item/warehouse",
            "model.tbl_StockStoreOnHand": "Store stock on hand by item/store",
            "model.tbl_StoreWarehouseRelationship": "Store-to-warehouse mapping with validity dates",
            "model.tbl_CoreAssortment": "Core assortment by item/warehouse",
            "fpo.tbl_ForecastStoreSales": "Store forecast by week",
            "fpo.tbl_CalcTimelineDay": "Daily calc timeline",
            "fpo.tbl_CalcTimelineWeek": "Weekly calc timeline",
            "fpo.tbl_CalcStoreStock": "Store-level calc state",
            "fpo.tbl_CalcWarehouseStock": "Warehouse-level calc state",
            "fpo.tbl_ItemWarehouse": "Item/warehouse config incl. ABC and lead-time fields",
            "fpo.tbl_ItemWarehouseOrderQty": "Order quantity controls (AOQ/EOQ/LOQ/SOQ)",
            "fpo.tbl_ItemWarehouseLeadtime": "Delivery/req-post calendar by calc week",
            "fpo.tbl_ConfigStoreCover": "Store cover horizon by week and ABC",
            "fpo.tbl_ConfigWarehouseCover": "Warehouse cover horizon by week and ABC",
            "fpo.tbl_ImportCoverConfig": "Configurable weeks-of-cover by warehouse/ABC/week",
            "am.tbl_JobControl": "Job control state table",
            "am.tbl_JobControlHistory": "Job run history",
        },
        "agent_driven_logic": {
            "target_cover": "Default 4-week cover (or config-derived)",
            "base_need": "target_stock_qty - (on_hand + inbound)",
            "constraints": "MOQ + order multiple (SOQ/AOQ)",
            "rounding": "ceil(required / order_multiple) * order_multiple",
        },
        "optional_reference_objects": {
            "fpo.usp_RefreshOrderData": "Operational refresh object",
            "fpo.usp_RecalcWarehouseStock": "Operational recalculation object",
        },
        "notes": [
            "Use read-only queries only.",
            "Agent computes recommendations from table data.",
            "Use the same table structure as PlanningToolsDB to keep output parity.",
        ],
        "fpo_core_reordering_contract": {
            "required_step_sequence": ["W1", "S2", "W3", "R4", "S5", "C6", "R7"],
            "strict_week_model": "CalcWeekNo / Wk01..Wk53 indexed horizon slots",
            "required_output_shape": [
                "update_calc_store_stock[]",
                "update_calc_warehouse_stock[]",
                "insert_recommended_orders[]",
                "reset_rec_fields[]",
                "set_block_until_week[]",
                "next_week_pointer",
            ],
        },
    }


def get_core_reordering_agent_payload(
    week_start: int = 1,
    item_scope: str = "all",
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    top_n: int = 500,
    iteration_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read-only data fetch payload for the FPO core reordering agent.
    No decision logic or fail-closed logic is applied here.
    """
    if week_start < 1 or week_start > 53:
        raise ValueError("week_start must be between 1 and 53")
    limit = _normalize_top_n(top_n, 2000)

    now_iso = _to_iso8601_utc_now()
    iteration = iteration_id or f"core_reorder_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    timeline_rows = get_calc_timeline_week(
        calc_week_no=None,
        year_and_week=None,
        top_n=53,
    )["results"]

    if item_number:
        item_rows = _query(
            """
SELECT TOP (1) ItemKey
FROM bicache.tbl_Item
WHERE ItemNumber = ?
""",
            [item_number],
        )
        if item_rows:
            item_key = int(item_rows[0]["ItemKey"])

    item_warehouse_state = get_calc_warehouse_stock(
        item_key=item_key,
        central_warehouse_key=None,
        top_n=limit,
    )["results"]
    item_store_state = get_calc_store_stock(
        item_key=item_key,
        store_key=None,
        central_warehouse_key=None,
        top_n=limit,
    )["results"]
    constraints = get_item_warehouse(
        item_key=item_key,
        central_warehouse_key=None,
        central_warehouse_code=None,
        top_n=limit,
    )["results"]
    data_quality_flags = get_item_warehouse(
        item_key=item_key,
        central_warehouse_key=None,
        central_warehouse_code=None,
        top_n=limit,
    )["results"]

    return {
        "run_context": {
            "item_scope": item_scope,
            "item_number": item_number,
            "item_key": item_key,
            "week_start": week_start,
            "today": now_iso[:10],
            "iteration_id": iteration,
        },
        "timeline": timeline_rows,
        "item_warehouse_state": item_warehouse_state,
        "item_store_state": item_store_state,
        "existing_orders": [],
        "constraints": constraints,
        "data_quality_flags": data_quality_flags,
        "meta": {
            "counts": {
                "timeline": len(timeline_rows),
                "item_warehouse_state": len(item_warehouse_state),
                "item_store_state": len(item_store_state),
                "existing_orders": 0,
                "constraints": len(constraints),
                "data_quality_flags": len(data_quality_flags),
            },
            "generated_at_utc": now_iso,
            "source": "raw_table_fetch_payload",
            "week_columns_supported": {
                "forecast": _build_week_columns("ForecastWk", 53),
                "demand": _build_week_columns("DemandWk", 53),
                "stock_in": _build_week_columns("StockInWk", 53),
                "close_stock": _build_week_columns("CloseStockWk", 53),
                "cover_qty": _build_week_columns("CoverQtyWk", 53),
            },
        },
    }


def get_core_ordering_snapshot(item_number: Optional[str] = None, top_n: int = 50) -> Dict[str, Any]:
    """
    Fetches a compact snapshot of core ordering state using canonical objects.
    """
    limit = max(1, min(top_n, 200))
    item_filter = ""
    params: List[Any] = []
    if item_number:
        item_filter = "AND i.ItemNumber = ?"
        params.insert(0, item_number)

    sql = f"""
WITH base AS (
    SELECT TOP ({limit})
        i.ItemNumber,
        i.ItemName,
        swa.CentralWarehouseKey,
        swa.StockOnHand,
        swa.QuantityOrdered,
        ca.ReqFC,
        ca.ReqPO,
        ca.HasStock,
        ca.HasPO,
        ca.DataTimestamp
    FROM model.tbl_CoreAssortment ca
    JOIN bicache.tbl_Item i
        ON ca.ItemKey = i.ItemKey
    LEFT JOIN model.tbl_StockWarehouseOnHand swa
        ON ca.ItemKey = swa.ItemKeyNew
        AND ca.CentralWarehouseKey = swa.CentralWarehouseKey
    WHERE 1=1
    {item_filter}
    ORDER BY ca.DataTimestamp DESC
)
SELECT * FROM base
"""
    rows = _query(sql, params)
    return {
        "count": len(rows),
        "item_number_filter": item_number,
        "results": rows,
    }


def get_core_ordering_inputs(
    target_weeks_cover: int = 4,
    use_cover_config: bool = True,
    central_warehouse_code: Optional[str] = None,
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    forecast_horizon_weeks: int = 53,
    top_n: int = 200,
) -> Dict[str, Any]:
    """
    Deterministic data fetch for core re-ordering inputs.
    Returns raw per-item/per-warehouse inputs from canonical tables (SELECT only).
    No reorder calculations are performed in this function.
    """
    limit = max(1, min(top_n, 500))
    if target_weeks_cover < 1 or target_weeks_cover > 12:
        raise ValueError("target_weeks_cover must be between 1 and 12")
    if forecast_horizon_weeks < 1 or forecast_horizon_weeks > 53:
        raise ValueError("forecast_horizon_weeks must be between 1 and 53")

    filters: List[str] = []
    params: List[Any] = []
    if central_warehouse_code:
        filters.append("cw.CentralWarehouseCode = ?")
        params.append(central_warehouse_code)
    if item_number:
        filters.append("i.ItemNumber = ?")
        params.append(item_number)
    if item_key is not None:
        filters.append("iw.ItemKey = ?")
        params.append(item_key)
    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

    forecast_cols = _build_forecast_week_columns(53)
    fs_select = ",\n        ".join(
        [f"SUM(COALESCE(f.{c}, 0)) AS {c}" for c in forecast_cols]
    )
    outer_select = ",\n    ".join([f"COALESCE(fs.{c}, 0) AS {c}" for c in forecast_cols])

    sql = f"""
WITH fs AS (
    SELECT
        f.ItemKey,
        f.CentralWarehouseKey,
        {fs_select}
    FROM fpo.tbl_ForecastStoreSales f
    GROUP BY f.ItemKey, f.CentralWarehouseKey
)
SELECT TOP ({limit})
    i.ItemNumber,
    i.ItemName,
    iw.ItemKey,
    iw.CentralWarehouseKey,
    cw.CentralWarehouseCode,
    iw.CategoryABC,
    COALESCE(iw.SafetyStockQty, 0) AS SafetyStockQty,
    COALESCE(s.StockOnHand, 0) AS StockOnHand,
    COALESCE(s.QuantityOrdered, 0) + COALESCE(s.QuntityInPurchase, 0) AS InboundQty,
    {outer_select},
    COALESCE(cov.NoOfWeeksCoverWarehouseOrder, 4) AS ConfigWeeksCover,
    oq.OrderQtyType,
    COALESCE(oq.AOQ, 0) AS AOQ,
    COALESCE(oq.EOQ, 0) AS EOQ,
    COALESCE(oq.LOQ, 0) AS LOQ,
    COALESCE(oq.SOQ, 0) AS SOQ
FROM fpo.tbl_ItemWarehouse iw
JOIN bicache.tbl_Item i
    ON iw.ItemKey = i.ItemKey
JOIN bicache.tbl_CentralWarehouse cw
    ON iw.CentralWarehouseKey = cw.CentralWarehouseKey
LEFT JOIN model.tbl_StockWarehouseOnHand s
    ON s.ItemKeyNew = iw.ItemKey
    AND s.CentralWarehouseKey = iw.CentralWarehouseKey
LEFT JOIN fs
    ON fs.ItemKey = iw.ItemKey
    AND fs.CentralWarehouseKey = iw.CentralWarehouseKey
LEFT JOIN fpo.tbl_ItemWarehouseOrderQty oq
    ON oq.ItemKey = iw.ItemKey
    AND oq.CentralWarehouseKey = iw.CentralWarehouseKey
LEFT JOIN fpo.tbl_ImportCoverConfig cov
    ON cov.CentralWarehouseCode = cw.CentralWarehouseCode
    AND cov.CategoryABC = iw.CategoryABC
    AND cov.WeekOfYear = DATEPART(ISO_WEEK, GETDATE())
{where_clause}
ORDER BY cw.CentralWarehouseCode, i.ItemNumber
"""
    rows = _query(sql, params)
    result_rows: List[Dict[str, Any]] = []
    for r in rows:
        result_rows.append(
            {
                "item_number": r.get("ItemNumber"),
                "item_name": r.get("ItemName"),
                "item_key": r.get("ItemKey"),
                "central_warehouse_code": r.get("CentralWarehouseCode"),
                "on_hand": round(float(r.get("StockOnHand") or 0), 2),
                "inbound_qty": round(float(r.get("InboundQty") or 0), 2),
                "safety_stock_qty": int(r.get("SafetyStockQty") or 0),
                "forecast_horizon_weeks": forecast_horizon_weeks,
                "config_weeks_cover": int(r.get("ConfigWeeksCover") or 4),
                "target_weeks_cover_input": target_weeks_cover,
                "use_cover_config": use_cover_config,
                "order_qty_type": r.get("OrderQtyType"),
                "aoq": int(r.get("AOQ") or 0),
                "eoq": int(r.get("EOQ") or 0),
                "loq": int(r.get("LOQ") or 0),
                "soq": int(r.get("SOQ") or 0),
                "forecast_by_week": {
                    c.replace("ForecastWk", "W"): round(float(r.get(c) or 0), 2)
                    for c in forecast_cols[:forecast_horizon_weeks]
                },
            }
        )
    return {
        "mode": "deterministic_core_ordering_inputs",
        "target_weeks_cover_input": target_weeks_cover,
        "use_cover_config": use_cover_config,
        "forecast_horizon_weeks": forecast_horizon_weeks,
        "filters": {
            "central_warehouse_code": central_warehouse_code,
            "item_number": item_number,
            "item_key": item_key,
        },
        "count": len(result_rows),
        "results": result_rows,
    }


def get_job_control_status(job_name: Optional[str] = None, top_n: int = 30) -> Dict[str, Any]:
    """
    Read latest job control and job history rows from PlanningToolsDB.
    """
    limit = max(1, min(top_n, 200))
    where_clause = ""
    params: List[Any] = []
    if job_name:
        where_clause = "WHERE jc.JobName = ?"
        params.append(job_name)

    sql = f"""
SELECT TOP ({limit})
    jc.JobName,
    jc.LastRunStart,
    jc.LastRunEnd,
    jc.LastRunErrorMessage,
    jh.JobStart,
    jh.JobEnd,
    jh.JobErrorMessage
FROM am.tbl_JobControl jc
LEFT JOIN am.tbl_JobControlHistory jh
    ON jc.JobName = jh.JobName
{where_clause}
ORDER BY COALESCE(jh.JobStart, jc.LastRunStart) DESC
"""
    rows = _query(sql, params)
    return {
        "job_name_filter": job_name,
        "count": len(rows),
        "results": rows,
    }


def run_planning_tools_readonly_query(sql_query: str) -> Dict[str, Any]:
    """
    Execute a read-only SQL query (SELECT/CTE only) against PlanningToolsDB.
    """
    _ensure_read_only_sql(sql_query)
    rows = _query(sql_query)
    return {
        "count": len(rows),
        "results": rows[:500],
        "truncated": len(rows) > 500,
    }


def get_reorder_context(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
    weeks_cover: int = 4,
    week_start: int = 1,
) -> Dict[str, Any]:
    """
    Deterministic reorder context from FPO PlanningToolsDB schema.
    """
    if not item_number:
        raise ValueError("item_number is required")

    inputs = get_core_ordering_inputs(
        target_weeks_cover=weeks_cover,
        use_cover_config=True,
        central_warehouse_code=central_warehouse_code,
        item_number=item_number,
        forecast_horizon_weeks=53,
        top_n=50,
    )
    payload = get_core_reordering_agent_payload(
        week_start=week_start,
        item_scope="item",
        item_number=item_number,
        top_n=200,
    )

    if not inputs.get("results"):
        return {
            "status": "insufficient_data",
            "message": "No reorder context found for item_number in FPO PlanningToolsDB.",
            "item_number": item_number,
        }

    selected = inputs["results"][0]
    case_pack = int(selected.get("soq") or selected.get("aoq") or 1)
    reorder_point = int(selected.get("safety_stock_qty") or 0)
    if reorder_point == 0:
        reorder_point = int(selected.get("loq") or 0)

    return {
        "status": "ok",
        "item_number": selected.get("item_number"),
        "item_key": selected.get("item_key"),
        "warehouse_code": selected.get("central_warehouse_code"),
        "on_hand": float(selected.get("on_hand") or 0),
        "inbound_qty": float(selected.get("inbound_qty") or 0),
        "weeks_cover": int(selected.get("config_weeks_cover") or weeks_cover),
        "reorder_point": reorder_point,
        "casePack": case_pack,
        "moq": int(selected.get("loq") or selected.get("eoq") or 0),
        "order_multiple": case_pack,
        "order_qty_type": selected.get("order_qty_type"),
        "forecast_by_week": selected.get("forecast_by_week", {}),
        "fpo_payload_meta": {
            "payload_errors": payload.get("payload_errors", []),
            "counts": payload.get("meta", {}).get("counts", {}),
        },
        "table_sources": [
            "fpo.tbl_ItemWarehouse",
            "fpo.tbl_ItemWarehouseOrderQty",
            "fpo.tbl_ForecastStoreSales",
            "model.tbl_StockWarehouseOnHand",
            "fpo.tbl_CalcWarehouseStock",
            "fpo.tbl_CalcStoreStock",
        ],
    }


def get_sales_history(
    item_number: str,
    week_start: int = 1,
    history_weeks: int = 8,
) -> Dict[str, Any]:
    """
    Returns recent demand history from FPO calc-state rows.
    """
    if history_weeks < 1 or history_weeks > 53:
        raise ValueError("history_weeks must be between 1 and 53")

    payload = get_core_reordering_agent_payload(
        week_start=week_start,
        item_scope="item",
        item_number=item_number,
        top_n=1000,
    )
    store_rows = payload.get("item_store_state", [])
    if not store_rows:
        return {
            "status": "insufficient_data",
            "message": "No item_store_state found for item_number in fpo.tbl_CalcStoreStock.",
            "item_number": item_number,
        }

    week_cols = [f"DemandWk{i:02d}" for i in range(1, history_weeks + 1)]
    aggregate = {wk: 0.0 for wk in week_cols}
    for row in store_rows:
        for wk in week_cols:
            aggregate[wk] += float(row.get(wk) or 0)

    series = [{"week": wk, "demand": round(aggregate[wk], 3)} for wk in week_cols]
    avg_demand = sum(v["demand"] for v in series) / len(series)
    return {
        "status": "ok",
        "item_number": item_number,
        "history_weeks": history_weeks,
        "series": series,
        "average_weekly_demand": round(avg_demand, 3),
        "source": "fpo.tbl_CalcStoreStock DemandWk01..DemandWk53",
    }


def get_forecast(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
    horizon_weeks: int = 8,
    weeks_cover: int = 4,
) -> Dict[str, Any]:
    """
    Returns forecast horizon from fpo.tbl_ForecastStoreSales.
    """
    if horizon_weeks < 1 or horizon_weeks > 53:
        raise ValueError("horizon_weeks must be between 1 and 53")

    inputs = get_core_ordering_inputs(
        target_weeks_cover=weeks_cover,
        use_cover_config=True,
        central_warehouse_code=central_warehouse_code,
        item_number=item_number,
        forecast_horizon_weeks=horizon_weeks,
        top_n=50,
    )
    if not inputs.get("results"):
        return {
            "status": "insufficient_data",
            "message": "No forecast rows found for item_number in fpo.tbl_ForecastStoreSales.",
            "item_number": item_number,
        }

    selected = inputs["results"][0]
    forecast = selected.get("forecast_by_week", {})
    weekly = [float(forecast.get(f"W{i:02d}", 0) or 0) for i in range(1, horizon_weeks + 1)]
    return {
        "status": "ok",
        "item_number": item_number,
        "warehouse_code": selected.get("central_warehouse_code"),
        "horizon_weeks": horizon_weeks,
        "weekly_forecast": forecast,
        "average_weekly_forecast": round(sum(weekly) / max(1, len(weekly)), 3),
        "source": "fpo.tbl_ForecastStoreSales ForecastWk01..ForecastWk53",
    }


def validate_proposal(
    quantity: int,
    reorderPoint: int,
    casePack: int,
    moq: int = 0,
) -> Dict[str, Any]:
    """
    Enforce reorder constraints and return adjusted quantity.
    """
    if casePack <= 0:
        return {
            "valid": False,
            "adjusted_quantity": 0,
            "issues": ["Invalid casePack; must be > 0"],
        }
    if reorderPoint < 0 or moq < 0:
        return {
            "valid": False,
            "adjusted_quantity": 0,
            "issues": ["reorderPoint and moq must be >= 0"],
        }

    q = max(0, int(quantity))
    q = max(q, int(reorderPoint), int(moq))
    adjusted = int(math.ceil(q / casePack) * casePack) if q > 0 else 0

    issues: List[str] = []
    if quantity != adjusted:
        issues.append("Quantity adjusted to satisfy reorderPoint/moq/casePack constraints.")

    return {
        "valid": True,
        "adjusted_quantity": adjusted,
        "issues": issues,
        "constraints": {
            "reorderPoint": reorderPoint,
            "casePack": casePack,
            "moq": moq,
        },
    }


def calculate_core_reorder_recommendations(
    target_weeks_cover: int = 4,
    use_cover_config: bool = True,
    central_warehouse_code: Optional[str] = None,
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    forecast_horizon_weeks: int = 4,
    top_n: int = 100,
) -> Dict[str, Any]:
    """
    Compatibility wrapper kept for existing callers.
    This tool now returns read-only deterministic inputs only (no calculations).
    """
    raw = get_core_ordering_inputs(
        target_weeks_cover=target_weeks_cover,
        use_cover_config=use_cover_config,
        central_warehouse_code=central_warehouse_code,
        item_number=item_number,
        item_key=item_key,
        forecast_horizon_weeks=forecast_horizon_weeks,
        top_n=top_n,
    )
    raw["mode"] = "deterministic_core_ordering_inputs_via_compat_wrapper"
    raw["note"] = (
        "No calculations are performed in tool layer. "
        "Agent must derive ordering logic from returned raw inputs."
    )
    return raw

