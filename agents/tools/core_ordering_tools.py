"""
Tools for core re-ordering analysis against PlanningToolsDB.

These tools are intentionally read-only and designed for agent use.
Each function fetches a single table — the agent performs all reasoning.
"""
import os
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple


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


def _query(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    return _query_sqlserver(sql, params)


def _query_safe(sql: str, params: Optional[List[Any]] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        return _query(sql, params), None
    except Exception as e:
        return [], str(e)


def _ensure_read_only_sql(sql: str) -> None:
    normalized = sql.strip().lower()
    if ";" in normalized:
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
    """Generic read-only table fetcher. Returns raw rows — agent does all reasoning."""
    limit = _normalize_top_n(top_n, 500)
    active_filters = [(col, val) for col, val in (filters or []) if val is not None]
    where_clause = ""
    params: List[Any] = []
    if active_filters:
        where_clause = "WHERE " + " AND ".join([f"{col} = ?" for col, _ in active_filters])
        params = [v for _, v in active_filters]

    sql = f"SELECT TOP ({limit}) * FROM {schema_table} {where_clause}"
    rows = _query(sql, params if params else None)
    return {
        "table": schema_table,
        "count": len(rows),
        "filters": {k: v for k, v in active_filters},
        "results": rows,
    }


# ---------------------------------------------------------------------------
# Simple single-table fetch tools (no joins, no aggregations)
# ---------------------------------------------------------------------------

def get_item_master(
    item_key: Optional[int] = None,
    item_number: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    """
    Fetch item master data from bicache.tbl_Item.
    Includes item dimensions, carton sizes, MOQ, pallet info, country of origin.
    """
    limit = _normalize_top_n(top_n, 500)
    filters: List[str] = []
    params: List[Any] = []
    if item_key is not None:
        filters.append("ItemKey = ?")
        params.append(item_key)
    if item_number is not None:
        filters.append("ItemNumber = ?")
        params.append(item_number)
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT TOP ({limit}) * FROM bicache.tbl_Item {where}"
    rows, err = _query_safe(sql, params if params else None)
    if err:
        return {"table": "bicache.tbl_Item", "count": 0, "error": err, "results": []}
    return {"table": "bicache.tbl_Item", "count": len(rows), "results": rows}


def get_central_warehouse(
    central_warehouse_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    """Fetch warehouse master from bicache.tbl_CentralWarehouse."""
    return _get_table_rows(
        "bicache.tbl_CentralWarehouse",
        top_n=top_n,
        filters=[
            ("CentralWarehouseKey", central_warehouse_key),
            ("CentralWarehouseCode", central_warehouse_code),
        ],
    )


def get_stock_warehouse_on_hand(
    item_key_new: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    """Fetch warehouse on-hand stock from model.tbl_StockWarehouseOnHand."""
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
    """Fetch store on-hand stock from model.tbl_StockStoreOnHand."""
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
    """Fetch store-to-warehouse mapping from model.tbl_StoreWarehouseRelationship."""
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
    """Fetch core assortment flags from model.tbl_CoreAssortment."""
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
    """
    Fetch per-store weekly forecast from fpo.tbl_ForecastStoreSales.
    Columns: ForecastWk01..ForecastWk53. Agent must aggregate across stores.
    """
    return _get_table_rows(
        "fpo.tbl_ForecastStoreSales",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def _get_current_calc_week_no() -> int:
    """Get CalcWeekNo for the week containing today. Default 1 if not found."""
    rows, err = _query_safe(
        "SELECT TOP 1 CalcWeekNo FROM fpo.tbl_CalcTimelineWeek "
        "WHERE CAST(GETDATE() AS DATE) BETWEEN WeekStartDate AND WeekEndDate"
    )
    if err or not rows:
        return 1
    return int(rows[0]["CalcWeekNo"])


def _build_forecast_case_expr() -> str:
    """Build CASE mapping CalcWeekNo to SUM(ForecastWkNN)."""
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(fss.ForecastWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_demand_case_expr() -> str:
    """Build CASE mapping CalcWeekNo to SUM(StockInWkNN)."""
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(css.StockInWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_whstock_case_expr() -> str:
    """Build CASE mapping CalcWeekNo to cws.CloseStockWkNN."""
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(cws.CloseStockWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_ststock_case_expr() -> str:
    """Build CASE mapping CalcWeekNo to SUM(css.CloseStockWkNN) across stores."""
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(css.CloseStockWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def get_forecast_by_warehouse_week(
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    max_weeks: int = 53,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """
    Forecast by warehouse and week. Maps CalcWeekNo to ForecastWkNN, sums across stores.
    When start_from_current_week=True (default), returns 53 weeks starting from the week containing today.
    Uses: CalcStoreStock JOIN ForecastStoreSales on ItemKey+StoreKey, CASE for week mapping.
    """
    if item_key is None and item_number:
        rows, err = _query_safe(
            "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
            [item_number.strip()],
        )
        if err or not rows:
            return {"status": "error", "message": f"Item lookup failed: {err or 'not found'}", "results": []}
        item_key = int(rows[0]["ItemKey"])
    if item_key is None:
        return {"status": "error", "message": "item_number or item_key required", "results": []}

    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_forecast_case_expr()
    if start_from_current_week:
        start_week = _get_current_calc_week_no()
        week_filter = "tw.CalcWeekNo >= ?"
        top_n = 53
        params.append(start_week)
    else:
        week_filter = "tw.CalcWeekNo BETWEEN 1 AND ?"
        top_n = min(max_weeks, 53)
        params.append(top_n)
    sql = f"""
SELECT TOP ({top_n})
  css.CentralWarehouseKey,
  cw.CentralWarehouseCode,
  tw.CalcWeekNo,
  tw.YearAndWeek,
  tw.WeekStartDate,
  tw.WeekEndDate,
  SUM({case_expr}) AS Forecast
FROM fpo.tbl_CalcTimelineWeek tw
CROSS JOIN (
  SELECT DISTINCT css.CentralWarehouseKey
  FROM fpo.tbl_CalcStoreStock css
  JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
  WHERE css.ItemKey = ? {wh_filter}
) wh
INNER JOIN fpo.tbl_CalcStoreStock css
  ON css.ItemKey = ? AND css.CentralWarehouseKey = wh.CentralWarehouseKey
INNER JOIN fpo.tbl_ForecastStoreSales fss
  ON fss.ItemKey = css.ItemKey AND fss.StoreKey = css.StoreKey
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
WHERE {week_filter}
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode, tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {
        "status": "ok",
        "item_key": item_key,
        "table": "forecast_by_warehouse_week",
        "count": len(rows or []),
        "results": rows or [],
    }


def get_demand_by_warehouse_week(
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    max_weeks: int = 53,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """
    Demand by warehouse and week. Maps CalcWeekNo to StockInWkNN, sums across stores.
    When start_from_current_week=True (default), returns 53 weeks starting from the week containing today.
    Uses: CalcStoreStock only (StockInWk01..53 = store pull from warehouse per week).
    """
    if item_key is None and item_number:
        rows, err = _query_safe(
            "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
            [item_number.strip()],
        )
        if err or not rows:
            return {"status": "error", "message": f"Item lookup failed: {err or 'not found'}", "results": []}
        item_key = int(rows[0]["ItemKey"])
    if item_key is None:
        return {"status": "error", "message": "item_number or item_key required", "results": []}

    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_demand_case_expr()
    if start_from_current_week:
        start_week = _get_current_calc_week_no()
        week_filter = "tw.CalcWeekNo >= ?"
        top_n = 53
        params.append(start_week)
    else:
        week_filter = "tw.CalcWeekNo BETWEEN 1 AND ?"
        top_n = min(max_weeks, 53)
        params.append(top_n)
    sql = f"""
SELECT TOP ({top_n})
  css.CentralWarehouseKey,
  cw.CentralWarehouseCode,
  tw.CalcWeekNo,
  tw.YearAndWeek,
  tw.WeekStartDate,
  tw.WeekEndDate,
  SUM({case_expr}) AS Demand
FROM fpo.tbl_CalcTimelineWeek tw
CROSS JOIN (
  SELECT DISTINCT css.CentralWarehouseKey
  FROM fpo.tbl_CalcStoreStock css
  JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
  WHERE css.ItemKey = ? {wh_filter}
) wh
INNER JOIN fpo.tbl_CalcStoreStock css
  ON css.ItemKey = ? AND css.CentralWarehouseKey = wh.CentralWarehouseKey
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
WHERE {week_filter}
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode, tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {
        "status": "ok",
        "item_key": item_key,
        "table": "demand_by_warehouse_week",
        "count": len(rows or []),
        "results": rows or [],
    }


def get_whstock_by_warehouse_week(
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """
    WhStock by warehouse and week. Maps CalcWeekNo to CloseStockWkNN from CalcWarehouseStock.
    Source: cws.CloseStockWk01..53 per CalcWeekNo.
    """
    if item_key is None and item_number:
        rows, err = _query_safe(
            "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
            [item_number.strip()],
        )
        if err or not rows:
            return {"status": "error", "message": f"Item lookup failed: {err or 'not found'}", "results": []}
        item_key = int(rows[0]["ItemKey"])
    if item_key is None:
        return {"status": "error", "message": "item_number or item_key required", "results": []}

    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_whstock_case_expr()
    if start_from_current_week:
        start_week = _get_current_calc_week_no()
        week_filter = "tw.CalcWeekNo >= ?"
        params.append(start_week)
    else:
        week_filter = "tw.CalcWeekNo BETWEEN 1 AND 53"
    sql = f"""
SELECT TOP 53
  cws.CentralWarehouseKey,
  cw.CentralWarehouseCode,
  tw.CalcWeekNo,
  tw.YearAndWeek,
  tw.WeekStartDate,
  tw.WeekEndDate,
  {case_expr} AS WhStock
FROM fpo.tbl_CalcTimelineWeek tw
INNER JOIN fpo.tbl_CalcWarehouseStock cws ON cws.ItemKey = ?
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey {wh_filter.replace("cw.CentralWarehouseCode", "cws.CentralWarehouseKey").replace("cw.", "cws.") if central_warehouse_code else ""}
WHERE {week_filter}
ORDER BY cws.CentralWarehouseKey, tw.CalcWeekNo
"""
    # Fix: wh_filter uses cw, but we join cws. Need to filter by CentralWarehouseKey.
    # Simpler: use the same CROSS JOIN pattern as forecast
    wh_sub = "AND cw.CentralWarehouseCode = ?" if central_warehouse_code else ""
    params_fixed: List[Any] = [item_key]
    if central_warehouse_code:
        params_fixed.append(central_warehouse_code.strip())
    params_fixed.append(item_key)
    if start_from_current_week:
        params_fixed.append(_get_current_calc_week_no())

    sql = f"""
SELECT TOP 53
  cws.CentralWarehouseKey,
  cw.CentralWarehouseCode,
  tw.CalcWeekNo,
  tw.YearAndWeek,
  tw.WeekStartDate,
  tw.WeekEndDate,
  {case_expr} AS WhStock
FROM fpo.tbl_CalcTimelineWeek tw
CROSS JOIN (
  SELECT DISTINCT cws.CentralWarehouseKey
  FROM fpo.tbl_CalcWarehouseStock cws
  JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey
  WHERE cws.ItemKey = ? {wh_filter}
) wh
INNER JOIN fpo.tbl_CalcWarehouseStock cws ON cws.ItemKey = ? AND cws.CentralWarehouseKey = wh.CentralWarehouseKey
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey
WHERE {week_filter}
ORDER BY cws.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {
        "status": "ok",
        "item_key": item_key,
        "table": "whstock_by_warehouse_week",
        "count": len(rows or []),
        "results": rows or [],
    }


def get_ststock_by_warehouse_week(
    item_number: Optional[str] = None,
    item_key: Optional[int] = None,
    central_warehouse_code: Optional[str] = None,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """
    StStock by warehouse and week. Maps CalcWeekNo to SUM(CloseStockWkNN) across stores.
    Source: CalcStoreStock.CloseStockWk01..53, summed per warehouse.
    """
    if item_key is None and item_number:
        rows, err = _query_safe(
            "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
            [item_number.strip()],
        )
        if err or not rows:
            return {"status": "error", "message": f"Item lookup failed: {err or 'not found'}", "results": []}
        item_key = int(rows[0]["ItemKey"])
    if item_key is None:
        return {"status": "error", "message": "item_number or item_key required", "results": []}

    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_ststock_case_expr()
    if start_from_current_week:
        start_week = _get_current_calc_week_no()
        week_filter = "tw.CalcWeekNo >= ?"
        params.append(start_week)
    else:
        week_filter = "tw.CalcWeekNo BETWEEN 1 AND 53"
    sql = f"""
SELECT TOP 53
  css.CentralWarehouseKey,
  cw.CentralWarehouseCode,
  tw.CalcWeekNo,
  tw.YearAndWeek,
  tw.WeekStartDate,
  tw.WeekEndDate,
  SUM({case_expr}) AS StStock
FROM fpo.tbl_CalcTimelineWeek tw
CROSS JOIN (
  SELECT DISTINCT css.CentralWarehouseKey
  FROM fpo.tbl_CalcStoreStock css
  JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
  WHERE css.ItemKey = ? {wh_filter}
) wh
INNER JOIN fpo.tbl_CalcStoreStock css
  ON css.ItemKey = ? AND css.CentralWarehouseKey = wh.CentralWarehouseKey
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = css.CentralWarehouseKey
WHERE {week_filter}
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode, tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {
        "status": "ok",
        "item_key": item_key,
        "table": "ststock_by_warehouse_week",
        "count": len(rows or []),
        "results": rows or [],
    }


def get_calc_timeline_day(
    calc_week_no: Optional[int] = None,
    year_and_week: Optional[int] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    """Fetch daily calculation timeline from fpo.tbl_CalcTimelineDay."""
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
    """Fetch weekly calculation timeline from fpo.tbl_CalcTimelineWeek."""
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
    """
    Fetch store-level calc state from fpo.tbl_CalcStoreStock.
    Columns include StockInWk01..53 (demand), CloseStockWk00..53 (closing stock).
    """
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
    """
    Fetch warehouse-level calc state from fpo.tbl_CalcWarehouseStock.
    Columns include CloseStockWk00..53 (warehouse closing stock per week).
    """
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
    """
    Fetch item/warehouse config from fpo.tbl_ItemWarehouse.
    Includes CategoryABC, ReqPO, SafetyStockQty, ShipLT, TotalLT.
    """
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
    """Fetch order quantity controls from fpo.tbl_ItemWarehouseOrderQty (AOQ, EOQ, LOQ, SOQ)."""
    return _get_table_rows(
        "fpo.tbl_ItemWarehouseOrderQty",
        top_n=top_n,
        filters=[
            ("ItemKey", item_key),
            ("CentralWarehouseKey", central_warehouse_key),
        ],
    )


def get_warehouse_order(
    item_key: Optional[int] = None,
    central_warehouse_key: Optional[int] = None,
    top_n: int = 200,
) -> Dict[str, Any]:
    """Fetch existing warehouse orders from fpo.tbl_WarehouseOrder."""
    return _get_table_rows(
        "fpo.tbl_WarehouseOrder",
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
    """
    Fetch delivery/req-post calendar from fpo.tbl_ItemWarehouseLeadtime.
    Columns include DeliveryDateWk01..20, BlockReasonWk01..20, ReqPostDateWk01..20.
    """
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
    """Fetch store cover config from fpo.tbl_ConfigStoreCover."""
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
    """Fetch warehouse cover config from fpo.tbl_ConfigWarehouseCover."""
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
    """Fetch import cover config from fpo.tbl_ImportCoverConfig."""
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
    """Fetch job control state from am.tbl_JobControl."""
    return _get_table_rows(
        "am.tbl_JobControl",
        top_n=top_n,
        filters=[("JobName", job_name)],
    )


def get_job_control_history(
    job_name: Optional[str] = None,
    top_n: int = 100,
) -> Dict[str, Any]:
    """Fetch job run history from am.tbl_JobControlHistory."""
    return _get_table_rows(
        "am.tbl_JobControlHistory",
        top_n=top_n,
        filters=[("JobName", job_name)],
    )


def get_item_ordering_data(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
    max_weeks: int = 53,
) -> Dict[str, Any]:
    """
    Fetch ALL raw tables for an item in ONE call. Use this first for core reordering
    to minimize tool calls and avoid rate limits. Returns simple SELECTs only — no joins.
    You must aggregate and reason from the raw rows yourself.
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

    # ItemWarehouse
    if wh_key is not None:
        tables["fpo_tbl_ItemWarehouse"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouse WHERE ItemKey = ? AND CentralWarehouseKey = ?",
            [item_key, wh_key],
        )
    else:
        tables["fpo_tbl_ItemWarehouse"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouse WHERE ItemKey = ?",
            [item_key],
        )

    # ItemWarehouseOrderQty
    tables["fpo_tbl_ItemWarehouseOrderQty"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ItemWarehouseOrderQty WHERE ItemKey = ?",
        [item_key],
    )

    # CalcWarehouseStock — filter by warehouse when provided for correct whstock
    cws_params: List[Any] = [item_key]
    cws_wh_filter = ""
    if wh_key is not None:
        cws_wh_filter = " AND CentralWarehouseKey = ?"
        cws_params.append(wh_key)
    tables["fpo_tbl_CalcWarehouseStock"], _ = _query_safe(
        f"SELECT ItemKey, CentralWarehouseKey, CloseStockWk00 FROM fpo.tbl_CalcWarehouseStock WHERE ItemKey = ?{cws_wh_filter}",
        cws_params,
    )

    # CalcStoreStock — excluded to reduce payload; forecast/demand are already aggregated by warehouse/week

    # Forecast by warehouse/week — 53 weeks from current week (week 1 = week we are in)
    fc_result = get_forecast_by_warehouse_week(
        item_key=item_key, central_warehouse_code=central_warehouse_code,
        max_weeks=53, start_from_current_week=True
    )
    tables["forecast_by_warehouse_week"] = fc_result.get("results") or []

    # Demand by warehouse/week — 53 weeks from current week
    dm_result = get_demand_by_warehouse_week(
        item_key=item_key, central_warehouse_code=central_warehouse_code,
        max_weeks=53, start_from_current_week=True
    )
    tables["demand_by_warehouse_week"] = dm_result.get("results") or []

    # WhStock by warehouse/week — CloseStockWkNN from CalcWarehouseStock per CalcWeekNo
    wh_result = get_whstock_by_warehouse_week(
        item_key=item_key, central_warehouse_code=central_warehouse_code,
        start_from_current_week=True
    )
    tables["whstock_by_warehouse_week"] = wh_result.get("results") or []

    # StStock by warehouse/week — SUM(CloseStockWkNN) across stores per CalcWeekNo
    st_result = get_ststock_by_warehouse_week(
        item_key=item_key, central_warehouse_code=central_warehouse_code,
        start_from_current_week=True
    )
    tables["ststock_by_warehouse_week"] = st_result.get("results") or []

    # ItemWarehouseLeadtime
    tables["fpo_tbl_ItemWarehouseLeadtime"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ItemWarehouseLeadtime WHERE ItemKey = ?",
        [item_key],
    )


    # CalcTimelineWeek — 53 weeks from current week (week 1 = week we are in)
    start_week = _get_current_calc_week_no()
    tables["fpo_tbl_CalcTimelineWeek"], _ = _query_safe(
        "SELECT TOP 53 CalcWeekNo, YearAndWeek, WeekStartDate, WeekEndDate "
        "FROM fpo.tbl_CalcTimelineWeek WHERE CalcWeekNo >= ? ORDER BY CalcWeekNo",
        [start_week],
    )

    # Cover configs (current week)
    tables["fpo_tbl_ConfigStoreCover"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ConfigStoreCover WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
    )
    tables["fpo_tbl_ConfigWarehouseCover"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ConfigWarehouseCover WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
    )
    tables["fpo_tbl_ImportCoverConfig"], _ = _query_safe(
        "SELECT * FROM fpo.tbl_ImportCoverConfig WHERE WeekOfYear = DATEPART(ISO_WEEK, GETDATE())",
    )

    current_week = _get_current_calc_week_no()
    return {
        "status": "ok",
        "item_number": item_number,
        "item_key": item_key,
        "current_calc_week_no": current_week,
        "tables": {k: v for k, v in tables.items() if v is not None},
        "counts": {k: len(v or []) for k, v in tables.items()},
    }


def get_core_ordering_schema_reference() -> Dict[str, Any]:
    """
    Returns the canonical table reference for core re-ordering.
    Use this to understand available tables and their purpose before querying.
    """
    return {
        "database": "PlanningToolsDb",
        "batch_tool": "get_item_ordering_data(item_number, central_warehouse_code?) — fetches all reorder tables in ONE call",
        "forecast_tool": "get_forecast_by_warehouse_week — maps CalcWeekNo to ForecastWkNN, sums across stores (css JOIN fss)",
        "demand_tool": "get_demand_by_warehouse_week — maps CalcWeekNo to StockInWkNN, sums across stores (css only)",
        "whstock_tool": "whstock_by_warehouse_week — maps CalcWeekNo to CloseStockWkNN from CalcWarehouseStock",
        "ststock_tool": "ststock_by_warehouse_week — maps CalcWeekNo to SUM(CloseStockWkNN) across stores from CalcStoreStock",
        "tables": {
            "bicache.tbl_Item": "Item master — dimensions, carton/pallet sizes, MOQ, country of origin",
            "bicache.tbl_CentralWarehouse": "Warehouse master — codes and keys",
            "model.tbl_StockWarehouseOnHand": "Current warehouse on-hand stock",
            "model.tbl_StockStoreOnHand": "Current store on-hand stock",
            "model.tbl_StoreWarehouseRelationship": "Store-to-warehouse mapping with validity dates",
            "model.tbl_CoreAssortment": "Core assortment by item/warehouse",
            "fpo.tbl_ForecastStoreSales": "Per-store weekly forecast (ForecastWk01..53)",
            "fpo.tbl_CalcTimelineDay": "Daily calc timeline — CalcDate, YearAndWeek, CalcWeekNo",
            "fpo.tbl_CalcTimelineWeek": "Weekly calc timeline — CalcWeekNo, YearAndWeek, WeekStartDate, WeekEndDate",
            "fpo.tbl_CalcStoreStock": "Store calc state — StockInWk01..53 (demand), CloseStockWk00..53",
            "fpo.tbl_CalcWarehouseStock": "Warehouse calc state — CloseStockWk00..53 (closing stock per week)",
            "fpo.tbl_ItemWarehouse": "Item/warehouse config — CategoryABC, ReqPO, SafetyStockQty, ShipLT, TotalLT",
            "fpo.tbl_ItemWarehouseOrderQty": "Order quantity controls — OrderQtyType, AOQ, EOQ, LOQ, SOQ",
            "fpo.tbl_ItemWarehouseLeadtime": "Lead time calendar — DeliveryDateWk01..20, BlockReasonWk01..20, ReqPostDateWk01..20",
            "fpo.tbl_ConfigStoreCover": "Store cover weeks by ABC and week-of-year",
            "fpo.tbl_ConfigWarehouseCover": "Warehouse cover weeks by ABC and week-of-year",
            "fpo.tbl_ImportCoverConfig": "Import cover weeks by warehouse/ABC/week-of-year",
            "am.tbl_JobControl": "Job control state",
            "am.tbl_JobControlHistory": "Job run history",
        },
        "notes": [
            "For core reordering: call get_item_ordering_data(item_number) first — one call returns all tables.",
            "Agent must perform all reasoning and aggregation from raw table data.",
            "StockInWk columns in CalcStoreStock = demand pulled from warehouse to stores.",
            "CloseStockWk00 in CalcWarehouseStock = opening (current) warehouse stock.",
            "Week alignment: forecast, demand, CalcTimelineWeek start from current week. week_index 1 = current week (e.g. CalcWeekNo 12). Always 53 weeks.",
        ],
    }


def run_planning_tools_readonly_query(sql_query: str) -> Dict[str, Any]:
    """
    Execute a read-only SQL query (SELECT/CTE only) against PlanningToolsDB.
    Use for custom queries not covered by the dedicated table-fetch tools.
    """
    _ensure_read_only_sql(sql_query)
    rows = _query(sql_query)
    return {
        "count": len(rows),
        "results": rows[:500],
        "truncated": len(rows) > 500,
    }


def validate_proposal(
    quantity: int,
    reorderPoint: int,
    casePack: int,
    moq: int = 0,
) -> Dict[str, Any]:
    """
    Enforce reorder constraints and return the adjusted quantity.
    Rounds up to the nearest casePack multiple, respecting reorderPoint and MOQ.
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
