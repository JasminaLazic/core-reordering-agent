"""
Core re-ordering tools for PlanningToolsDB.

Agent tools (registered with the agent):
  get_item_ordering_data  — one-shot batch fetch for a single item/warehouse
  get_fpo_source_table    — whitelist-based lookup of any FPO raw source table

Internal helpers (used only by the above, not exposed to the agent):
  get_forecast_by_warehouse_week, get_demand_by_warehouse_week,
  get_whstock_by_warehouse_week, get_ststock_by_warehouse_week

API-only (used by api.py endpoints, not registered with the agent):
  cursor_simulation, validate_proposal
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


def _query(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    return _query_sqlserver(sql, params)


def _query_safe(
    sql: str, params: Optional[List[Any]] = None
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        return _query(sql, params), None
    except Exception as e:
        return [], str(e)


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
# CASE expression builders (used by the four internal weekly series fetchers)
# ---------------------------------------------------------------------------

def _build_forecast_case_expr() -> str:
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(fss.ForecastWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_demand_case_expr() -> str:
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(css.StockInWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_whstock_case_expr() -> str:
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(cws.CloseStockWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


def _build_ststock_case_expr() -> str:
    cases = [
        f"WHEN tw.CalcWeekNo = {i} THEN COALESCE(css.CloseStockWk{i:02d}, 0)"
        for i in range(1, 54)
    ]
    return "CASE " + " ".join(cases) + " ELSE 0 END"


# ---------------------------------------------------------------------------
# Internal weekly series fetchers (called by get_item_ordering_data /
# cursor_simulation — not registered as agent tools)
# ---------------------------------------------------------------------------

def get_forecast_by_warehouse_week(
    item_key: int,
    central_warehouse_code: Optional[str] = None,
    max_weeks: int = 53,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """Forecast per warehouse/week — SUM(ForecastWkNN) across stores."""
    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_forecast_case_expr()
    if start_from_current_week:
        week_filter = "tw.CalcWeekNo >= ?"
        top_n = 53
        params.append(_get_current_calc_week_no())
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
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode,
         tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {"status": "ok", "item_key": item_key, "count": len(rows or []), "results": rows or []}


def get_demand_by_warehouse_week(
    item_key: int,
    central_warehouse_code: Optional[str] = None,
    max_weeks: int = 53,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """Demand per warehouse/week — SUM(StockInWkNN) across stores."""
    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_demand_case_expr()
    if start_from_current_week:
        week_filter = "tw.CalcWeekNo >= ?"
        top_n = 53
        params.append(_get_current_calc_week_no())
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
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode,
         tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {"status": "ok", "item_key": item_key, "count": len(rows or []), "results": rows or []}


def get_whstock_by_warehouse_week(
    item_key: int,
    central_warehouse_code: Optional[str] = None,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """Warehouse closing stock per week — CloseStockWkNN from CalcWarehouseStock."""
    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_whstock_case_expr()
    if start_from_current_week:
        week_filter = "tw.CalcWeekNo >= ?"
        params.append(_get_current_calc_week_no())
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
CROSS JOIN (
  SELECT DISTINCT cws.CentralWarehouseKey
  FROM fpo.tbl_CalcWarehouseStock cws
  JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey
  WHERE cws.ItemKey = ? {wh_filter}
) wh
INNER JOIN fpo.tbl_CalcWarehouseStock cws
  ON cws.ItemKey = ? AND cws.CentralWarehouseKey = wh.CentralWarehouseKey
JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = cws.CentralWarehouseKey
WHERE {week_filter}
ORDER BY cws.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {"status": "ok", "item_key": item_key, "count": len(rows or []), "results": rows or []}


def get_ststock_by_warehouse_week(
    item_key: int,
    central_warehouse_code: Optional[str] = None,
    start_from_current_week: bool = True,
) -> Dict[str, Any]:
    """Store closing stock per warehouse/week — SUM(CloseStockWkNN) across stores."""
    wh_filter = ""
    params: List[Any] = [item_key]
    if central_warehouse_code:
        wh_filter = " AND cw.CentralWarehouseCode = ?"
        params.append(central_warehouse_code.strip())
    params.append(item_key)

    case_expr = _build_ststock_case_expr()
    if start_from_current_week:
        week_filter = "tw.CalcWeekNo >= ?"
        params.append(_get_current_calc_week_no())
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
GROUP BY css.CentralWarehouseKey, cw.CentralWarehouseCode,
         tw.CalcWeekNo, tw.YearAndWeek, tw.WeekStartDate, tw.WeekEndDate
ORDER BY css.CentralWarehouseKey, tw.CalcWeekNo
"""
    rows, err = _query_safe(sql, params)
    if err:
        return {"status": "error", "message": str(err), "results": []}
    return {"status": "ok", "item_key": item_key, "count": len(rows or []), "results": rows or []}


# ---------------------------------------------------------------------------
# AGENT TOOL 1: get_item_ordering_data
# ---------------------------------------------------------------------------

def get_item_ordering_data(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
    include_all_warehouses: bool = False,
) -> Dict[str, Any]:
    """
    Fetch ALL data needed for a core reorder recommendation in ONE call.

    Returns item master/config, demand, forecast, warehouse/store closing stock,
    cover configs, leadtime calendar, and a 53-week timeline starting from today.
    Warehouse orders (tbl_WarehouseOrder) are intentionally excluded — this is the
    recommendation (no-orders) scenario.

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
    if wh_key is not None:
        tables["fpo_tbl_ItemWarehouse"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouse WHERE ItemKey = ? AND CentralWarehouseKey = ?",
            [item_key, wh_key],
        )
    else:
        tables["fpo_tbl_ItemWarehouse"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouse WHERE ItemKey = ?", [item_key],
        )

    # ItemWarehouseOrderQty (AOQ, EOQ, LOQ, SOQ, carton/pallet sizes)
    if wh_key is not None:
        tables["fpo_tbl_ItemWarehouseOrderQty"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouseOrderQty WHERE ItemKey = ? AND CentralWarehouseKey = ?",
            [item_key, wh_key],
        )
    else:
        tables["fpo_tbl_ItemWarehouseOrderQty"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouseOrderQty WHERE ItemKey = ?", [item_key],
        )

    # CalcWarehouseStock — opening stock (CloseStockWk00) only
    cws_params: List[Any] = [item_key]
    cws_wh_filter = ""
    if wh_key is not None:
        cws_wh_filter = " AND CentralWarehouseKey = ?"
        cws_params.append(wh_key)
    tables["fpo_tbl_CalcWarehouseStock"], _ = _query_safe(
        f"SELECT ItemKey, CentralWarehouseKey, CloseStockWk00 "
        f"FROM fpo.tbl_CalcWarehouseStock WHERE ItemKey = ?{cws_wh_filter}",
        cws_params,
    )

    # 53-week series (forecast, demand, store stock, warehouse stock reference)
    tables["forecast_by_warehouse_week"] = (
        get_forecast_by_warehouse_week(
            item_key=item_key, central_warehouse_code=central_warehouse_code,
            start_from_current_week=True,
        ).get("results") or []
    )
    tables["demand_by_warehouse_week"] = (
        get_demand_by_warehouse_week(
            item_key=item_key, central_warehouse_code=central_warehouse_code,
            start_from_current_week=True,
        ).get("results") or []
    )
    tables["ststock_by_warehouse_week"] = (
        get_ststock_by_warehouse_week(
            item_key=item_key, central_warehouse_code=central_warehouse_code,
            start_from_current_week=True,
        ).get("results") or []
    )
    tables["whstock_by_warehouse_week_ref"] = (
        get_whstock_by_warehouse_week(
            item_key=item_key, central_warehouse_code=central_warehouse_code,
            start_from_current_week=True,
        ).get("results") or []
    )

    # ItemWarehouseLeadtime (DeliveryDateWk01..53, BlockReasonWk01..53)
    if wh_key is not None:
        tables["fpo_tbl_ItemWarehouseLeadtime"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouseLeadtime WHERE ItemKey = ? AND CentralWarehouseKey = ?",
            [item_key, wh_key],
        )
    else:
        tables["fpo_tbl_ItemWarehouseLeadtime"], _ = _query_safe(
            "SELECT * FROM fpo.tbl_ItemWarehouseLeadtime WHERE ItemKey = ?", [item_key],
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

    result: Dict[str, Any] = {
        "status": "ok",
        "item_number": item_number,
        "item_key": item_key,
        "current_calc_week_no": start_week,
        "warehouse_code": central_warehouse_code,
        "orders_excluded": True,
        "tables": {k: v for k, v in tables.items() if v is not None},
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
# API-only: cursor_simulation, validate_proposal
# (used by api.py endpoints — not registered as agent tools)
# ---------------------------------------------------------------------------

def cursor_simulation(
    item_number: str,
    warehouse_code: str,
    hypothetical_orders: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Step week-by-week through projected warehouse stock with optional order injections.

    Fetches opening stock (CloseStockWk00), 53-week demand and forecast, and the
    calendar from the DB, then simulates closing stock week by week.

    hypothetical_orders: list of {"week_index": int (1-53), "quantity": int/float}
      Multiple entries for the same week_index are summed.
      week_index=1 is the current week (demand = 0 per business rules).
    """
    if not (item_number or "").strip():
        return {"status": "error", "message": "item_number required"}
    if not (warehouse_code or "").strip():
        return {"status": "error", "message": "warehouse_code required"}

    item_rows, err = _query_safe(
        "SELECT TOP 1 ItemKey FROM bicache.tbl_Item WHERE ItemNumber = ?",
        [item_number.strip()],
    )
    if err or not item_rows:
        return {"status": "error", "message": f"Item '{item_number}' not found: {err or 'not found'}"}
    item_key = int(item_rows[0]["ItemKey"])

    wh_rows, err = _query_safe(
        "SELECT TOP 1 CentralWarehouseKey FROM bicache.tbl_CentralWarehouse WHERE CentralWarehouseCode = ?",
        [warehouse_code.strip()],
    )
    if err or not wh_rows:
        return {"status": "error", "message": f"Warehouse '{warehouse_code}' not found: {err or 'not found'}"}
    wh_key = int(wh_rows[0]["CentralWarehouseKey"])

    stock_rows, err = _query_safe(
        "SELECT TOP 1 CloseStockWk00 FROM fpo.tbl_CalcWarehouseStock "
        "WHERE ItemKey = ? AND CentralWarehouseKey = ?",
        [item_key, wh_key],
    )
    if err or not stock_rows:
        return {"status": "error", "message": f"No warehouse stock found: {err or 'not found'}"}
    opening_stock = float(stock_rows[0].get("CloseStockWk00") or 0)

    demand_by_cwn: Dict[int, float] = {
        int(r["CalcWeekNo"]): float(r.get("Demand") or 0)
        for r in (get_demand_by_warehouse_week(
            item_key=item_key, central_warehouse_code=warehouse_code,
            start_from_current_week=True,
        ).get("results") or [])
    }
    forecast_by_cwn: Dict[int, float] = {
        int(r["CalcWeekNo"]): float(r.get("Forecast") or 0)
        for r in (get_forecast_by_warehouse_week(
            item_key=item_key, central_warehouse_code=warehouse_code,
            start_from_current_week=True,
        ).get("results") or [])
    }

    start_week = _get_current_calc_week_no()
    timeline_rows, err = _query_safe(
        "SELECT TOP 53 CalcWeekNo, YearAndWeek, WeekStartDate, WeekEndDate "
        "FROM fpo.tbl_CalcTimelineWeek WHERE CalcWeekNo >= ? ORDER BY CalcWeekNo",
        [start_week],
    )
    if err or not timeline_rows:
        return {"status": "error", "message": f"Timeline fetch failed: {err or 'empty'}"}

    injected: Dict[int, float] = {}
    for order in (hypothetical_orders or []):
        wi = int(order.get("week_index", 0))
        qty = float(order.get("quantity", 0))
        if 1 <= wi <= 53 and qty > 0:
            injected[wi] = injected.get(wi, 0.0) + qty

    stock = opening_stock
    trajectory: List[Dict[str, Any]] = []
    stockout_weeks: List[int] = []

    for i, week in enumerate(timeline_rows):
        week_index = i + 1
        cwn        = int(week["CalcWeekNo"])
        demand     = 0.0 if week_index == 1 else demand_by_cwn.get(cwn, 0.0)
        forecast   = forecast_by_cwn.get(cwn, 0.0)
        qty_in     = injected.get(week_index, 0.0)
        stock      = stock + qty_in - demand
        rounded    = round(stock, 2)

        trajectory.append({
            "week_index":       week_index,
            "calc_week_no":     cwn,
            "year_week":        str(week.get("YearAndWeek", "")),
            "week_start_date":  str(week.get("WeekStartDate", "")),
            "week_end_date":    str(week.get("WeekEndDate", "")),
            "forecast":         round(forecast, 4),
            "demand":           round(demand, 4),
            "injected_quantity": qty_in,
            "closing_stock":    rounded,
        })
        if rounded < 0:
            stockout_weeks.append(week_index)

    min_stock = min((e["closing_stock"] for e in trajectory), default=0.0)
    return {
        "status": "ok",
        "item_number": item_number,
        "item_key": item_key,
        "warehouse_code": warehouse_code,
        "opening_stock": opening_stock,
        "hypothetical_orders": hypothetical_orders or [],
        "total_injected": sum(injected.values()),
        "trajectory": trajectory,
        "summary": {
            "weeks_simulated":    len(trajectory),
            "stockout_weeks":     stockout_weeks,
            "min_closing_stock":  round(min_stock, 2),
            "first_stockout_week": stockout_weeks[0] if stockout_weeks else None,
        },
    }


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
