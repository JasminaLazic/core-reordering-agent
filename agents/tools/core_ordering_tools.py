"""
Tools for core re-ordering analysis against PlanningToolsDB.

These tools are intentionally read-only and designed for agent use.
"""
import os
import math
from datetime import datetime, timezone
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
    # Preferred: full connection string in env
    full = os.environ.get("PLANNING_TOOLS_SQL_CONNECTION_STRING")
    if full:
        return full

    server = os.environ.get("PLANNING_TOOLS_SQL_SERVER")
    database = os.environ.get("PLANNING_TOOLS_SQL_DATABASE", "PlanningToolsDb")
    username = os.environ.get("PLANNING_TOOLS_SQL_USERNAME")
    password = os.environ.get("PLANNING_TOOLS_SQL_PASSWORD")
    driver = os.environ.get("PLANNING_TOOLS_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

    if not server:
        raise RuntimeError(
            "Missing PlanningToolsDB SQL config. Set either "
            "PLANNING_TOOLS_SQL_CONNECTION_STRING or "
            "PLANNING_TOOLS_SQL_SERVER (+ auth variables)."
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
            "fpo.tbl_WarehouseOrder": "Recommended/stored/posted/actual warehouse orders",
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
            "fpo.tvf_OrderLoad": "Optional object for output-shape comparison",
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
    Build a normalized, deterministic payload for the FPO core reordering agent.
    This function is read-only and mirrors deterministic data preparation only.
    """
    if week_start < 1 or week_start > 53:
        raise ValueError("week_start must be between 1 and 53")
    limit = max(1, min(top_n, 2000))

    now_iso = _to_iso8601_utc_now()
    iteration = iteration_id or f"core_reorder_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    filter_params: List[Any] = []
    warehouse_where: List[str] = []
    store_where: List[str] = []
    orders_where: List[str] = []
    constraints_where: List[str] = []
    issue_where: List[str] = []

    item_join = ""
    if item_number:
        item_join = "JOIN bicache.tbl_Item i ON cws.ItemKey = i.ItemKey"
        warehouse_where.append("i.ItemNumber = ?")
        store_where.append("i.ItemNumber = ?")
        orders_where.append("i.ItemNumber = ?")
        constraints_where.append("i.ItemNumber = ?")
        issue_where.append("i.ItemNumber = ?")
        filter_params.append(item_number)

    if item_key is not None:
        warehouse_where.append("cws.ItemKey = ?")
        store_where.append("css.ItemKey = ?")
        orders_where.append("wo.ItemKey = ?")
        constraints_where.append("iw.ItemKey = ?")
        issue_where.append("iw.ItemKey = ?")
        filter_params.append(item_key)

    warehouse_where_sql = ("WHERE " + " AND ".join(warehouse_where)) if warehouse_where else ""
    store_where_sql = ("WHERE " + " AND ".join(store_where)) if store_where else ""
    orders_where_sql = ("WHERE " + " AND ".join(orders_where)) if orders_where else ""
    constraints_where_sql = ("WHERE " + " AND ".join(constraints_where)) if constraints_where else ""
    issue_where_sql = ("WHERE " + " AND ".join(issue_where)) if issue_where else ""

    timeline_sql = """
SELECT
    CalcWeekNo,
    YearAndWeek,
    WeekStartDate,
    WeekEndDate
FROM fpo.tbl_CalcTimelineWeek
WHERE CalcWeekNo BETWEEN 1 AND 53
ORDER BY CalcWeekNo
"""
    timeline_rows, timeline_err = _query_safe(timeline_sql)

    warehouse_sql = f"""
SELECT TOP ({limit})
    cws.*
FROM fpo.tbl_CalcWarehouseStock cws
{item_join}
{warehouse_where_sql}
ORDER BY cws.ItemKey, cws.CentralWarehouseKey
"""
    warehouse_rows, warehouse_err = _query_safe(warehouse_sql, filter_params if filter_params else None)

    store_sql = f"""
SELECT TOP ({limit})
    css.*
FROM fpo.tbl_CalcStoreStock css
LEFT JOIN bicache.tbl_Item i ON css.ItemKey = i.ItemKey
{store_where_sql}
ORDER BY css.ItemKey, css.StoreKey, css.CentralWarehouseKey
"""
    store_rows, store_err = _query_safe(store_sql, filter_params if filter_params else None)

    order_sql = f"""
SELECT TOP ({limit})
    wo.*
FROM fpo.tbl_WarehouseOrder wo
LEFT JOIN bicache.tbl_Item i ON wo.ItemKey = i.ItemKey
{orders_where_sql}
ORDER BY wo.ItemKey, wo.CentralWarehouseKey, wo.DeliveryDate
"""
    order_rows, order_err = _query_safe(order_sql, filter_params if filter_params else None)

    constraints_sql = f"""
SELECT TOP ({limit})
    iw.ItemKey,
    iw.CentralWarehouseKey,
    iw.ReqPO,
    iw.SafetyStockQty,
    iw.MOQ,
    iw.CentralWarehouseCode,
    iw.CentralWarehouseCountryCode,
    oq.OrderQtyType,
    COALESCE(oq.AOQ, 0) AS AOQ,
    COALESCE(oq.EOQ, 0) AS EOQ,
    COALESCE(oq.LOQ, 0) AS LOQ,
    COALESCE(oq.SOQ, 0) AS SOQ,
    COALESCE(iw.NumberOfUnitsPerParcelForStore, 0) AS NumberOfUnitsPerParcelForStore,
    COALESCE(iw.NumberOfUnitsPerParcelForPurchase, 0) AS NumberOfUnitsPerParcelForPurchase,
    COALESCE(iw.NumberOfUnitsPerPallet, 0) AS NumberOfUnitsPerPallet
FROM fpo.tbl_ItemWarehouse iw
LEFT JOIN fpo.tbl_ItemWarehouseOrderQty oq
    ON oq.ItemKey = iw.ItemKey
    AND oq.CentralWarehouseKey = iw.CentralWarehouseKey
LEFT JOIN bicache.tbl_Item i ON iw.ItemKey = i.ItemKey
{constraints_where_sql}
ORDER BY iw.ItemKey, iw.CentralWarehouseKey
"""
    constraints_rows, constraints_err = _query_safe(constraints_sql, filter_params if filter_params else None)

    issues_sql = f"""
SELECT TOP ({limit})
    iw.ItemKey,
    iw.CentralWarehouseKey,
    iw.HasAnyIssue,
    iw.HasCalcIssue,
    iw.HasPostIssue
FROM fpo.tbl_ItemWarehouse iw
LEFT JOIN bicache.tbl_Item i ON iw.ItemKey = i.ItemKey
{issue_where_sql}
ORDER BY iw.ItemKey, iw.CentralWarehouseKey
"""
    issue_rows, issue_err = _query_safe(issues_sql, filter_params if filter_params else None)

    errors: List[Dict[str, Any]] = []
    if timeline_err:
        errors.append({"section": "timeline", "error": timeline_err})
    if warehouse_err:
        errors.append({"section": "item_warehouse_state", "error": warehouse_err})
    if store_err:
        errors.append({"section": "item_store_state", "error": store_err})
    if order_err:
        errors.append({"section": "existing_orders", "error": order_err})
    if constraints_err:
        errors.append({"section": "constraints", "error": constraints_err})
    if issue_err:
        errors.append({"section": "data_quality_flags", "error": issue_err})

    if not timeline_rows:
        errors.append({"section": "timeline", "error": "No timeline rows found"})
    if not warehouse_rows:
        errors.append({"section": "item_warehouse_state", "error": "No calc warehouse rows found"})

    blocking_error_actions: List[Dict[str, Any]] = []
    if errors:
        blocking_error_actions.append(
            {
                "action": "error",
                "reason_code": "PRE",
                "error_code": "MISSING_REQUIRED_INPUTS",
                "message": "Fail-closed: required payload sections are missing or unreadable.",
                "details": errors,
                "run_id": iteration,
                "iteration_id": iteration,
                "timestamp_utc": now_iso,
            }
        )

    return {
        "run_context": {
            "item_scope": item_scope,
            "item_number": item_number,
            "item_key": item_key,
            "week_start": week_start,
            "today": now_iso[:10],
            "iteration_id": iteration,
            "strict_mode": True,
            "required_step_sequence": ["W1", "S2", "W3", "R4", "S5", "C6", "R7"],
        },
        "timeline": timeline_rows,
        "item_warehouse_state": warehouse_rows,
        "item_store_state": store_rows,
        "existing_orders": order_rows,
        "constraints": constraints_rows,
        "data_quality_flags": issue_rows,
        "blocking_error_actions": blocking_error_actions,
        "payload_errors": errors,
        "meta": {
            "counts": {
                "timeline": len(timeline_rows),
                "item_warehouse_state": len(warehouse_rows),
                "item_store_state": len(store_rows),
                "existing_orders": len(order_rows),
                "constraints": len(constraints_rows),
                "data_quality_flags": len(issue_rows),
            },
            "generated_at_utc": now_iso,
            "source": "deterministic_prepared_payload",
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

