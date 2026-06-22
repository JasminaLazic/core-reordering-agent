"""
Rules management and deterministic simulation tools.

Agent tools (registered with the agent):
  get_current_rules              — show active rules config + version info
  update_rules                   — merge param changes into active config, save new version
  rollback_rules                 — activate a specific historical version
  list_rules_history             — list all versions with metadata
  execute_reordering_with_rules  — deterministic simulation using active rules
"""

import json
import math
from typing import Any, Dict, List, Optional

from agents.tools.core_ordering_tools import (
    _build_connection_string,
    _get_pyodbc,
    _query_safe,
    _query_sqlserver,
    get_item_ordering_data,
)

# ---------------------------------------------------------------------------
# Default rules config — mirrors current stored procedure baseline
# ---------------------------------------------------------------------------

DEFAULT_RULES_CONFIG: Dict[str, Any] = {
    "simulation_weeks": 53,
    "cover_weeks": None,              # None = use DB (fpo_tbl_ImportCoverConfig); int = global override
    "req_po": None,                   # None = use DB ReqPO per item; True/False = force globally
    "safety_stock_multiplier": 1.0,   # multiplier on DB SafetyStockQty (1.0 = no change)
    "demand_scale_factor": 1.0,
    "ignore_moq": False,
    "order_qty_type": None,           # None = use DB per item; "A"/"E"/"L"/"S"/"C" = force globally
    "rounding_override": None,        # None = auto per item; "P"/"S"/"C"/"U" = force globally
    "block_cny": True,                # True = respect CNY blocks; False = ignore them
    "abc_classes_to_process": ["A", "B", "C"],
}

RULES_SCHEMA_DESCRIPTION = """
Rule parameters (use update_rules to change):
  simulation_weeks (int 1-53)          Weeks to simulate. Default: 53
  cover_weeks (int | null)             Weeks of cover for order sizing. null = use DB config per item
  req_po (bool | null)                 Force ordering on/off globally. null = use DB ReqPO per item
  safety_stock_multiplier (float)      Multiplier on DB SafetyStockQty. Default: 1.0
  demand_scale_factor (float)          Multiply all demand/stockin values. Default: 1.0
  ignore_moq (bool)                    Skip MOQ validation globally. Default: false
  order_qty_type (null|"A"|"E"|"L"|"S"|"C")  Force order qty type globally. null = use DB per item
  rounding_override (null|"P"|"S"|"C"|"U")   Force rounding type globally. null = auto per item
  block_cny (bool)                     Respect CNY blocks. Default: true
  abc_classes_to_process (array)       ABC classes to include. Default: ["A","B","C"]
"""

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
IF OBJECT_ID('config.tbl_AgentRulesHistory', 'U') IS NULL
    CREATE TABLE config.tbl_AgentRulesHistory (
        VersionId   INT IDENTITY(1,1) PRIMARY KEY,
        CreatedAt   DATETIME2     NOT NULL DEFAULT GETDATE(),
        CreatedBy   NVARCHAR(200) NOT NULL DEFAULT SYSTEM_USER,
        Description NVARCHAR(500) NULL,
        RulesJson   NVARCHAR(MAX) NOT NULL,
        IsActive    BIT           NOT NULL DEFAULT 0
    )
"""

_SEED_SQL = """
IF NOT EXISTS (SELECT 1 FROM config.tbl_AgentRulesHistory)
    INSERT INTO config.tbl_AgentRulesHistory (Description, RulesJson, IsActive)
    VALUES (
        'Default rules — baseline matching PlanningTools stored procedure',
        ?,
        1
    )
"""

_GET_ACTIVE_SQL = """
SELECT TOP 1 VersionId, CreatedAt, CreatedBy, Description, RulesJson
FROM config.tbl_AgentRulesHistory
WHERE IsActive = 1
ORDER BY VersionId DESC
"""

_GET_VERSION_SQL = """
SELECT VersionId, CreatedAt, CreatedBy, Description, RulesJson, IsActive
FROM config.tbl_AgentRulesHistory
WHERE VersionId = ?
"""

_LIST_HISTORY_SQL = """
SELECT TOP (?) VersionId, CreatedAt, CreatedBy, Description, IsActive
FROM config.tbl_AgentRulesHistory
ORDER BY VersionId DESC
"""

_rules_table_ready = False


def _ensure_rules_table() -> Optional[str]:
    global _rules_table_ready
    if _rules_table_ready:
        return None
    try:
        _query_sqlserver(_CREATE_TABLE_SQL)
        _query_sqlserver(_SEED_SQL, [json.dumps(DEFAULT_RULES_CONFIG)])
        _rules_table_ready = True
        return None
    except Exception as e:
        return str(e)


def _get_active_row() -> Optional[Dict[str, Any]]:
    rows, _ = _query_safe(_GET_ACTIVE_SQL)
    return rows[0] if rows else None


def _load_active_rules() -> Dict[str, Any]:
    """Return active rules config dict, falling back to DEFAULT if DB unavailable."""
    err = _ensure_rules_table()
    if err:
        return {**DEFAULT_RULES_CONFIG, "_source": "default_fallback", "_db_error": err}
    row = _get_active_row()
    if not row:
        return {**DEFAULT_RULES_CONFIG, "_source": "default_no_active_version"}
    config = json.loads(row["RulesJson"])
    config["_version_id"] = row["VersionId"]
    config["_created_at"] = str(row["CreatedAt"])
    config["_description"] = row["Description"]
    return config


def _save_version_atomically(description: str, rules_json: str) -> int:
    """Deactivate all versions + insert new active version in one transaction."""
    pyodbc = _get_pyodbc()
    conn_str = _build_connection_string()
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE config.tbl_AgentRulesHistory SET IsActive = 0")
        cur.execute(
            "INSERT INTO config.tbl_AgentRulesHistory (Description, RulesJson, IsActive) "
            "OUTPUT INSERTED.VersionId VALUES (?, ?, 1)",
            [description, rules_json],
        )
        row = cur.fetchone()
        return int(row[0]) if row else -1


def _activate_version_atomically(version_id: int) -> None:
    pyodbc = _get_pyodbc()
    conn_str = _build_connection_string()
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE config.tbl_AgentRulesHistory SET IsActive = 0")
        cur.execute(
            "UPDATE config.tbl_AgentRulesHistory SET IsActive = 1 WHERE VersionId = ?",
            [version_id],
        )


# ---------------------------------------------------------------------------
# AGENT TOOL: get_current_rules
# ---------------------------------------------------------------------------

def get_current_rules() -> Dict[str, Any]:
    """
    Return the currently active rules configuration with version metadata.

    Call this before update_rules to see what parameters are currently set.
    """
    err = _ensure_rules_table()
    if err:
        return {
            "status": "error",
            "message": f"Cannot access rules table: {err}",
            "fallback_defaults": DEFAULT_RULES_CONFIG,
        }
    row = _get_active_row()
    if not row:
        return {
            "status": "ok",
            "source": "default",
            "version_id": None,
            "rules": DEFAULT_RULES_CONFIG,
            "schema": RULES_SCHEMA_DESCRIPTION,
        }
    config = json.loads(row["RulesJson"])
    return {
        "status": "ok",
        "source": "database",
        "version_id": row["VersionId"],
        "created_at": str(row["CreatedAt"]),
        "created_by": row["CreatedBy"],
        "description": row["Description"],
        "rules": config,
        "schema": RULES_SCHEMA_DESCRIPTION,
    }


# ---------------------------------------------------------------------------
# AGENT TOOL: update_rules
# ---------------------------------------------------------------------------

def update_rules(changes_json: str, description: Optional[str] = None) -> Dict[str, Any]:
    """
    Merge rule parameter changes into the active config and save as a new version.

    changes_json: JSON object with only the parameters to change.
                  Unknown keys are rejected. Unmentioned keys carry over unchanged.
    description:  Optional human-readable summary of what changed and why.

    Returns the new version_id and the full merged config.

    Examples:
      changes_json='{"cover_weeks": 6}'
      changes_json='{"ignore_moq": true, "demand_scale_factor": 1.2}'
      changes_json='{"simulation_weeks": 26, "block_cny": false}'
      changes_json='{"cover_weeks": null}'   — resets cover_weeks back to DB-driven
    """
    err = _ensure_rules_table()
    if err:
        return {"status": "error", "message": f"Cannot access rules table: {err}"}

    try:
        changes = json.loads(changes_json)
    except Exception as e:
        return {"status": "error", "message": f"Invalid JSON in changes_json: {e}"}

    allowed = set(DEFAULT_RULES_CONFIG.keys())
    unknown = set(changes.keys()) - allowed
    if unknown:
        return {
            "status": "error",
            "message": f"Unknown rule keys: {sorted(unknown)}. Allowed: {sorted(allowed)}",
            "schema": RULES_SCHEMA_DESCRIPTION,
        }

    row = _get_active_row()
    current = json.loads(row["RulesJson"]) if row else dict(DEFAULT_RULES_CONFIG)
    merged = {**current, **{k: v for k, v in changes.items() if k in allowed}}

    try:
        new_version_id = _save_version_atomically(
            description or f"Updated: {', '.join(sorted(changes.keys()))}",
            json.dumps(merged),
        )
    except Exception as e:
        return {"status": "error", "message": f"Failed to save rules: {e}"}

    return {
        "status": "ok",
        "new_version_id": new_version_id,
        "changes_applied": changes,
        "new_rules": merged,
    }


# ---------------------------------------------------------------------------
# AGENT TOOL: rollback_rules
# ---------------------------------------------------------------------------

def rollback_rules(version_id: int) -> Dict[str, Any]:
    """
    Activate a specific historical rules version, making it the current active config.

    Use list_rules_history() to find available version_ids.
    To reset to factory defaults, use version_id=1.
    """
    err = _ensure_rules_table()
    if err:
        return {"status": "error", "message": f"Cannot access rules table: {err}"}

    rows, e = _query_safe(_GET_VERSION_SQL, [int(version_id)])
    if e or not rows:
        return {"status": "error", "message": f"Version {version_id} not found."}

    try:
        _activate_version_atomically(int(version_id))
    except Exception as ex:
        return {"status": "error", "message": f"Failed to activate version: {ex}"}

    row = rows[0]
    return {
        "status": "ok",
        "activated_version_id": int(version_id),
        "description": row["Description"],
        "originally_created_at": str(row["CreatedAt"]),
        "rules": json.loads(row["RulesJson"]),
    }


# ---------------------------------------------------------------------------
# AGENT TOOL: list_rules_history
# ---------------------------------------------------------------------------

def list_rules_history(limit: int = 20) -> Dict[str, Any]:
    """
    List the most recent rules versions with metadata.

    Shows version_id, created_at, description, and is_active for each version.
    Use rollback_rules(version_id) to activate any previous version.
    version_id=1 is always the factory default.
    """
    err = _ensure_rules_table()
    if err:
        return {"status": "error", "message": f"Cannot access rules table: {err}"}

    rows, e = _query_safe(_LIST_HISTORY_SQL, [min(int(limit), 100)])
    if e:
        return {"status": "error", "message": e}

    return {"status": "ok", "count": len(rows), "versions": rows}


# ---------------------------------------------------------------------------
# Simulation engine (deterministic — no LLM)
# ---------------------------------------------------------------------------

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def _run_simulation(data: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic week-by-week simulation. Implements the same algorithm as AGENT_INSTRUCTIONS.
    Called by execute_reordering_with_rules — not exposed as an agent tool directly.
    """
    tables = data.get("tables", {})
    item_number = data.get("item_number", "")
    item_key = data.get("item_key")
    warehouse_code = data.get("warehouse_code")

    item_master = (tables.get("bicache_tbl_Item") or [{}])[0]
    iw = (tables.get("fpo_tbl_ItemWarehouse") or [{}])[0]
    oq = (tables.get("fpo_tbl_ItemWarehouseOrderQty") or [{}])[0]
    cws = (tables.get("fpo_tbl_CalcWarehouseStock") or [{}])[0]
    lt = (tables.get("fpo_tbl_ItemWarehouseLeadtime") or [{}])[0]

    cover_cfg_rows = tables.get("fpo_tbl_ImportCoverConfig") or []
    wh_cover_rows = tables.get("fpo_tbl_ConfigWarehouseCover") or []
    timeline_rows = tables.get("fpo_tbl_CalcTimelineWeek") or []
    wh_key = ((tables.get("bicache_tbl_CentralWarehouse") or [{}])[0]).get("CentralWarehouseKey")

    sim_weeks = int(rules.get("simulation_weeks", 53))

    # Safety stock
    db_ss = _safe_float(iw.get("SafetyStockQty"), 0.0)
    ss_mult = float(rules.get("safety_stock_multiplier", 1.0))
    effective_ss = db_ss * ss_mult

    # ReqPO
    db_req_po = _safe_int(iw.get("ReqPO"), 0)
    rules_req_po = rules.get("req_po")
    effective_req_po = (1 if rules_req_po else 0) if rules_req_po is not None else db_req_po

    # ABC class + filter
    category_abc = (iw.get("CategoryABC") or "C").strip().upper()
    abc_to_process = set(rules.get("abc_classes_to_process") or ["A", "B", "C"])
    if category_abc not in abc_to_process:
        return {
            "status": "skipped",
            "reason": f"CategoryABC={category_abc} not in abc_classes_to_process={sorted(abc_to_process)}",
            "item_number": item_number,
            "warehouse_code": warehouse_code,
        }

    # Cover weeks
    rules_cover = rules.get("cover_weeks")
    if rules_cover is not None:
        effective_cover_weeks = int(rules_cover)
    else:
        db_cover_weeks = 4
        matched = False
        for row in cover_cfg_rows:
            if (row.get("CategoryABC") or "").strip().upper() == category_abc:
                db_cover_weeks = _safe_int(row.get("NoOfWeeksCoverWarehouseOrder"), 4) or 4
                matched = True
                break
        if not matched:
            if cover_cfg_rows:
                db_cover_weeks = _safe_int(cover_cfg_rows[0].get("NoOfWeeksCoverWarehouseOrder"), 4) or 4
            elif wh_cover_rows:
                db_cover_weeks = (
                    _safe_int(wh_cover_rows[0].get("NoOfWeeksCoverWarehouseOrder"), 0)
                    or _safe_int(wh_cover_rows[0].get("WeeksOfCover"), 4)
                    or 4
                )
        effective_cover_weeks = db_cover_weeks

    # Order qty type
    db_oqt = (oq.get("OrderQtyType") or "C").strip().upper()
    rules_oqt = rules.get("order_qty_type")
    effective_oqt = rules_oqt.upper() if rules_oqt else db_oqt

    aoq = _safe_float(oq.get("AOQ"), 0.0)
    eoq = _safe_float(oq.get("EOQ"), 0.0)
    loq = _safe_float(oq.get("LOQ"), 0.0)
    soq = _safe_float(oq.get("SOQ"), 0.0)

    # Item dimensions
    store_cs = _safe_float(item_master.get("StoreCartonSize"), 1.0) or 1.0
    supplier_cs = _safe_float(item_master.get("SupplierCartonSize"), 0.0)
    pallet_sz = _safe_float(item_master.get("PalletSize"), 0.0)
    db_moq = _safe_float(item_master.get("MOQ"), 0.0)
    effective_moq = 0.0 if rules.get("ignore_moq") else db_moq

    # Rounding type
    rounding_override = rules.get("rounding_override")
    if rounding_override:
        rounding_type = rounding_override.upper()
    else:
        country_rows = tables.get("config_tbl_Country") or []
        if pallet_sz > 0 and country_rows:
            rounding_type = "P"
        elif supplier_cs > 0:
            rounding_type = "S"
        elif store_cs > 0:
            rounding_type = "C"
        else:
            rounding_type = "U"

    pack_size = {"P": pallet_sz, "S": supplier_cs, "C": store_cs, "U": 1.0}.get(rounding_type, 1.0) or 1.0

    # Warehouse stock state
    opening_stock = _safe_float(cws.get("CloseStockWk00"), 0.0)
    latest_delivery_week = _safe_int(cws.get("LatestDeliveryWeek"), 0)
    db_block_until = _safe_int(cws.get("BlockRecUntilWeekNo"), 0)

    existing_inbound: Dict[int, float] = {
        n: _safe_float(cws.get(f"StockInWk{n:02d}"), 0.0) for n in range(1, sim_weeks + 1)
    }

    # Weekly series with demand scaling
    scale = float(rules.get("demand_scale_factor", 1.0))
    demand_by_week = {d["week_index"]: d["demand"] * scale for d in (data.get("demand_by_week") or [])}
    stockin_by_week = {d["week_index"]: d["stockin"] * scale for d in (data.get("store_stockin_by_week") or [])}
    forecast_by_week = {d["week_index"]: d["forecast"] for d in (data.get("forecast_by_week") or [])}
    ststock_by_week = {d["week_index"]: d["ststock"] for d in (data.get("ststock_by_week") or [])}

    # Zero-demand guard
    total_forecast = sum(forecast_by_week.get(n, 0.0) for n in range(1, sim_weeks + 1))
    total_demand = sum(demand_by_week.get(n, 0.0) for n in range(1, sim_weeks + 1))
    if total_forecast == 0 and total_demand == 0:
        return {
            "status": "no_demand",
            "message": "No forecast or demand data. No orders placed.",
            "item_number": item_number,
            "warehouse_code": warehouse_code,
        }

    timeline_map: Dict[int, Dict] = {i: row for i, row in enumerate(timeline_rows, 1)}
    block_cny = bool(rules.get("block_cny", True))

    # ── Week-by-week simulation ───────────────────────────────────────────

    wh_stock = opening_stock
    local_block_until = 0
    recommendations: List[Dict[str, Any]] = []
    skipped_weeks: List[Dict[str, Any]] = []
    weekly_projection: List[Dict[str, Any]] = []

    for t in range(1, sim_weeks + 1):
        existing_inb = existing_inbound.get(t, 0.0)
        sent_to_stores = stockin_by_week.get(t, 0.0)
        demand_t = demand_by_week.get(t, 0.0)
        forecast_t = forecast_by_week.get(t, 0.0)
        ststock_t = ststock_by_week.get(t, 0.0)

        delivery_date = lt.get(f"DeliveryDateWk{t:02d}")
        block_reason_raw = lt.get(f"BlockReasonWk{t:02d}")
        req_post_date = lt.get(f"ReqPostDateWk{t:02d}")
        year_week = str((timeline_map.get(t) or {}).get("YearAndWeek", ""))

        new_order = 0.0
        skip_reason: Optional[str] = None

        # CHECK A — Delivery gate
        if delivery_date is None:
            skip_reason = "no_delivery_date"
        elif block_reason_raw is not None:
            if str(block_reason_raw) == "CNY" and not block_cny:
                pass  # CNY blocks ignored per rules config
            else:
                skip_reason = str(block_reason_raw)

        # CHECK B — Pending inbound gate
        if skip_reason is None and latest_delivery_week and latest_delivery_week > t:
            skip_reason = f"pending_inbound_wk{latest_delivery_week}"

        # CHECK C — Block gate
        if skip_reason is None:
            if db_block_until >= t:
                skip_reason = f"blocked_until_wk{db_block_until}"
            elif local_block_until > 0 and t <= local_block_until:
                skip_reason = f"blocked_until_wk{local_block_until}"

        # CHECK D — Config gate
        if skip_reason is None:
            if effective_req_po != 1:
                skip_reason = "req_po_off"
            elif t + effective_cover_weeks > sim_weeks:
                skip_reason = "cover_exceeds_horizon"

        # CHECK E — Stock trigger
        if skip_reason is None:
            projected = wh_stock + existing_inb - sent_to_stores
            if projected > effective_ss:
                skip_reason = "stock_above_ss"

        # CHECK F — Demand / forecast present
        if skip_reason is None:
            if demand_t == 0 and sent_to_stores == 0 and forecast_t == 0:
                skip_reason = "no_demand_or_forecast"

        if skip_reason is None:
            projected = wh_stock + existing_inb - sent_to_stores
            safety_gap = max(0.0, effective_ss - projected)

            if effective_oqt == "A":
                raw_qty = aoq
            elif effective_oqt == "E":
                raw_qty = eoq
            elif effective_oqt == "L":
                raw_qty = loq
            elif effective_oqt == "S":
                raw_qty = soq
            else:  # "C" — cover-based
                rec_demand = sum(demand_by_week.get(w, 0.0) for w in range(t, t + effective_cover_weeks))
                raw_qty = rec_demand + safety_gap + store_cs

            if rounding_type in ("P", "S", "C") and pack_size > 0:
                rounded_qty = math.ceil(raw_qty / pack_size) * pack_size
            else:
                rounded_qty = math.ceil(raw_qty)

            if rounded_qty <= 0:
                skip_reason = "rounded_qty_zero"
            else:
                moq_status = (
                    "meets_item_moq_alone"
                    if (rules.get("ignore_moq") or rounded_qty >= effective_moq)
                    else "requires_group_validation"
                )
                new_order = float(rounded_qty)
                local_block_until = t + effective_cover_weeks - 1

                recommendations.append({
                    "item_key": item_key,
                    "central_warehouse_key": wh_key,
                    "warehouse_code": warehouse_code,
                    "rec_order_week": t,
                    "year_week": year_week,
                    "delivery_date": str(delivery_date) if delivery_date else None,
                    "req_post_date": str(req_post_date) if req_post_date else None,
                    "order_qty": int(rounded_qty),
                    "weeks_cover": effective_cover_weeks,
                    "rounding_type": rounding_type,
                    "moq_status": moq_status,
                    "raw_qty_before_rounding": raw_qty,
                    "safety_gap": safety_gap,
                    "projected_stock_before_order": projected,
                    "reasoning": (
                        f"wk{t}: projected={projected:.0f}, ss={effective_ss:.0f}, "
                        f"gap={safety_gap:.0f}, raw={raw_qty:.0f}, rounded={rounded_qty:.0f} "
                        f"({rounding_type} pack={pack_size:.0f}), moq={effective_moq:.0f}"
                    ),
                })

        if skip_reason:
            skipped_weeks.append({"week_index": t, "year_week": year_week, "skip_reason": skip_reason})

        wh_stock = wh_stock + existing_inb + new_order - sent_to_stores

        weekly_projection.append({
            "week_index": t,
            "year_week": year_week,
            "forecast": forecast_t,
            "store_movement": sent_to_stores,
            "existing_inbound": existing_inb,
            "new_order": new_order,
            "whstock": wh_stock,
            "ststock": ststock_t,
            "block_reason": str(block_reason_raw) if block_reason_raw else None,
        })

    return {
        "status": "ok",
        "mode": "simulate",
        "scope": {
            "item_number": item_number,
            "item_key": item_key,
            "warehouse_code": warehouse_code,
            "weeks_simulated": sim_weeks,
        },
        "parameters": {
            "weeks_of_cover": effective_cover_weeks,
            "safety_stock_qty": effective_ss,
            "safety_stock_multiplier": ss_mult,
            "order_qty_type": effective_oqt,
            "req_po": effective_req_po,
            "moq": effective_moq,
            "store_carton_size": store_cs,
            "supplier_carton_size": supplier_cs,
            "pallet_size": pallet_sz,
            "rounding_type": rounding_type,
            "latest_delivery_week": latest_delivery_week or None,
            "block_rec_until_week": db_block_until or None,
            "category_abc": category_abc,
            "opening_wh_stock": opening_stock,
        },
        "recommendations": recommendations,
        "skipped_weeks": skipped_weeks,
        "warehouse_views": [
            {
                "warehouse_code": warehouse_code,
                "weekly_projection": weekly_projection,
            }
        ],
    }


# ---------------------------------------------------------------------------
# AGENT TOOL: execute_reordering_with_rules
# ---------------------------------------------------------------------------

def execute_reordering_with_rules(
    item_number: str,
    central_warehouse_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a deterministic reorder simulation using the currently active rules config.

    All simulation logic runs in Python — no LLM inference on the simulation itself.
    The same active rules config always produces the same result for the same DB data.

    To change simulation parameters: call update_rules() first.
    To see active parameters:         call get_current_rules().
    To activate a previous version:   call rollback_rules(version_id).

    Returns the same JSON structure as the standard simulate mode.
    """
    rules = _load_active_rules()
    version_id = rules.get("_version_id")
    clean_rules = {k: v for k, v in rules.items() if not k.startswith("_")}

    sim_weeks = int(clean_rules.get("simulation_weeks", 53))
    data = get_item_ordering_data(
        item_number=item_number,
        central_warehouse_code=central_warehouse_code,
        n_weeks=sim_weeks,
    )

    if data.get("status") != "ok":
        return data

    result = _run_simulation(data, clean_rules)
    result["rules_version"] = version_id
    result["rules_source"] = rules.get("_source", "database")
    return result
