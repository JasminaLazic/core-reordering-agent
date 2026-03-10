import asyncio
from types import SimpleNamespace

from azure.ai.projects.aio import AIProjectClient
from azure.core.exceptions import ResourceExistsError
from agent_framework.azure import AzureAIAgentClient
from auth import get_azure_credential
try:
    # Older agent-framework builds expose Agent at top-level.
    from agent_framework import Agent
except ImportError:
    # Newer builds expose ChatAgent instead.
    from agent_framework import ChatAgent as Agent

from config import (
    AI_FOUNDRY_PROJECT_ENDPOINT,
    MODEL_DEPLOYMENT_NAME,
    CORE_ORDERING_AGENT_ID,
)
from agents.tools.core_ordering_tools import (
    get_core_ordering_schema_reference,
    get_stock_warehouse_on_hand,
    get_stock_store_on_hand,
    get_store_warehouse_relationship,
    get_core_assortment,
    get_forecast_store_sales,
    get_calc_timeline_day,
    get_calc_timeline_week,
    get_calc_store_stock,
    get_calc_warehouse_stock,
    get_item_warehouse,
    get_item_warehouse_order_qty,
    get_item_warehouse_leadtime,
    get_config_store_cover,
    get_config_warehouse_cover,
    get_import_cover_config,
    get_job_control,
    get_job_control_history,
    run_planning_tools_readonly_query,
)


def _extract_obj_value(obj: object, key: str) -> object:
    if isinstance(obj, dict):
        return obj.get(key)
    getter = getattr(obj, "get", None)
    if callable(getter):
        try:
            return getter(key)
        except Exception:
            pass
    return getattr(obj, key, None)


async def _resolve_agent_id_if_needed(raw_agent_ref: str, credential: object) -> str:
    """
    Accept either an `asst_...` id or an agent name.
    If a name is provided, resolve it to id from Foundry project.
    """
    ref = (raw_agent_ref or "").strip()
    if ref.startswith("asst_"):
        return ref

    async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
        candidates = []
        try:
            if hasattr(project_client.agents, "list"):
                pager = project_client.agents.list(limit=200)
                async for a in pager:
                    candidates.append(a)
            elif hasattr(project_client.agents, "list_agents"):
                pager = project_client.agents.list_agents(limit=200)
                async for a in pager:
                    candidates.append(a)
        except Exception:
            candidates = []

        # Try exact name match first.
        for a in candidates:
            name = _extract_obj_value(a, "name")
            aid = _extract_obj_value(a, "id")
            if str(name or "").strip() == ref and str(aid or "").startswith("asst_"):
                return str(aid)

        # Fallback to direct getter by provided ref.
        fetched = None
        try:
            if hasattr(project_client.agents, "get_agent"):
                fetched = await project_client.agents.get_agent(ref)
            else:
                fetched = await project_client.agents.get(ref)
        except Exception:
            fetched = None

        resolved = _extract_obj_value(fetched, "id") if fetched is not None else None
        if resolved and str(resolved).startswith("asst_"):
            return str(resolved)

        raise RuntimeError(
            "CORE_ORDERING_AGENT_ID must be an 'asst_' id (or a resolvable agent name). "
            f"Could not resolve '{ref}' to a valid assistant id."
        )


def _make_agent_definition(model: str, instructions: str) -> object:
    """
    Build an agent definition compatible with multiple azure-ai-projects SDK versions.
    """
    try:
        from azure.ai.projects.models import PromptAgentDefinition  # type: ignore
        return PromptAgentDefinition(model=model, instructions=instructions)
    except Exception:
        # Older/newer SDK variants may accept plain JSON body shape.
        return {
            "model": model,
            "instructions": instructions,
        }


def _patch_ai_projects_agents_compat() -> None:
    """
    Bridge API differences between azure-ai-projects SDK variants.
    Some agent_framework_azure_ai builds expect agents.{get_agent,create_agent,delete_agent}
    while newer azure-ai-projects exposes agents.{get,create,delete}.
    """
    try:
        from azure.ai.projects.aio.operations._operations import AgentsOperations
    except Exception:
        return

    if not hasattr(AgentsOperations, "get_agent") and hasattr(AgentsOperations, "get"):
        async def _get_agent(self, agent_name: str, **kwargs: object) -> object:
            return await self.get(agent_name, **kwargs)
        setattr(AgentsOperations, "get_agent", _get_agent)

    if not hasattr(AgentsOperations, "delete_agent") and hasattr(AgentsOperations, "delete"):
        async def _delete_agent(self, agent_name: str, **kwargs: object) -> object:
            return await self.delete(agent_name, **kwargs)
        setattr(AgentsOperations, "delete_agent", _delete_agent)

    if not hasattr(AgentsOperations, "create_agent") and hasattr(AgentsOperations, "create"):
        async def _create_agent(self, **kwargs: object) -> object:
            name = str(kwargs.pop("name", "CoreOrderingAgent"))
            definition = kwargs.pop("definition", None)
            if definition is None:
                model = kwargs.pop("model", None)
                instructions = kwargs.pop("instructions", None)
                if model is None:
                    raise TypeError("create_agent compatibility wrapper requires model or definition.")
                definition = _make_agent_definition(str(model), str(instructions or ""))
            return await self.create(name=name, definition=definition, **kwargs)
        setattr(AgentsOperations, "create_agent", _create_agent)


class CompatAzureAIAgentClient(AzureAIAgentClient):
    """
    Compatibility wrapper for mixed azure-ai-projects/agent-framework versions.
    Some SDK combinations return AgentObject shapes without `tools` / `instructions`
    attributes that agent_framework_azure_ai expects.
    """

    async def _load_agent_definition_if_needed(self):  # type: ignore[override]
        try:
            definition = await super()._load_agent_definition_if_needed()
        except Exception:
            return None
        if definition is None:
            return None

        if hasattr(definition, "tools") and hasattr(definition, "instructions"):
            return definition

        # Coerce dict-like AgentObject to an object with expected attrs.
        get_value = getattr(definition, "get", None)
        if callable(get_value):
            tools = get_value("tools", []) or []
            instructions = get_value("instructions")
            tool_resources = get_value("tool_resources")
            coerced = SimpleNamespace(
                tools=tools,
                instructions=instructions,
                tool_resources=tool_resources,
            )
            self._agent_definition = coerced
            return coerced

        # If shape is unknown, skip loading definition instead of failing requests.
        return None


async def get_core_ordering_agent() -> Agent:
    if not CORE_ORDERING_AGENT_ID:
        raise RuntimeError("Missing CORE_ORDERING_AGENT_ID in .env (create the agent once first).")

    _patch_ai_projects_agents_compat()
    async with get_azure_credential() as credential:
        resolved_agent_id = await _resolve_agent_id_if_needed(CORE_ORDERING_AGENT_ID, credential)
        chat_client = CompatAzureAIAgentClient(
            project_endpoint=AI_FOUNDRY_PROJECT_ENDPOINT,
            async_credential=credential,
            agent_id=resolved_agent_id,
        )
        tools = [
            get_core_ordering_schema_reference,
            get_stock_warehouse_on_hand,
            get_stock_store_on_hand,
            get_store_warehouse_relationship,
            get_core_assortment,
            get_forecast_store_sales,
            get_calc_timeline_day,
            get_calc_timeline_week,
            get_calc_store_stock,
            get_calc_warehouse_stock,
            get_item_warehouse,
            get_item_warehouse_order_qty,
            get_item_warehouse_leadtime,
            get_config_store_cover,
            get_config_warehouse_cover,
            get_import_cover_config,
            get_job_control,
            get_job_control_history,
            run_planning_tools_readonly_query,
        ]
        # Backward/forward compatibility across agent_framework versions:
        # older versions expect client=..., newer ChatAgent expects chat_client=...
        try:
            return Agent(
                client=chat_client,
                tools=tools,
                store=True,
            )
        except TypeError:
            return Agent(
                chat_client=chat_client,
                tools=tools,
                store=True,
            )


async def create_core_ordering_agent() -> str:
    _patch_ai_projects_agents_compat()
    async with get_azure_credential() as credential:
        async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
            instructions = """You are the FPO Core Reordering Engine (Exact SQL Parity Mode).

MISSION
Replicate legacy core reordering behavior as closely as possible using only available read-only base-table data and deterministic reasoning.
Your role is execution-grade parity logic, not advisory storytelling.

HARD GUARANTEES
1) Deterministic execution
   - Same scoped input data MUST produce the same output JSON.
   - No hidden randomness, no heuristic drift.

2) Data-grounded only
   - Use only tool-returned values from approved base tables.
   - Never fabricate weeks, quantities, lead times, stock, demand, or statuses.

3) No stored procedure claims
   - Do not claim stored procedures were executed.
   - Do not assume write-side effects happened in DB.

4) No result-table dependency
   - Do not read output/result tables as source of truth for recommendations.
   - Build results from base state tables only.

5) Fail closed when mandatory inputs are missing
   - If required fields are missing/inconsistent after tool retrieval, return blocking_error.

APPROVED BASE DATA SOURCES (via tools)
- fpo.tbl_CalcTimelineWeek
- fpo.tbl_CalcWarehouseStock
- fpo.tbl_CalcStoreStock
- fpo.tbl_ForecastStoreSales
- fpo.tbl_ItemWarehouse
- fpo.tbl_ItemWarehouseOrderQty
- fpo.tbl_ItemWarehouseLeadtime
- fpo.tbl_ConfigStoreCover
- fpo.tbl_ConfigWarehouseCover
- fpo.tbl_ImportCoverConfig
- model.tbl_StockWarehouseOnHand
- model.tbl_StockStoreOnHand
- model.tbl_StoreWarehouseRelationship
- model.tbl_CoreAssortment
- bicache item/warehouse dimensions only when needed for key resolution

DISALLOWED AS PRIMARY SOURCE
- Any stored procedure output assumptions
- Any result/order fact table as logic source for new recommendation decisions
- Any fabricated inferred field not derivable from retrieved rows

REQUEST RESOLUTION RULES
1) Resolve item
   - If item_number and item_key both provided and conflict, return:
     status=blocking_error, code=INCONSISTENT_INPUT.
   - If only item_number is provided, resolve item_key from dimension/tool data.
2) Resolve warehouse
   - If warehouse code is provided, use it.
   - If user says Danish warehouse, map to DK01WH.
   - If no explicit warehouse is provided and user asks "per warehouse"/"all warehouses"/tabular comparison, process all resolved warehouses and emit one section per warehouse.
   - If no explicit warehouse is provided and intent is singular (not per-warehouse), return blocking_error (AMBIGUOUS_INPUT).
3) Resolve horizon
   - week_start must be in [1..53].
   - horizon_weeks must be >=1 and clipped to remaining horizon if needed.

ENGINE EXECUTION SEQUENCE (STRICT)
INIT -> W1 -> S2 -> W3 -> R4 -> S5 -> C6 -> R7
Run week-by-week across the requested horizon.

STEP CONTRACTS

INIT
- Validate resolved scope and required week columns.
- Initialize run context and iteration counter.
- Prepare per-week working state arrays.
- If mandatory fields absent for scoped item/warehouse/store set, fail closed.

W1 (warehouse inbound + delivery context)
- Build current-week inbound/arrival context from base state fields.
- Set latest delivery/arrival context where available from leadtime/date fields.
- Do not pull result-order outcomes as ground truth.

S2 (store-side demand and satisfaction)
- Compute/store week demand needs from forecast, cover, and local stock trajectory.
- Allocate from warehouse-available stock with deterministic ordering (store key ascending).
- Track unmet demand explicitly.

W3 (warehouse stock trajectory + trigger pressure)
- Update warehouse close stock using prior close + inbound - satisfied demand.
- Track safety pressure / below-threshold week markers.
- Build reorder trigger signals from stock pressure + demand outlook + valid timing window.

R4 (quantity shaping and constraints)
- Compute raw recommendation demand at trigger point.
- Apply quantity type and rounding priority deterministically.
- Enforce MOQ and order multiple constraints.
- Record adjusted quantity and constraint impact.

S5 (store trajectory while recommendation active)
- Recompute dependent store close trajectories when recommendation state is active.

C6 (cover/consistency checks)
- Compute coverage consistency metrics and timing reasonableness checks.

R7 (finalize per-week output)
- Emit per-week projection record for requested horizon.
- Emit action arrays for deterministic state transitions.
- Set next_week_pointer if continuing; null when finished.

NUMERIC RULES
- Use decimal-safe arithmetic.
- Never round down when constraint rounding applies.
- Preserve precision for stock/forecast/demand, but output normalized numeric values.
- Keep rounding policy consistent across weeks.

MANDATORY VALIDATIONS
- week_start valid
- resolved item and warehouse valid
- required week columns available for each processed step
- no unresolved item_number/item_key mismatch
- no ambiguous warehouse scope
If any fail: blocking_error + empty actions + no fabricated projection.

DEFAULT RESPONSE FORMAT (JSON ONLY)
{
  "run_id": "string",
  "status": "ok | blocking_error",
  "scope": {
    "item_number": "string|null",
    "item_key": "number|null",
    "warehouse_code": "string|null",
    "week_start": "number|null",
    "horizon_weeks": "number|null"
  },
  "blocking_error": {
    "code": "MISSING_REQUIRED_INPUT | INCONSISTENT_INPUT | AMBIGUOUS_INPUT",
    "message": "string",
    "details": ["string"]
  },
  "weekly_projection": [
    {
      "week_index": 0,
      "year_week": "string|null",
      "forecast": 0,
      "demand": 0,
      "quantity": 0,
      "order": 0,
      "whstock": 0,
      "ststock": 0,
      "downtime": "string|null",
      "reason_codes": ["INIT","W1","S2","W3","R4","S5","C6","R7"]
    }
  ],
  "warehouse_views": [
    {
      "warehouse_code": "string",
      "weekly_projection": [
        {
          "week_index": 0,
          "year_week": "string|null",
          "forecast": 0,
          "demand": 0,
          "quantity": 0,
          "order": 0,
          "whstock": 0,
          "ststock": 0,
          "downtime": "string|null",
          "reason_codes": ["INIT","W1","S2","W3","R4","S5","C6","R7"]
        }
      ],
      "totals": {
        "forecast": 0,
        "demand": 0,
        "quantity": 0,
        "order": 0
      }
    }
  ],
  "actions": {
    "update_calc_store_stock": [],
    "update_calc_warehouse_stock": [],
    "insert_recommended_orders": [],
    "reset_rec_fields": [],
    "set_block_until_week": [],
    "next_week_pointer": null
  },
  "trace": {
    "constraint_summary": {
      "order_qty_type": "string|null",
      "moq": 0,
      "order_multiple": 0
    },
    "data_sources_used": ["string"],
    "assumptions": ["string"]
  }
}

INTERACTION POLICY
- Return JSON only.
- No markdown.
- No explanatory prose outside JSON.
- If user asks natural language, still return structured JSON with same schema.
- For per-warehouse asks, always populate warehouse_views and keep weekly_projection as the primary warehouse only for backward compatibility.
"""
            definition = _make_agent_definition(
                model=MODEL_DEPLOYMENT_NAME,
                instructions=instructions,
            )

            try:
                if hasattr(project_client.agents, "create_agent"):
                    created = await project_client.agents.create_agent(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
                elif hasattr(project_client.agents, "create"):
                    created = await project_client.agents.create(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
                else:
                    # Compatibility fallback for SDK versions exposing a private create method.
                    created = await project_client.agents._create_agent(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
            except ResourceExistsError:
                # Idempotent behavior: if the named agent exists, return its id.
                if hasattr(project_client.agents, "get_agent"):
                    created = await project_client.agents.get_agent("CoreOrderingAgent")
                else:
                    created = await project_client.agents.get("CoreOrderingAgent")
            return created.id


if __name__ == "__main__":
    agent_id = asyncio.run(create_core_ordering_agent())
    print("Created agent id:", agent_id)
    print("Add to .env: CORE_ORDERING_AGENT_ID=" + agent_id)
