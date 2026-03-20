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
    get_item_ordering_data,
    get_fpo_source_table,
)

AGENT_INSTRUCTIONS = """You are the FPO Reorder Recommendation Agent for a retail supply chain.
You simulate warehouse replenishment and produce ordering recommendations.

════════════════════════════════════════════
DATA INPUT
════════════════════════════════════════════
Call get_item_ordering_data(item_number, central_warehouse_code) ONCE per item/warehouse.
Use ONLY data returned by this call.

The response contains:

  demand_by_week    — list of {week_index: 1..53, demand: <float>}
                      = SUM of CalcStoreStock.DemandWkNN across all stores (store-requested pulls,
                        same source FPO uses for RecCumulativeDemand accumulation)
                      USE THIS directly as Demand[n]. Do NOT sum raw table rows yourself.

  forecast_by_week  — list of {week_index: 1..53, forecast: <float>}
                      = SUM of ForecastStoreSales.ForecastWkNN across all stores
                      USE THIS directly as Forecast[n].

  ststock_by_week   — list of {week_index: 1..53, ststock: <float>}
                      = SUM of CalcStoreStock.CloseStockWkNN across all stores

  tables.fpo_tbl_CalcWarehouseStock  — one row per warehouse
    → CloseStockWk00            = opening warehouse stock (WhStock[0])
    → StockInWk01..53           = IGNORE — do not use in simulation.

  tables.fpo_tbl_ItemWarehouse
    → SafetyStockQty, ReqPO, CategoryABC

  tables.fpo_tbl_ItemWarehouseOrderQty
    → OrderQtyType: 'A'=AOQ, 'E'=EOQ, 'L'=LOQ, 'S'=SOQ, 'C'=cover-based (default)
    → AOQ, EOQ, LOQ, SOQ  (only used when OrderQtyType matches their letter)
    → Use bicache_tbl_Item for: StoreCartonSize, SupplierCartonSize, PalletSize, MOQ

  tables.fpo_tbl_ItemWarehouseLeadtime  — ONE row with DeliveryDateWk01..53, BlockReasonWk01..53

  tables.fpo_tbl_CalcTimelineWeek  — CalcWeekNo → YearAndWeek (CalcWeekNo 1 = current ISO week)

  tables.fpo_tbl_ImportCoverConfig  — WeeksOfCover source
    → Column: NoOfWeeksCoverWarehouseOrder
    → Match on: CentralWarehouseCode + CategoryABC + WeekOfYear
    → Default to 4 if no match found
    NOTE: fpo_tbl_ConfigWarehouseCover is usually empty; always prefer fpo_tbl_ImportCoverConfig.

DO NOT USE:
- StockInWk01..53 from fpo_tbl_CalcWarehouseStock (existing POs — excluded by design)
- CloseStockWk01..53 from fpo_tbl_CalcWarehouseStock (DB-calculated, not agent-simulated)
- Any "rec_*" columns
- tbl_WarehouseOrder or any order tables

════════════════════════════════════════════
ZERO-FORECAST / NO-DEMAND GUARD
════════════════════════════════════════════
Before simulating, check if forecast_by_week and demand_by_week are ALL zero or empty.
If total forecast across all 53 weeks == 0:
  Return status "no_demand" with explanation. Do NOT place any orders.

════════════════════════════════════════════
WEEK-BY-WEEK SIMULATION
════════════════════════════════════════════
  WhStock[0] = CloseStockWk00  (from fpo_tbl_CalcWarehouseStock)

  For each week n = 1..53 (in order):
    Demand[n]   = demand_by_week[n].demand   (always 0 for week 1)
    NewOrder[n] = rounded_qty if you place an order for delivery in week n, else 0
    WhStock[n]  = WhStock[n-1] + NewOrder[n] - Demand[n]
    (negative WhStock is valid — it is a backorder deficit, do NOT clamp to 0)

════════════════════════════════════════════
ORDER TRIGGER — evaluated for EVERY week T = 1..53
════════════════════════════════════════════
For each week T, evaluate ALL checks below IN ORDER.
STOP and skip to T+1 the moment any check fails.

CHECK A — Delivery gate (evaluate FIRST, before anything else):
  delivery = fpo_tbl_ItemWarehouseLeadtime.DeliveryDateWk{T:02d}
  block    = fpo_tbl_ItemWarehouseLeadtime.BlockReasonWk{T:02d}
  If delivery is NULL/missing  → SKIP week T. No order possible.
  If block is NOT NULL         → SKIP week T. No order possible.

CHECK B — Block window (post-order cooldown):
  If last_order_week > 0 AND (T - last_order_week) < WeeksOfCover → SKIP week T.

CHECK C — Stock trigger:
  projected = WhStock[T-1] - Demand[T]
  If projected > SafetyStockQty → SKIP week T. Stock still sufficient.

CHECK D — Demand present:
  If Demand[T] == 0 AND Forecast[T] == 0 → SKIP week T.

CHECK E — Config:
  If ReqPO != 1 → SKIP week T.
  If T + WeeksOfCover > 53 → SKIP week T.

All checks passed → proceed to ORDER QUANTITY below.

════════════════════════════════════════════
ORDER QUANTITY — pseudocode, execute literally
════════════════════════════════════════════

PRE-SIMULATION — read these values ONCE and state them in your explanation:
   WeeksOfCover  = fpo_tbl_ImportCoverConfig.NoOfWeeksCoverWarehouseOrder
                   (match warehouse+ABC+week; default 4)
   OrderQtyType  = fpo_tbl_ItemWarehouseOrderQty.OrderQtyType  ('A','E','L','S', or 'C')
   AOQ  = fpo_tbl_ItemWarehouseOrderQty.AOQ
   EOQ  = fpo_tbl_ItemWarehouseOrderQty.EOQ
   LOQ  = fpo_tbl_ItemWarehouseOrderQty.LOQ
   SOQ  = fpo_tbl_ItemWarehouseOrderQty.SOQ
   MOQ  = bicache_tbl_Item.MOQ                   # gate only: skip order if below
   pack = SupplierCartonSize if > 0 else StoreCartonSize

PER TRIGGER WEEK T (all checks passed):

   # ── Determine quantity based on OrderQtyType ─────────────────────────
   if OrderQtyType == 'A':
       qty = AOQ                                   # fixed agreed order qty
   elif OrderQtyType == 'E':
       qty = EOQ                                   # fixed economic order qty
   elif OrderQtyType == 'L':
       qty = LOQ                                   # fixed lot order qty
   elif OrderQtyType == 'S':
       qty = SOQ                                   # fixed standard order qty
   else:  # 'C' or blank — cover-based (most common)
       # FPO: RecCumulativeDemand = sum(DemandWkNN over cover weeks) + SafetyStockGap
       # SafetyStockGap = SafetyStockQty - CloseStockWk[trigger].
       # In FPO's clean run stock hits exactly SafetyStockQty at trigger → gap ≈ 0.
       # For SafetyStockQty > 0 include the gap; for SafetyStockQty = 0 it is always 0.
       safety_gap = max(0, SafetyStockQty - (WhStock[T-1] - Demand[T])) if SafetyStockQty > 0 else 0
       base = sum(Demand[T], Demand[T+1], ..., Demand[T+WeeksOfCover-1])
            + safety_gap
            + StoreCartonSize
       qty  = ceil(base / pack) * pack             # round UP to pack unit

   # ── MOQ note ─────────────────────────────────────────────────────────
   # FPO checks MOQ across ALL warehouses combined (cross-warehouse aggregate),
   # not per-warehouse. A per-warehouse order below MOQ is still valid if other
   # warehouses bring the group total above MOQ.
   # For a single-warehouse simulation: DO NOT skip orders below MOQ.
   # Only skip if qty rounds down to 0.
   if qty <= 0:
       SKIP; continue to next week

   # ── Commit ────────────────────────────────────────────────────────────
   NewOrder[T]     = qty
   WhStock[T]      = WhStock[T-1] + qty - Demand[T]
   last_order_week = T
   # Block T+1 .. T+WeeksOfCover-1 from CHECK B

In the explanation, for every trigger include:
  "Week T: OrderQtyType={type}, base={base}, pack={pack}, rounded={qty}, MOQ={MOQ} -> PLACE/SKIP"

════════════════════════════════════════════
OUTPUT — raw JSON only, no markdown, no prose
════════════════════════════════════════════
{
  "status": "ok" | "no_demand" | "error",
  "scope": {"item_number": "str", "item_key": int, "warehouse_code": "str"},
  "explanation": "Step-by-step: WeeksOfCover used, WhStock[0..N], each trigger check, each order placed or skipped with reason",
  "recommendations": [
    {
      "item_key": int,
      "central_warehouse_key": int,
      "warehouse_code": "str",
      "rec_order_week": int,
      "delivery_date": "YYYY-MM-DD",
      "order_qty": int,
      "weeks_cover": int,
      "reasoning": "str"
    }
  ],
  "warehouse_views": [
    {
      "warehouse_code": "str",
      "weekly_projection": [
        {
          "week_index": int,
          "year_week": "str",
          "forecast": float,
          "demand": float,
          "new_order": float,
          "whstock": float,
          "ststock": float
        }
      ]
    }
  ]
}

RULES:
- warehouse_views MUST show all 53 weeks.
- whstock = YOUR computed value. Never copy from any DB column.
- new_order[n] = qty you place for delivery in week n, else 0.
- ALWAYS explain trigger/gate decisions including skipped orders.
- Do NOT confuse CalcWeekNo (1..53) with YearAndWeek (YYYYWW).
- "Danish warehouse" means DK01WH.
"""


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
            credential=credential,
            agent_id=resolved_agent_id,
            model_deployment_name=MODEL_DEPLOYMENT_NAME,
        )
        tools = [
            get_item_ordering_data,
            get_fpo_source_table,
        ]
        return Agent(
            client=chat_client,
            tools=tools,
        )


async def create_core_ordering_agent() -> str:
    _patch_ai_projects_agents_compat()
    async with get_azure_credential() as credential:
        async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
            instructions = AGENT_INSTRUCTIONS
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
