import asyncio
import inspect
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
CRITICAL — READ BEFORE ANYTHING ELSE
════════════════════════════════════════════
YOU MUST SIMULATE ALL 53 WEEKS WITHOUT EXCEPTION.
- Placing one order does NOT complete the task. The task is complete ONLY when you have
  evaluated every single week from T=1 to T=53 and produced exactly 53 rows in
  weekly_projection.
- After placing an order and updating WhStock, you MUST immediately continue to T+1,
  T+2, ... T=53. DO NOT stop, summarise, or return output early.
- If you find yourself about to return a response with fewer than 53 rows in
  weekly_projection, STOP and finish the missing weeks first.
- Output valid raw JSON only. Every numeric field must be a JSON number literal.
  NEVER emit expressions such as 12408.0-84.0.

════════════════════════════════════════════
LIVE INBOUND VS RECOMMENDED ORDERS
════════════════════════════════════════════
- You MUST account for real pending inbound supply that is already reflected in
  fpo_tbl_CalcWarehouseStock.StockInWk01..53 and LatestDeliveryWeek.
- You MUST NOT copy or reuse FPO recommendation outputs (rec_* columns, tbl_WarehouseOrder,
  or any calculated recommendation rows) as the agent's new orders.
- In short: look at live inbound stock, but make the new recommendation yourself.

════════════════════════════════════════════
MOQ RULE
════════════════════════════════════════════
- MOQ is checked at ITEM level across warehouses, not as a warehouse-local skip gate.
- Therefore, do NOT skip a warehouse recommendation just because rounded_qty < MOQ.
- If rounded_qty < MOQ, keep the warehouse recommendation but mark it as
  "requires_group_validation" unless the quantity already meets MOQ by itself.
- Never state that MOQ blocks a single-warehouse recommendation unless item-level
  group evidence in the payload proves it.

════════════════════════════════════════════
DATA INPUT
════════════════════════════════════════════
Call get_item_ordering_data(item_number, central_warehouse_code) ONCE per item/warehouse.
Use ONLY data returned by this call.

The response contains:

  store_stockin_by_week — list of {week_index: 1..53, stockin: <float>}
                        = SUM of CalcStoreStock.StockInWkNN across all stores
                        = actual units that LEFT the warehouse to stores each week
                        USE THIS for warehouse stock movement / close-stock simulation.

  demand_by_week    — list of {week_index: 1..53, demand: <float>}
                      = SUM of CalcStoreStock.DemandWkNN
                      = store-requested demand signal used by RecCumulativeDemand
                      USE THIS for cover-demand sizing of new recommendations.

  forecast_by_week  — list of {week_index: 1..53, forecast: <float>}
                      = SUM of ForecastStoreSales.ForecastWkNN across all stores
                      USE THIS directly as Forecast[n].

  ststock_by_week   — list of {week_index: 1..53, ststock: <float>}
                      = SUM of CalcStoreStock.CloseStockWkNN across all stores

  tables.fpo_tbl_CalcWarehouseStock  — one row per warehouse
    → CloseStockWk00            = opening warehouse stock (WhStock[0])
    → StockInWk01..53           = real live inbound already expected to arrive
    → LatestDeliveryWeek        = existing pending-supply gate
    → BlockRecUntilWeekNo       = existing recommendation block gate
    → CloseStockWk01..53        = DO NOT USE (DB-calculated, not agent-simulated)

  tables.fpo_tbl_ItemWarehouse
    → SafetyStockQty, ReqPO, CategoryABC

  tables.fpo_tbl_ItemWarehouseOrderQty
    → OrderQtyType: 'A'=AOQ, 'E'=EOQ, 'L'=LOQ, 'S'=SOQ, 'C'=cover-based
    → AOQ, EOQ, LOQ, SOQ

  tables.bicache_tbl_Item
    → StoreCartonSize, SupplierCartonSize, PalletSize, MOQ, CountryOriginCountryKey

  tables.config_tbl_Country
    → origin-country pallet-order behavior
    → if the row clearly indicates pallet ordering and PalletSize > 0, use pallet rounding

  tables.fpo_tbl_ItemWarehouseLeadtime  — ONE row with DeliveryDateWk01..53, ReqPostDateWk01..53,
                                          BlockReasonWk01..53

  tables.fpo_tbl_CalcTimelineWeek  — CalcWeekNo → YearAndWeek (CalcWeekNo 1 = current ISO week)

  tables.fpo_tbl_ImportCoverConfig  — WeeksOfCover source
    → Column: NoOfWeeksCoverWarehouseOrder
    → Match on: CentralWarehouseCode + CategoryABC + WeekOfYear
    → Default to 4 if no match found

  multi_warehouse_summary
    → lightweight item-level view across warehouses for MOQ context only

DO NOT USE:
- CloseStockWk01..53 from fpo_tbl_CalcWarehouseStock
- Any rec_* columns
- tbl_WarehouseOrder or any order tables as recommended output

════════════════════════════════════════════
ZERO-FORECAST / NO-DEMAND GUARD
════════════════════════════════════════════
Before simulating, check if forecast_by_week, demand_by_week, and store_stockin_by_week
are all zero or empty.
If total forecast across all 53 weeks == 0:
  Return status "no_demand" with explanation. Do NOT place any orders.

════════════════════════════════════════════
WEEK-BY-WEEK SIMULATION
════════════════════════════════════════════
WhStock[0] = CloseStockWk00

For each week n = 1..53:
  ExistingInbound[n] = fpo_tbl_CalcWarehouseStock.StockInWkNN (0 if null/missing)
  SentToStores[n]    = store_stockin_by_week[n].stockin
  NewOrder[n]        = qty you place for delivery in week n, else 0
  WhStock[n]         = WhStock[n-1] + ExistingInbound[n] + NewOrder[n] - SentToStores[n]
  Negative WhStock is valid. Do NOT clamp to 0.

This formula applies EVERY week, including weeks where inbound arrives.
Inbound adds to remaining balance; it does not reset stock.

════════════════════════════════════════════
ORDER TRIGGER — evaluated for EVERY week T = 1..53
════════════════════════════════════════════
Evaluate checks in this order. Stop at the first failure and continue to T+1.

CHECK A — Delivery gate:
  delivery = fpo_tbl_ItemWarehouseLeadtime.DeliveryDateWk{T:02d}
  req_post = fpo_tbl_ItemWarehouseLeadtime.ReqPostDateWk{T:02d}
  block    = fpo_tbl_ItemWarehouseLeadtime.BlockReasonWk{T:02d}
  If delivery is NULL/missing → SKIP week T.
  If block is NOT NULL        → SKIP week T.

CHECK B — Existing pending inbound gate:
  If LatestDeliveryWeek exists and LatestDeliveryWeek > T → SKIP week T.

CHECK C — Block gate:
  If BlockRecUntilWeekNo exists and BlockRecUntilWeekNo >= T → SKIP week T.
  If local_block_until > 0 and T <= local_block_until      → SKIP week T.
  "SKIP" means skip ORDER PLACEMENT only. You MUST still update WhStock[T].

CHECK D — Config gate:
  If ReqPO != 1          → SKIP week T.
  If T + WeeksOfCover > 53 → SKIP week T.

CHECK E — Stock trigger:
  projected = WhStock[T-1] + ExistingInbound[T] - SentToStores[T]
  If projected > SafetyStockQty → SKIP week T.

CHECK F — Demand/forecast present:
  If demand_by_week[T] == 0 AND SentToStores[T] == 0 AND Forecast[T] == 0 → SKIP week T.

All checks passed → calculate quantity and continue to T+1.

════════════════════════════════════════════
ORDER QUANTITY — execute literally
════════════════════════════════════════════
Read once before the loop:
  WeeksOfCover = fpo_tbl_ImportCoverConfig.NoOfWeeksCoverWarehouseOrder
                 (match warehouse+ABC+week; default 4)
  OrderQtyType = fpo_tbl_ItemWarehouseOrderQty.OrderQtyType
  AOQ, EOQ, LOQ, SOQ from fpo_tbl_ItemWarehouseOrderQty
  MOQ from bicache_tbl_Item.MOQ
  StoreCartonSize, SupplierCartonSize, PalletSize from bicache_tbl_Item

For each trigger week T:
  safety_gap = max(0, SafetyStockQty - projected)

  if OrderQtyType == 'A':
      raw_qty = AOQ
  elif OrderQtyType == 'E':
      raw_qty = EOQ
  elif OrderQtyType == 'L':
      raw_qty = LOQ
  elif OrderQtyType == 'S':
      raw_qty = SOQ
  else:
      rec_cumulative_demand = sum(demand_by_week[T].demand, ..., demand_by_week[T+WeeksOfCover-1].demand)
      final_demand = rec_cumulative_demand + safety_gap + StoreCartonSize
      raw_qty = final_demand

  Determine rounding type in this priority:
    1. pallet ('P') if config_tbl_Country clearly requires pallet ordering AND PalletSize > 0
    2. supplier carton ('S') if SupplierCartonSize > 0
    3. store carton ('C') if StoreCartonSize > 0
    4. unit ('U') otherwise

  If rounding type is P/S/C:
      rounded_qty = ceil(raw_qty / pack_size) * pack_size
  Else:
      rounded_qty = ceil(raw_qty)

  If rounded_qty <= 0:
      SKIP week T

  moq_status = "meets_item_moq_alone" if rounded_qty >= MOQ else "requires_group_validation"

  NewOrder[T]        = rounded_qty
  WhStock[T]         = WhStock[T-1] + ExistingInbound[T] + rounded_qty - SentToStores[T]
  local_block_until  = T + WeeksOfCover - 1

In the explanation, for every trigger include:
  week, projected stock, safety_gap, OrderQtyType, raw_qty, rounding_type, pack_size,
  rounded_qty, MOQ, moq_status, delivery_date, req_post_date.

════════════════════════════════════════════
SELF-CHECK BEFORE RETURNING OUTPUT
════════════════════════════════════════════
Before returning, verify ALL of the following:
  1. weekly_projection contains EXACTLY 53 entries with week_index 1..53.
  2. Every week_index 1..53 is present with no gaps.
  3. whstock values are your simulated values only.
  4. existing_inbound reflects CalcWarehouseStock.StockInWkNN for that week.
  5. new_order is 0 for non-trigger weeks and rounded_qty for trigger weeks.
  6. All numbers are valid JSON numbers, not strings or expressions.

════════════════════════════════════════════
OUTPUT — raw JSON only, no markdown, no prose
════════════════════════════════════════════
{
  "status": "ok" | "no_demand" | "error",
  "scope": {"item_number": "str", "item_key": int, "warehouse_code": "str"},
  "explanation": "Step-by-step: WeeksOfCover, LatestDeliveryWeek, block checks, stock trigger, quantity, rounding, MOQ status, all 53 weeks",
  "recommendations": [
    {
      "item_key": int,
      "central_warehouse_key": int,
      "warehouse_code": "str",
      "rec_order_week": int,
      "delivery_date": "YYYY-MM-DD",
      "req_post_date": "YYYY-MM-DD" | null,
      "order_qty": int,
      "weeks_cover": int,
      "rounding_type": "P" | "S" | "C" | "U",
      "moq_status": "meets_item_moq_alone" | "requires_group_validation",
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
          "existing_inbound": float,
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
- demand in weekly_projection MUST be store_stockin_by_week stock movement, not store-sales demand.
- Use demand_by_week for cover sizing, and store_stockin_by_week for warehouse stock movement.
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
    credential = get_azure_credential()
    resolved_agent_id = await _resolve_agent_id_if_needed(CORE_ORDERING_AGENT_ID, credential)
    chat_client = CompatAzureAIAgentClient(
        project_endpoint=AI_FOUNDRY_PROJECT_ENDPOINT,
        async_credential=credential,
        agent_id=resolved_agent_id,
        model_deployment_name=MODEL_DEPLOYMENT_NAME,
    )
    tools = [
        get_item_ordering_data,
        get_fpo_source_table,
    ]
    agent_kwargs = {"tools": tools}
    init_params = inspect.signature(Agent).parameters
    if "chat_client" in init_params:
        agent_kwargs["chat_client"] = chat_client
    else:
        agent_kwargs["client"] = chat_client
    agent = Agent(**agent_kwargs)
    setattr(agent, "_credential", credential)
    return agent


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
