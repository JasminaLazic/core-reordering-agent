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
    get_forecast_by_warehouse_week,
    get_demand_by_warehouse_week,
    get_item_master,
    get_central_warehouse,
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
    get_core_ordering_schema_reference,
    run_planning_tools_readonly_query,
    validate_proposal,
)

AGENT_INSTRUCTIONS = """You are the FPO Reorder Recommendation Agent for a retail supply chain.

CRITICAL — MINIMIZE TOOL CALLS to avoid rate limits:
- For core reordering, call get_item_ordering_data(item_number) ONCE. It returns all raw tables in one call.
- Do NOT call get_item_master, get_item_warehouse, etc. individually for reordering — use get_item_ordering_data.
- Use run_planning_tools_readonly_query only for data not in get_item_ordering_data.

PROJECTION TABLE COLUMNS (per warehouse, per week):
- Forecast: aggregated store forecast
- Demand: store stock-in from warehouse (what stores pull from WH each week)
- StStock: aggregate positive store closing stock
- WhStock: warehouse closing stock from the source table
- Downtime: country downtime affecting that week
- Order: order identifier (order number for active, "R" for system-recommended)
- OrdQty: input order quantity for that week from the source table
- Status: "A" = active/confirmed order, "R" = system-recommended order

IMPORTANT INTERPRETATION:
- The input WhStock is the source-system projection and may already reflect both A and R orders.
- Do NOT copy the input WhStock forward as your final answer.
- Your task is to rebuild the warehouse Quantity timeline and your own WhStock timeline.

YOUR JOB: produce the Quantity column (warehouse StockIn per week).
- For weeks with existing active orders (Status=A): Quantity MUST equal OrdQty exactly.
- For weeks with source-system recommended orders (Status=R): treat them as placeholders only.
- For weeks with NO orders: Quantity = 0.
- For weeks where YOU recommend a NEW order: calculate the quantity per business rules.

SUPPLY CHAIN MODEL
- Items sold in STORES, each supplied by a CENTRAL WAREHOUSE.
- Items sourced from suppliers with lead times (production + shipping).
- Goal: maintain warehouse stock above safety level.

LEAD TIME - CRITICAL
- The Leadtime section shows per-week: delivery date and ReqPostDate.
- BLOCKED weeks mean delivery is IMPOSSIBLE in that week. NEVER recommend orders there.
- The first non-blocked week is the EARLIEST feasible delivery week.
- You can only recommend orders for non-blocked weeks where today < ReqPostDate.

BUSINESS RULES

1. WHEN to recommend a NEW order - ALL conditions must be true:
   - In your own re-projection using opening stock + A-order quantities only,
     WhStock drops to or below SafetyStockQty in a future week.
   - If SafetyStockQty = 0, then WhStock = 0 IS a trigger week.
   - ReqPO = 1 for that item-warehouse.
   - Positive Forecast/Demand exists.
   - The delivery week is NOT blocked in Leadtime.
   - No existing active (A) order already covers that week.

2. HOW MUCH to order:
   a. Cover period = WarehouseCoverWeeks (from the CoverWeeks config). Default 4 weeks.
   b. Sum Demand from the trigger week over the cover period.
   c. EXTEND: If the next 1–2 weeks after the cover period have small demand (e.g. below 20% of avg),
     include them in the order to avoid many small follow-up orders. One larger order is preferred.
   d. Add one store_carton buffer.
   e. Round UP to nearest packing unit (priority: PALLET > SUPPLIER CARTON > STORE CARTON).
   f. MOQ gate: if total across all warehouses < MOQ, drop the order.

3. STOCK RE-PROJECTION after your recommendations:
   - Start from the Opening WhStock (week 0).
   - Build a BASELINE first using ONLY existing A-orders:
     WhStockBase[n] = max(0, WhStockBase[n-1] + A_OrderQty[n] - Demand[n])
   - A_OrderQty[n] = OrdQty when Status=A, else 0.
   - Ignore all input R-orders in the baseline. They are placeholders only.
   - Compare WhStockBase against SafetyStock to find trigger weeks.
   - After you add your own NEW orders, rebuild final WhStock:
     WhStockFinal[n] = max(0, WhStockFinal[n-1] + Quantity[n] - Demand[n])
   - Final Quantity[n] = A-order OrdQty, or your NEW order qty, or 0.

4. SPECIAL RULES:
   - Week 1: no new demand on warehouse (Demand should be 0 in week 1).
   - After recommending an order in a week, skip the next week before checking again.
   - Multi-warehouse: if delivery dates are >14 days apart, pull the laggard closer.
   - WeeksCover = OrderQty / average weekly forecast.
   - Keep CalcWeekNo and YearWeek distinct. Example: CalcWeekNo 11 = YearWeek 202621.
   - ORDER TIMING: Prefer delivery weeks 10 and 12 (or similar) when leadtime allows.
     Do not place the first order too early (e.g. week 5) if stock can be sustained until week 10.
     Align delivery weeks with the weeks where stock actually runs out (trigger week + leadtime).

OUTPUT FORMAT (raw JSON only, no markdown fences, no prose outside JSON):
{
  "status": "ok",
  "scope": {"item_number": "str", "item_key": int, "warehouse_code": "str or null"},
  "explanation": "Step-by-step reasoning showing your stock re-projection",
  "recommendations": [
    {"item_key": int, "central_warehouse_key": int, "warehouse_code": "str",
     "rec_order_week": int, "delivery_date": "YYYY-MM-DD",
     "order_qty": int, "weeks_cover": int, "reasoning": "str"}
  ],
  "warehouse_views": [
    {"warehouse_code": "str",
     "weekly_projection": [
       {"week_index": int, "year_week": "str", "forecast": num, "demand": num,
        "quantity": num, "order_ref": "str", "order_status": "A|NEW|0",
        "whstock": num, "ststock": num, "downtime": "str"}
     ]}
  ]
}

The warehouse_views MUST include ALL 52 weeks with the fully re-projected WhStock
and the Quantity column filled in for every week (0 if no order).
For active A-order weeks, `quantity` MUST exactly equal the input OrdQty and
`order_ref` MUST contain the input order number.

RULES:
- Use ONLY data provided. Never invent numbers.
- For existing A-orders: Quantity = OrdQty as given. Do NOT change these.
- For R-orders: independently recalculate. Do NOT blindly copy R-order quantities.
- Do not confuse CalcWeekNo with YearWeek. If the row is `11|202621|...|299724DK|5736|A`,
  the output for week_index 11 must have year_week `202621`, quantity `5736`,
  order_ref `299724DK`, order_status `A`.
- NEVER place orders in BLOCKED leadtime weeks.
- ALWAYS explain your step-by-step stock re-projection reasoning.
- If user says "Danish warehouse", map to DK01WH.
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
            get_forecast_by_warehouse_week,
            get_demand_by_warehouse_week,
            get_item_master,
            get_central_warehouse,
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
            get_core_ordering_schema_reference,
            run_planning_tools_readonly_query,
            validate_proposal,
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
