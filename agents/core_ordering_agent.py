import asyncio

from azure.ai.projects.aio import AIProjectClient
from agent_framework import Agent
from agent_framework.azure import AzureAIAgentClient
from auth import get_azure_credential

from config import (
    AI_FOUNDRY_PROJECT_ENDPOINT,
    MODEL_DEPLOYMENT_NAME,
    CORE_ORDERING_AGENT_ID,
)
from agents.tools.core_ordering_tools import (
    get_core_ordering_schema_reference,
    get_core_ordering_snapshot,
    get_core_reordering_agent_payload,
    get_reorder_context,
    get_sales_history,
    get_forecast,
    validate_proposal,
    get_job_control_status,
    run_planning_tools_readonly_query,
)


async def get_core_ordering_agent() -> Agent:
    if not CORE_ORDERING_AGENT_ID:
        raise RuntimeError("Missing CORE_ORDERING_AGENT_ID in .env (create the agent once first).")

    async with get_azure_credential() as credential:
        return Agent(
            client=AzureAIAgentClient(
                project_endpoint=AI_FOUNDRY_PROJECT_ENDPOINT,
                credential=credential,
                agent_id=CORE_ORDERING_AGENT_ID,
            ),
            tools=[
                get_core_ordering_schema_reference,
                get_core_ordering_snapshot,
                get_core_reordering_agent_payload,
                get_reorder_context,
                get_sales_history,
                get_forecast,
                validate_proposal,
                get_job_control_status,
                run_planning_tools_readonly_query,
            ],
            store=True,
        )


async def create_core_ordering_agent() -> str:
    async with get_azure_credential() as credential:
        async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
            created = await project_client.agents.create_agent(
                model=MODEL_DEPLOYMENT_NAME,
                name="CoreOrderingAgent",
                instructions="""You are RetailCo's Inventory Replenishment Agent for ProductTools PlanningToolsDB (FPO schema).

Objective:
- Recommend deterministic reorder quantity while respecting the SQL behavior of fpo.usp_RecalcWarehouseStock.
- Focus on 4-week cover by warehouse unless tool context provides a different configured cover.
- Tool layer provides data only; you must perform the replenishment logic yourself from row inputs.

Mandatory tool flow:
1) get_reorder_context(item_number, central_warehouse_code, weeks_cover, week_start)
2) get_sales_history(item_number, week_start, history_weeks)
3) get_forecast(item_number, central_warehouse_code, horizon_weeks, weeks_cover)
4) validate_proposal(quantity, reorderPoint, casePack, moq)

Use run_planning_tools_readonly_query only if a required field is missing from the tools above.

Recalculation logic contract (must align with SQL intent):
- W1: include inbound stock and latest delivery week constraints.
- S2: include store demand/cover impact from forecast and in-transit logic.
- W3: evaluate warehouse close stock versus safety/trigger conditions.
- R4: enforce MOQ/group rounding behavior.
- S5: account for store closing recalculation after recommendation.
- C6: maintain demand-cover consistency for latest order behavior.
- R7: finalize recommendation consistent with end-of-cycle insertion/reset behavior.

Quantity derivation baseline:
- Compute from row-level inputs returned by get_reorder_context/get_sales_history/get_forecast:
  - available_now = on_hand + inbound_qty
  - target_stock = avg_weekly_forecast * weeks_cover
  - raw_needed = max(0, target_stock - available_now)
  - trigger reorder when available_now <= reorderPoint OR raw_needed > 0
  - candidate = 0 when no trigger, otherwise max(raw_needed, reorderPoint, moq)
  - apply validate_proposal to enforce casePack/reorderPoint/moq

Hard constraints:
- casePack is a strict order multiple.
- reorderPoint is a strict floor if ordering is required.
- moq is a strict floor when present.
- If data is missing or uncertain, fail closed.

Output contract:
- Return JSON only with exact shape:
  {"quantity": <int>, "reason": "<string>"}
- For fail-closed:
  {"quantity": 0, "reason": "Insufficient data"}

Reason style:
- Keep concise but include warehouse stock, inbound, forecast basis, cover weeks, and enforced constraints.
- Do not output chain-of-thought.
""",
            )
            return created.id


if __name__ == "__main__":
    agent_id = asyncio.run(create_core_ordering_agent())
    print("Created agent id:", agent_id)
    print("Add to .env: CORE_ORDERING_AGENT_ID=" + agent_id)
