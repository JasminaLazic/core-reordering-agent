# Core Ordering Agent Setup (PlanningToolsDB)

This guide sets up a read-only Azure agent that answers core re-ordering questions
using the same PlanningToolsDB structure used in ProductTools.

## What was added

- Agent file: `agents/core_ordering_agent.py`
- Tools file: `agents/tools/core_ordering_tools.py`
- API routing (query/chat): `api.py` (`agent: core_ordering`)
- Config var: `CORE_ORDERING_AGENT_ID` in `config.py`

## Capability

The agent can:

- Explain schema/objects used in core re-ordering (`model`, `fpo`, `am` objects)
- Compute reorder recommendations from tables (4-week cover + MOQ + rounding)
- Fetch ordering snapshots using canonical tables
- Read jobControl/job history status
- Run custom read-only SQL (SELECT/CTE only)

## 1) Expose data to the agent (recommended way)

You should expose PlanningToolsDB via SQL connection env vars (read-only account):

- `PLANNING_TOOLS_SQL_CONNECTION_STRING`
  - Recommended single var format:
  - `DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=PlanningTools;UID=...;PWD=...;Encrypt=yes;TrustServerCertificate=yes;`

Alternative split vars:

- `PLANNING_TOOLS_SQL_SERVER`
- `PLANNING_TOOLS_SQL_DATABASE` (default: `PlanningTools`)
- `PLANNING_TOOLS_SQL_USERNAME`
- `PLANNING_TOOLS_SQL_PASSWORD`
- `PLANNING_TOOLS_SQL_DRIVER` (default: `ODBC Driver 18 for SQL Server`)

### Local mock mode (SQLite, no real SQL Server yet)

You can run the same tools against a local mock DB:

```bash
python3 scripts/create_planningtools_mock_sqlite.py
```

Then set:

```bash
PLANNING_TOOLS_SQLITE_PATH=/absolute/path/to/retail-analytics/data/planningtools_mock.sqlite
```

When `PLANNING_TOOLS_SQLITE_PATH` is set, core ordering tools use SQLite mock data.
When it is not set, they use SQL Server settings above.

Important:
- Use a SQL user with **read-only permissions**.
- Grant only required schemas/tables/functions.

## 2) Install dependency

If not already installed:

```bash
pip install pyodbc
```

Also ensure local ODBC driver is installed (`ODBC Driver 18 for SQL Server`).

## 3) Create the agent in Foundry

```bash
cd agents
python core_ordering_agent.py
```

Copy output ID into `.env`:

```bash
CORE_ORDERING_AGENT_ID=asst_...
```

## 4) Run and use

Start app:

```bash
uvicorn api:app --reload --port 8000
```

In UI (`/index.html`), try prompts:

- `Calculate core reorder for 4 weeks cover for item 123456`
- `Reorder recommendation for warehouse W01 using MOQ and order multiples`
- `Use config cover weeks and give recommendation for item 123456 in warehouse W01`
- `Show job control status for core reordering`
- `Show warehouse stock snapshot for item 123456`
- `Explain core reordering flow used in PlanningToolsDB`
- `Run PlanningTools query: SELECT TOP 20 * FROM am.tbl_JobControl ORDER BY LastRunAt DESC`

## 5) Security / guardrails

- Custom query tool allows **read-only** SELECT/CTE only.
- Non-read statements are rejected.
- Keep DB credential scoped to reporting/analysis only.

## 6) Reordering logic model

The agent reads canonical ProductTools tables and computes recommendation logic directly from data:

- `model.tbl_StockWarehouseOnHand`
- `model.tbl_StoreWarehouseRelationship`
- `model.tbl_CoreAssortment`
- `fpo.tbl_ForecastStoreSales`
- `fpo.tbl_ItemWarehouse`
- `fpo.tbl_ItemWarehouseOrderQty`
- `fpo.tbl_ImportCoverConfig`

Core formula (agent-driven):

- `target_stock = weekly_avg_demand * cover_weeks`
- `base_needed = target_stock - (on_hand + inbound)`
- `recommended_qty = ceil(max(base_needed, MOQ) / order_multiple) * order_multiple`

It keeps the same data structure and business intent while running the logic in the agent layer.
