# Core Reordering Agent

Standalone extraction of the core ordering components from `azure-agents/retail-analytics`.

## Included

- Core ordering Azure agent (`agents/core_ordering_agent.py`)
- Core ordering tools (`agents/tools/core_ordering_tools.py`)
- FastAPI entrypoint (`api.py`) with `/api/query` and `/api/chat`
- Auth and config (`auth.py`, `config.py`)
- Core scripts (`scripts/run_core_ordering_agent.py`)
- Core setup doc (`docs/CORE_ORDERING_AGENT_SETUP.md`)
- Data directory (`data/`)

## Quick start

1. Install dependencies:

```bash
pip install -r requirements.txt
pip install fastapi uvicorn pyodbc
```

2. Create `.env` from `ENV_EXAMPLE` and set values.
   - For offline local testing, set `IS_LOCAL=true` to use built-in mock PlanningTools tables/data.
   - For real SQL Server usage, set `IS_LOCAL=false` (or omit) and provide `PLANNING_TOOLS_SQL_*` values.

3. Run API:

```bash
uvicorn api:app --reload --port 8010
```

4. Open:

- `http://127.0.0.1:8010/index.html`

