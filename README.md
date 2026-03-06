# Core Reordering Agent

Standalone extraction of the core ordering components from `azure-agents/retail-analytics`.

## Included

- Core ordering Azure agent (`agents/core_ordering_agent.py`)
- Core ordering tools (`agents/tools/core_ordering_tools.py`)
- FastAPI entrypoint (`api.py`) with `/api/query` and `/api/chat`
- Auth and config (`auth.py`, `config.py`)
- Core scripts (`scripts/run_core_ordering_agent.py`, `scripts/create_planningtools_mock_sqlite.py`)
- Core setup doc (`docs/CORE_ORDERING_AGENT_SETUP.md`)
- Data directory (`data/`)

## Quick start

1. Install dependencies:

```bash
pip install -r requirements.txt
pip install fastapi uvicorn pyodbc
```

2. Create `.env` from `ENV_EXAMPLE` and set values.

3. Run API:

```bash
uvicorn api:app --reload --port 8010
```

4. Open:

- `http://127.0.0.1:8010/index.html`

