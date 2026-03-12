import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.core_ordering_agent import get_core_ordering_agent
import agents.tools.core_ordering_tools as tools


queries: list[dict[str, object]] = []
orig_query = tools._query
orig_query_safe = tools._query_safe


def logged_query(sql: str, params=None):
    queries.append({"sql": sql, "params": params or []})
    return orig_query(sql, params)


def logged_query_safe(sql: str, params=None):
    queries.append({"sql": sql, "params": params or []})
    return orig_query_safe(sql, params)


tools._query = logged_query
tools._query_safe = logged_query_safe


PROMPT = (
    "Give me reorder recommendations for item number 1450090 across all warehouses. "
    "Include weekly projections with forecast, demand, quantity, order, whstock, "
    "ststock, and downtime."
)


async def main() -> None:
    agent = await get_core_ordering_agent()
    text = ""
    error = ""
    try:
        result = await agent.run(PROMPT)
        text = result.text if result else ""
    except Exception as exc:
        error = str(exc)
    finally:
        chat_client = getattr(agent, "chat_client", None)
        close_fn = getattr(chat_client, "close", None)
        if callable(close_fn):
            try:
                await close_fn()
            except Exception:
                pass

    (ROOT / "agent_trace_item_1450090_response.txt").write_text(text or "", encoding="utf-8")
    (ROOT / "agent_trace_item_1450090_queries.json").write_text(
        json.dumps(queries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (ROOT / "agent_trace_item_1450090_error.txt").write_text(error, encoding="utf-8")
    print(json.dumps({
        "query_count": len(queries),
        "response_len": len(text or ""),
        "error": error,
    }))


if __name__ == "__main__":
    asyncio.run(main())
