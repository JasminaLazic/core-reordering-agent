import os
import re
import json
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agents.core_ordering_agent import get_core_ordering_agent
from agents.tools.core_ordering_tools import (
    get_reorder_context,
    get_sales_history,
    get_forecast,
    get_item_master,
    run_planning_tools_readonly_query,
    validate_proposal,
    get_complete_ordering_context,
    format_ordering_context_for_prompt,
)


class QueryRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    store_id: str = Field(default="ALL")
    start_date: str | None = None
    end_date: str | None = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    store_id: str | None = None


class ReorderDataRequest(BaseModel):
    item_number: str = Field(..., min_length=1)
    central_warehouse_code: str | None = None
    weeks_cover: int = 4
    week_start: int = 1
    history_weeks: int = 8
    horizon_weeks: int = 8


class ValidateProposalRequest(BaseModel):
    quantity: int
    reorderPoint: int
    casePack: int
    moq: int = 0


_ITEM_NUMBER_RE = re.compile(r"\bitem\s*(?:number)?\s*([0-9]{4,})", flags=re.IGNORECASE)
_WAREHOUSE_CODE_RE = re.compile(r"\b([A-Z]{2}[0-9]{2}WH)\b", flags=re.IGNORECASE)


def _extract_item_number(prompt: str) -> str | None:
    match = _ITEM_NUMBER_RE.search(prompt or "")
    return match.group(1) if match else None


def _extract_warehouse_code(prompt: str) -> str | None:
    match = _WAREHOUSE_CODE_RE.search((prompt or "").upper())
    return match.group(1).upper() if match else None


def _build_core_ordering_grounded_prompt(prompt: str) -> str:
    """
    Pre-fetch ALL data for the item and embed it in the prompt.

    This eliminates the need for the agent to make 7+ separate tool calls
    (each a separate LLM API round-trip with accumulated context), reducing
    total token usage from ~90k to ~7k per request.
    """
    user_prompt = (prompt or "").strip()
    item_number = _extract_item_number(user_prompt)
    if not item_number:
        return user_prompt

    warehouse_code = _extract_warehouse_code(user_prompt)

    try:
        ctx = get_complete_ordering_context(
            item_number=item_number,
            central_warehouse_code=warehouse_code,
        )
    except Exception as e:
        return f"{user_prompt}\n\nData fetch error: {e}"

    status = ctx.get("status", "")
    if status != "ok":
        return (
            f"{user_prompt}\n\n"
            f"Data note: {ctx.get('message', 'No data available for this item')}"
        )

    data_text = format_ordering_context_for_prompt(ctx)
    return f"{user_prompt}\n\n{data_text}"


def _try_parse_json_response(text: str) -> dict[str, Any] | None:
    if not text or not isinstance(text, str):
        return None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if fenced:
        try:
            obj = json.loads(fenced.group(1).strip())
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    first = text.find("{")
    last = text.rfind("}")
    if first >= 0 and last > first:
        try:
            obj = json.loads(text[first:last + 1])
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    return None


def _has_usable_structure(obj: dict[str, Any] | None) -> bool:
    """Check if the response has a usable structure — either recommendations or warehouse_views."""
    if not obj:
        return False
    if isinstance(obj.get("recommendations"), list) and obj["recommendations"]:
        return True
    views = obj.get("warehouse_views")
    if isinstance(views, list) and views:
        first = views[0]
        if isinstance(first, dict) and isinstance(first.get("weekly_projection"), list):
            return True
    return False


async def _close_agent_if_possible(agent: Any) -> None:
    if agent is None:
        return
    chat_client = getattr(agent, "chat_client", None)
    close_fn = getattr(chat_client, "close", None)
    if callable(close_fn):
        try:
            await close_fn()
        except Exception:
            pass


app = FastAPI(title="Core Reordering Agent API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return """
    <html>
      <body>
        <h3>Core Reordering Agent API is running.</h3>
        <p>Open <a href="/index.html">/index.html</a> to use the frontend.</p>
      </body>
    </html>
    """


@app.get("/index.html")
def serve_index() -> FileResponse:
    return FileResponse(
        os.path.join(BASE_DIR, "index.html"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


async def _run_agent_query(prompt: str) -> str:
    """Run agent with pre-fetched data in prompt. Lightweight format retry."""
    agent: Any = None
    try:
        agent = await get_core_ordering_agent()
        enriched = _build_core_ordering_grounded_prompt(prompt)
        result = await agent.run(enriched)
        text = result.text if result else "No response from agent"

        parsed = _try_parse_json_response(text)
        if parsed is not None:
            return json.dumps(parsed, ensure_ascii=False)

        # Lightweight retry: only ask for format fix, don't re-analyze.
        # Uses a short prompt so the LLM mostly just reformats its prior output.
        retry_prompt = (
            "Your previous response was not valid JSON. "
            "Re-emit ONLY the JSON object (no markdown fences, no prose). "
            "Keep the same analysis and numbers."
        )
        retry_result = await agent.run(retry_prompt)
        retry_text = retry_result.text if retry_result else text
        retry_parsed = _try_parse_json_response(retry_text)
        if retry_parsed is not None:
            return json.dumps(retry_parsed, ensure_ascii=False)

        return retry_text
    finally:
        await _close_agent_if_possible(agent)


@app.post("/api/query")
async def query(req: QueryRequest) -> Dict[str, Any]:
    try:
        text = await _run_agent_query(req.prompt.strip())
        return {
            "type": "chat",
            "agent": "core_ordering",
            "response": text,
        }
    except Exception as e:
        return {
            "type": "error",
            "error": str(e),
            "hint": (
                "Core ordering agent failed. Ensure CORE_ORDERING_AGENT_ID is set and "
                "PlanningToolsDB SQL env vars are configured."
            ),
        }


@app.post("/api/chat")
async def chat(req: ChatRequest) -> Dict[str, Any]:
    try:
        text = await _run_agent_query(req.message.strip())
        return {
            "type": "chat",
            "agent": "core_ordering",
            "response": text,
        }
    except Exception as e:
        return {
            "type": "chat",
            "error": str(e),
            "hint": (
                "Ensure .env has AI_FOUNDRY_PROJECT_ENDPOINT, MODEL_DEPLOYMENT_NAME, "
                "CORE_ORDERING_AGENT_ID and PlanningToolsDB SQL configuration."
            ),
        }


@app.post("/api/reorder/context")
def reorder_context(req: ReorderDataRequest) -> Dict[str, Any]:
    """Direct data endpoint from FPO PlanningToolsDB tools."""
    context = get_reorder_context(
        item_number=req.item_number,
        central_warehouse_code=req.central_warehouse_code,
        weeks_cover=req.weeks_cover,
        week_start=req.week_start,
    )
    sales = get_sales_history(
        item_number=req.item_number,
        week_start=req.week_start,
        history_weeks=req.history_weeks,
    )
    forecast = get_forecast(
        item_number=req.item_number,
        central_warehouse_code=req.central_warehouse_code,
        horizon_weeks=req.horizon_weeks,
        weeks_cover=req.weeks_cover,
    )
    return {
        "type": "reorder_context",
        "context": context,
        "sales_history": sales,
        "forecast": forecast,
    }


@app.post("/api/reorder/validate")
def reorder_validate(req: ValidateProposalRequest) -> Dict[str, Any]:
    """Validate/enforce reorder constraints for a proposed quantity."""
    return {
        "type": "proposal_validation",
        **validate_proposal(
            quantity=req.quantity,
            reorderPoint=req.reorderPoint,
            casePack=req.casePack,
            moq=req.moq,
        ),
    }
