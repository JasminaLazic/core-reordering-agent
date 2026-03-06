import os
import json
import re
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agents.core_ordering_agent import get_core_ordering_agent
from agents.tools.core_ordering_tools import (
    get_core_reordering_agent_payload,
    get_reorder_context,
    get_sales_history,
    get_forecast,
    validate_proposal,
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


def _extract_item_number_for_core_ordering(prompt: str) -> str | None:
    m = re.search(r"\bitem(?:\s*number)?\s*[:=]?\s*(\d{4,})\b", prompt, re.IGNORECASE)
    return m.group(1) if m else None


def _extract_item_key_for_core_ordering(prompt: str) -> int | None:
    m = re.search(r"\bitem[_\s-]?key\s*[:=]?\s*(\d+)\b", prompt, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_week_start_for_core_ordering(prompt: str) -> int | None:
    m = re.search(r"\b(?:week[_\s]?start|@weekstart)\s*[:=]?\s*(\d{1,2})\b", prompt, re.IGNORECASE)
    if not m:
        return None
    value = int(m.group(1))
    return value if 1 <= value <= 53 else None


def _extract_warehouse_code_for_core_ordering(prompt: str) -> str | None:
    explicit = re.search(
        r"\b(?:warehouse|central_warehouse_code|wh|dc)\s*[:=]?\s*([A-Z0-9]{2,12})\b",
        prompt,
        re.IGNORECASE,
    )
    if explicit:
        return explicit.group(1).upper()

    generic = re.search(r"\b([A-Z]{1,4}\d{2}(?:WH)?)\b", prompt.upper())
    return generic.group(1) if generic else None


def _build_core_ordering_grounded_prompt(prompt: str) -> str:
    item_number = _extract_item_number_for_core_ordering(prompt)
    item_key = _extract_item_key_for_core_ordering(prompt)
    week_start = _extract_week_start_for_core_ordering(prompt) or 1
    warehouse_code = _extract_warehouse_code_for_core_ordering(prompt)

    if not item_number and item_key is None:
        return (
            f"{prompt}\n\n"
            "Missing required item identifier.\n"
            "Return JSON only: {\"quantity\": 0, \"reason\": \"Insufficient data\"}."
        )

    try:
        payload = get_core_reordering_agent_payload(
            week_start=week_start,
            item_scope="item",
            item_number=item_number,
            item_key=item_key,
            top_n=250,
        )
        if payload.get("payload_errors"):
            return (
                f"{prompt}\n\n"
                "Deterministic payload prefetch produced blocking errors. "
                "Return JSON only: {\"quantity\": 0, \"reason\": \"Insufficient data\"}.\n"
                f"Blocking payload: {json.dumps(payload, default=str)}"
            )
        return (
            f"{prompt}\n\n"
            "Use FPO PlanningToolsDB tools to compute reorder quantity from row-level warehouse inputs.\n"
            "Call get_reorder_context, get_sales_history, get_forecast, then validate_proposal.\n"
            "Compute using row logic: available_now=on_hand+inbound, target_stock=avg_weekly_forecast*weeks_cover, raw_needed=max(0,target_stock-available_now).\n"
            "Trigger reorder when available_now<=reorderPoint OR raw_needed>0.\n"
            "If reorder triggered, candidate=max(raw_needed,reorderPoint,moq), then validate_proposal.\n"
            "Enforce casePack, reorderPoint, and moq constraints.\n"
            "If multiple warehouses appear and no warehouse is specified, return quantity 0 with reason 'Insufficient data'.\n"
            "If required fields are missing/uncertain, return quantity 0 with reason 'Insufficient data'.\n"
            "Output JSON only with exact shape: {\"quantity\": <int>, \"reason\": \"<string>\"}.\n"
            f"Parsed context: item_number={item_number}, item_key={item_key}, week_start={week_start}, warehouse_code={warehouse_code}.\n"
            f"Payload: {json.dumps(payload, default=str)}"
        )
    except Exception as e:
        return (
            f"{prompt}\n\n"
            "Deterministic payload prefetch failed before agent reasoning.\n"
            f"Prefetch error: {e}\n"
            "Return JSON only: {\"quantity\": 0, \"reason\": \"Insufficient data\"}."
        )


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


@app.post("/api/query")
async def query(req: QueryRequest) -> Dict[str, Any]:
    prompt = req.prompt.strip()
    try:
        agent = await get_core_ordering_agent()
        result = await agent.run(_build_core_ordering_grounded_prompt(prompt))
        return {
            "type": "chat",
            "agent": "core_ordering",
            "response": result.text if result else "No response from agent",
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
        agent = await get_core_ordering_agent()
        result = await agent.run(_build_core_ordering_grounded_prompt(req.message.strip()))
        return {
            "type": "chat",
            "agent": "core_ordering",
            "response": result.text if result else "No response from agent",
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
    """
    Direct deterministic data endpoint from FPO PlanningToolsDB tools.
    """
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
    """
    Validate/enforce reorder constraints for a proposed quantity.
    """
    return {
        "type": "proposal_validation",
        **validate_proposal(
            quantity=req.quantity,
            reorderPoint=req.reorderPoint,
            casePack=req.casePack,
            moq=req.moq,
        ),
    }
