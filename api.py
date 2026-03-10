import os
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


def _build_core_ordering_grounded_prompt(prompt: str) -> str:
    # User prompt passthrough by design. No API-side gating, extraction,
    # deterministic prefetch, or fallback behavior.
    return prompt


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
                "PlanningToolsDB SQL env vars are configured, or set IS_LOCAL=true for mock mode."
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
                "CORE_ORDERING_AGENT_ID and PlanningToolsDB SQL configuration, or set IS_LOCAL=true."
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
