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
from agents.tools.core_ordering_tools import validate_proposal


class QueryRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    store_id: str = Field(default="ALL")
    start_date: str | None = None
    end_date: str | None = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    store_id: str | None = None


class ValidateProposalRequest(BaseModel):
    quantity: int
    reorderPoint: int
    casePack: int
    moq: int = 0


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
    """Run agent query. The agent calls table-fetch tools to gather data itself."""
    agent: Any = None
    try:
        agent = await get_core_ordering_agent()
        result = await agent.run(prompt.strip())
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
