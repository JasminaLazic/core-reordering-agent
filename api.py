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
    get_core_ordering_inputs,
    run_planning_tools_readonly_query,
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


_ITEM_NUMBER_RE = re.compile(r"\bitem\s*number\b\s*([0-9]{4,})", flags=re.IGNORECASE)
_WAREHOUSE_CODE_RE = re.compile(r"\b([A-Z]{2}[0-9]{2}WH)\b", flags=re.IGNORECASE)


def _extract_item_number(prompt: str) -> str | None:
    match = _ITEM_NUMBER_RE.search(prompt or "")
    return match.group(1) if match else None


def _extract_warehouse_code(prompt: str) -> str | None:
    match = _WAREHOUSE_CODE_RE.search((prompt or "").upper())
    return match.group(1).upper() if match else None


def _get_projection_columns_by_week(
    item_number: str,
    warehouse_code: str,
) -> dict[str, list[float]] | None:
    fs_expr: list[str] = []
    ss_expr: list[str] = []
    cws_expr: list[str] = []
    final_cols: list[str] = []
    for i in range(1, 54):
        wk = f"{i:02d}"
        fs_expr.append("SUM(COALESCE(f.ForecastWk" + wk + ",0)) AS ForecastWk" + wk)
        ss_expr.append("SUM(COALESCE(s.DemandWk" + wk + ",0)) AS DemandWk" + wk)
        ss_expr.append(
            "SUM(COALESCE(CASE WHEN s.CloseStockWk"
            + wk
            + ">0 THEN s.CloseStockWk"
            + wk
            + " ELSE 0 END,0)) AS StStockWk"
            + wk
        )
        cws_expr.append("SUM(COALESCE(w.StockInWk" + wk + ",0)) AS QuantityWk" + wk)
        cws_expr.append("SUM(COALESCE(w.CloseStockWk" + wk + ",0)) AS WhStockWk" + wk)
        final_cols.append("fs.ForecastWk" + wk + " AS ForecastWk" + wk)
        final_cols.append("ss.DemandWk" + wk + " AS DemandWk" + wk)
        final_cols.append("cws.QuantityWk" + wk + " AS QuantityWk" + wk)
        final_cols.append("cws.WhStockWk" + wk + " AS WhStockWk" + wk)
        final_cols.append("ss.StStockWk" + wk + " AS StStockWk" + wk)

    sql = (
        "WITH x AS ("
        " SELECT TOP 1 iw.ItemKey, iw.CentralWarehouseKey"
        " FROM fpo.tbl_ItemWarehouse iw"
        " JOIN bicache.tbl_Item i ON i.ItemKey = iw.ItemKey"
        " JOIN bicache.tbl_CentralWarehouse cw ON cw.CentralWarehouseKey = iw.CentralWarehouseKey"
        " WHERE i.ItemNumber = '"
        + item_number
        + "' AND cw.CentralWarehouseCode = '"
        + warehouse_code
        + "'"
        "), fs AS ("
        " SELECT "
        + ", ".join(fs_expr)
        + " FROM fpo.tbl_ForecastStoreSales f"
        " JOIN x ON x.ItemKey = f.ItemKey AND x.CentralWarehouseKey = f.CentralWarehouseKey"
        "), ss AS ("
        " SELECT "
        + ", ".join(ss_expr)
        + " FROM fpo.tbl_CalcStoreStock s"
        " JOIN x ON x.ItemKey = s.ItemKey AND x.CentralWarehouseKey = s.CentralWarehouseKey"
        "), cws AS ("
        " SELECT "
        + ", ".join(cws_expr)
        + " FROM fpo.tbl_CalcWarehouseStock w"
        " JOIN x ON x.ItemKey = w.ItemKey AND x.CentralWarehouseKey = w.CentralWarehouseKey"
        ") SELECT "
        + ", ".join(final_cols)
        + " FROM fs CROSS JOIN ss CROSS JOIN cws"
    )
    try:
        rows = run_planning_tools_readonly_query(sql).get("results", [])
    except Exception:
        return None
    if not rows:
        return None
    row = rows[0]
    out: dict[str, list[float]] = {
        "forecast": [],
        "demand": [],
        "quantity": [],
        "whstock": [],
        "ststock": [],
    }
    for i in range(1, 54):
        wk = f"{i:02d}"
        out["forecast"].append(float(row.get(f"ForecastWk{wk}") or 0))
        out["demand"].append(float(row.get(f"DemandWk{wk}") or 0))
        out["quantity"].append(float(row.get(f"QuantityWk{wk}") or 0))
        out["whstock"].append(float(row.get(f"WhStockWk{wk}") or 0))
        out["ststock"].append(float(row.get(f"StStockWk{wk}") or 0))
    return out


def _patch_projection_columns_from_db(obj: dict[str, Any] | None, prompt: str) -> dict[str, Any] | None:
    if not isinstance(obj, dict):
        return obj
    item_number = _extract_item_number(prompt)
    if not item_number:
        return obj

    views = obj.get("warehouse_views")
    if not isinstance(views, list) or not views:
        return obj
    prompt_wh = _extract_warehouse_code(prompt)

    for idx, view in enumerate(views):
        if not isinstance(view, dict):
            continue
        warehouse_code = str(view.get("warehouse_code") or "").strip().upper()
        if idx == 0 and not warehouse_code and prompt_wh:
            warehouse_code = prompt_wh
            view["warehouse_code"] = warehouse_code
        if not warehouse_code:
            continue

        weekly = view.get("weekly_projection")
        if not isinstance(weekly, list) or len(weekly) != 53:
            continue
        if any(not isinstance(row, dict) for row in weekly):
            continue

        by_week = _get_projection_columns_by_week(
            item_number=item_number,
            warehouse_code=warehouse_code,
        )
        if not by_week:
            continue

        for i, row in enumerate(weekly):
            row["forecast"] = round(float(by_week["forecast"][i]), 2)
            row["demand"] = round(float(by_week["demand"][i]), 2)
            row["quantity"] = round(float(by_week["quantity"][i]), 2)
            row["whstock"] = round(float(by_week["whstock"][i]), 2)
            row["ststock"] = round(float(by_week["ststock"][i]), 2)
            if not isinstance(row.get("downtime"), str):
                row["downtime"] = str(row.get("downtime", ""))

    return obj


def _build_core_ordering_grounded_prompt(prompt: str) -> str:
    """
    Enrich prompt with deterministic read-only DB facts to reduce hallucinations.
    """
    user_prompt = (prompt or "").strip()
    item_number = _extract_item_number(user_prompt)
    if not item_number:
        return user_prompt

    try:
        inputs = get_core_ordering_inputs(
            target_weeks_cover=4,
            use_cover_config=True,
            item_number=item_number,
            forecast_horizon_weeks=53,
            top_n=50,
        )
    except Exception:
        # If deterministic prefetch fails, fall back to original prompt.
        return user_prompt

    rows = inputs.get("results", [])
    if not rows:
        return (
            f"{user_prompt}\n\n"
            "Grounding check (read-only PlanningToolsDB): no rows were returned from "
            "fpo.tbl_ItemWarehouse for this item_number. If data is empty, return a blocking "
            "data-setup response and do not fabricate warehouse mappings."
        )

    warehouses = sorted(
        {
            str(r.get("central_warehouse_code")).strip()
            for r in rows
            if r.get("central_warehouse_code")
        }
    )
    warehouse_list = ", ".join(warehouses) if warehouses else "(none)"

    return (
        f"{user_prompt}\n\n"
        "Grounding facts (read-only PlanningToolsDB, fetched just now):\n"
        f"- item_number: {item_number}\n"
        f"- mapping rows in fpo.tbl_ItemWarehouse: {len(rows)}\n"
        f"- mapped warehouses: {warehouse_list}\n"
        "- Do not claim there are no warehouse mappings when mappings are present.\n"
        "- Build your answer from these fetched facts and tool data.\n\n"
        "OUTPUT CONTRACT (STRICT):\n"
        "- Return raw JSON only (no markdown, no prose).\n"
        "- Include key `warehouse_views` as an array.\n"
        "- For single-warehouse requests, return one warehouse entry.\n"
        "- Each warehouse entry must include `warehouse_code` and `weekly_projection`.\n"
        "- `weekly_projection` must contain exactly 53 objects (week_index 1..53).\n"
        "- Each row must include numeric fields: forecast, demand, quantity, order, whstock, ststock.\n"
        "- If a numeric value is unavailable, return 0 (not null).\n"
        "- Include `downtime` as string (empty string allowed).\n"
        "- Never return narrative fallback when data exists in tools.\n\n"
        "RECOMMENDATION BEHAVIOR REQUIREMENTS:\n"
        "- Use trigger+constraint logic from agent instructions to derive `order`.\n"
        "- Do NOT output all-zero `order` across 53 weeks when trigger conditions are met.\n"
        "- If projected whstock is at/below safety with positive forecast/demand and valid constraints, emit non-zero order weeks.\n"
        "- Apply MOQ/order-multiple rounding when deriving non-zero orders.\n"
        "- Keep `quantity` aligned with recommendation logic (inbound effect of orders), not constant zero defaults.\n\n"
        "ORDER/QUANTITY LOGIC GUIDANCE (AGENT-DRIVEN):\n"
        "- Required inputs per week: Forecast, Demand, StStock, WhStock, MOQ.\n"
        "- Also use when available: SafetyStockQty, ReqPO, warehouse weeks-of-cover, lot sizes, leadtime feasibility/block reasons.\n"
        "- Trigger recommendation only when all true: WhStock <= SafetyStockQty, Forecast > 0, ReqPO=1, feasible delivery week, no active block.\n"
        "- If no trigger: Quantity must be 0 and order should remain 0 for that week.\n"
        "- If trigger: build cumulative demand across cover horizon plus safety stock gap; add one store-carton buffer; round up to correct lot size; then apply MOQ gate.\n"
        "- Do not reuse fixed repeated quantity for every order wave; recalculate next recommendations from updated weekly state after prior orders."
    )


def _try_parse_json_response(text: str) -> dict[str, Any] | None:
    if not text or not isinstance(text, str):
        return None
    # Direct JSON response
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # Fenced JSON block
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if fenced:
        try:
            obj = json.loads(fenced.group(1).strip())
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    # Largest JSON object fallback
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


def _has_structured_rows(obj: dict[str, Any] | None) -> bool:
    if not obj:
        return False
    views = obj.get("warehouse_views")
    if not isinstance(views, list) or not views:
        return False
    first = views[0]
    if not isinstance(first, dict):
        return False
    warehouse_code = first.get("warehouse_code")
    if not isinstance(warehouse_code, str) or not warehouse_code.strip():
        return False

    weekly = first.get("weekly_projection")
    if not isinstance(weekly, list) or len(weekly) != 53:
        return False

    required_numeric = ["forecast", "demand", "quantity", "order", "whstock", "ststock"]
    for i, row in enumerate(weekly, start=1):
        if not isinstance(row, dict):
            return False
        wk = row.get("week_index")
        if wk != i:
            return False
        for key in required_numeric:
            val = row.get(key)
            if not isinstance(val, (int, float)):
                return False
        dt = row.get("downtime")
        if not isinstance(dt, str):
            return False
    return True


async def _close_agent_if_possible(agent: Any) -> None:
    """
    Best-effort cleanup to avoid leaked aiohttp sessions/connectors.
    """
    if agent is None:
        return
    chat_client = getattr(agent, "chat_client", None)
    close_fn = getattr(chat_client, "close", None)
    if callable(close_fn):
        try:
            await close_fn()
        except Exception:
            # Swallow cleanup errors; request result is already decided.
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


@app.post("/api/query")
async def query(req: QueryRequest) -> Dict[str, Any]:
    prompt = req.prompt.strip()
    agent: Any = None
    try:
        agent = await get_core_ordering_agent()
        first_prompt = _build_core_ordering_grounded_prompt(prompt)
        result = await agent.run(first_prompt)
        text = result.text if result else "No response from agent"

        parsed = _patch_projection_columns_from_db(_try_parse_json_response(text), prompt)
        if not _has_structured_rows(parsed):
            retry_prompt = (
                first_prompt
                + "\n\nFINAL RETRY CONTRACT:\n"
                + "- Return ONLY raw JSON object.\n"
                + "- Include warehouse_views[0].weekly_projection with 53 rows.\n"
                + "- No markdown, no prose, no explanations.\n"
                + "- Use numeric 0 for unavailable numeric fields.\n"
                + "- Derive non-zero order weeks when trigger conditions are met; do not leave order all-zero by default.\n"
            )
            retry_result = await agent.run(retry_prompt)
            retry_text = retry_result.text if retry_result else text
            retry_parsed = _patch_projection_columns_from_db(_try_parse_json_response(retry_text), prompt)
            if _has_structured_rows(retry_parsed):
                text = json.dumps(retry_parsed, ensure_ascii=False)
            else:
                text = retry_text
        else:
            text = json.dumps(parsed, ensure_ascii=False)

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
    finally:
        await _close_agent_if_possible(agent)


@app.post("/api/chat")
async def chat(req: ChatRequest) -> Dict[str, Any]:
    agent: Any = None
    try:
        agent = await get_core_ordering_agent()
        first_prompt = _build_core_ordering_grounded_prompt(req.message.strip())
        result = await agent.run(first_prompt)
        text = result.text if result else "No response from agent"

        chat_prompt = req.message.strip()
        parsed = _patch_projection_columns_from_db(_try_parse_json_response(text), chat_prompt)
        if not _has_structured_rows(parsed):
            retry_prompt = (
                first_prompt
                + "\n\nFINAL RETRY CONTRACT:\n"
                + "- Return ONLY raw JSON object.\n"
                + "- Include warehouse_views[0].weekly_projection with 53 rows.\n"
                + "- No markdown, no prose, no explanations.\n"
                + "- Use numeric 0 for unavailable numeric fields.\n"
                + "- Derive non-zero order weeks when trigger conditions are met; do not leave order all-zero by default.\n"
            )
            retry_result = await agent.run(retry_prompt)
            retry_text = retry_result.text if retry_result else text
            retry_parsed = _patch_projection_columns_from_db(_try_parse_json_response(retry_text), chat_prompt)
            if _has_structured_rows(retry_parsed):
                text = json.dumps(retry_parsed, ensure_ascii=False)
            else:
                text = retry_text
        else:
            text = json.dumps(parsed, ensure_ascii=False)

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
    finally:
        await _close_agent_if_possible(agent)


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
