"""Generic reorder script. Usage: python run_item.py <item_number> [warehouse_code] [output_name]"""
import csv, json, re, sys, os, requests
from datetime import datetime
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
except ImportError:
    pass

from agents.tools.core_ordering_tools import get_item_ordering_data

if len(sys.argv) < 2:
    print("Usage: python run_item.py <item_number> [warehouse_code] [output_name]")
    raise SystemExit(1)

ITEM     = sys.argv[1]
WH       = sys.argv[2] if len(sys.argv) > 2 else "DK01WH"
OUT_NAME = sys.argv[3] if len(sys.argv) > 3 else ITEM
ROOT     = Path(ROOT_DIR)

# ── 1. Pre-fetch tool data ────────────────────────────────────────────────────
print(f"Fetching raw tool data: item={ITEM} warehouse={WH} ...")
tool_result = get_item_ordering_data(item_number=ITEM, central_warehouse_code=WH)

if tool_result.get("status") != "ok":
    print(f"ERROR: {tool_result.get('message')}")
    raise SystemExit(1)

counts = tool_result.get("counts", {})
(ROOT / f"{OUT_NAME}_tool_data.json").write_text(
    json.dumps(tool_result, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(f"  item_key          : {tool_result.get('item_key')}")
print(f"  current_calc_week : {tool_result.get('current_calc_week_no')}")
print(f"\nTables exposed to agent:")
for name, n in counts.items():
    print(f"  {name:<45} {n:>5} row(s)")

if counts.get("fpo_tbl_CalcWarehouseStock", 0) == 0:
    print("\nCANNOT RUN: fpo_tbl_CalcWarehouseStock is empty")
    raise SystemExit(1)

total_forecast = sum(x.get("forecast", 0) or 0 for x in tool_result.get("forecast_by_week", []))
total_demand   = sum(x.get("demand",   0) or 0 for x in tool_result.get("demand_by_week",   []))
print(f"\nPre-aggregated series:")
print(f"  total_forecast : {total_forecast:,.2f}")
print(f"  total_demand   : {total_demand:,.0f}")
for label, key in [("demand","demand_by_week"), ("forecast","forecast_by_week"), ("ststock","ststock_by_week")]:
    arr = tool_result.get(key, [])
    sample = [f"wk{r['week_index']}={r[label]}" for r in arr[:5]]
    print(f"  {key:<25} {', '.join(sample)}")

if total_forecast == 0:
    print("\nCANNOT RUN: forecast is all zeros — item has no demand data")
    raise SystemExit(1)

# ── 2. Read order constraints ─────────────────────────────────────────────────
tables    = tool_result.get("tables", {})
oqty_rows = tables.get("fpo_tbl_ItemWarehouseOrderQty", [])
item_rows  = tables.get("bicache_tbl_Item", [])
oqty = oqty_rows[0] if oqty_rows else {}
item = item_rows[0]  if item_rows  else {}
order_qty_type = oqty.get("OrderQtyType") or "C"
AOQ = oqty.get("AOQ") or 0
EOQ = oqty.get("EOQ") or 0
LOQ = oqty.get("LOQ") or 0
SOQ = oqty.get("SOQ") or 0
MOQ = item.get("MOQ") or 0
print(f"\nOrder constraints: OrderQtyType={order_qty_type}, AOQ={AOQ}, EOQ={EOQ}, LOQ={LOQ}, SOQ={SOQ}, MOQ={MOQ}")

if order_qty_type in ("C", None, ""):
    qty_hint = (
        f"Since OrderQtyType='C', use cover-based formula: "
        f"qty = ceil((demand_sum + StoreCarton) / pack) * pack. "
        f"MOQ={MOQ} is cross-warehouse in FPO — do NOT skip orders below MOQ for single-warehouse run. "
        f"Only skip if qty rounds to 0. "
        f"Do NOT add safety-stock deficit to quantity when SafetyStockQty=0."
    )
else:
    qty_hint = (
        f"Since OrderQtyType='{order_qty_type}', use fixed qty "
        f"(AOQ={AOQ}, EOQ={EOQ}, LOQ={LOQ}, SOQ={SOQ}). "
        f"MOQ is cross-warehouse — do not skip single-warehouse orders below MOQ."
    )

# ── 3. Call the agent ─────────────────────────────────────────────────────────
print(f"\nRequesting agent reorder: item={ITEM} warehouse={WH} ...")
r = requests.post(
    "http://localhost:18010/api/query",
    json={"prompt": (
        f"Call get_item_ordering_data(item_number='{ITEM}', central_warehouse_code='{WH}'). "
        "Compute core reordering from scratch using CloseStockWk00 as opening stock. "
        "Follow all trigger and quantity rules in your instructions exactly. "
        f"ORDER QTY TYPE: OrderQtyType='{order_qty_type}'. {qty_hint} "
        "After placing each order update WhStock and CONTINUE scanning every subsequent week until week 53. "
        "Do NOT stop after the first recommendation — find ALL triggers across the full 53-week horizon. "
        "Apply block window (WeeksOfCover weeks after each placed order) then resume scanning. "
        "Return raw JSON with ALL recommendations and warehouse_views for all 53 weeks."
    )},
    timeout=360,
)
r.raise_for_status()
data = r.json()
if data.get("type") == "error":
    print("ERROR:", data.get("error"))
    raise SystemExit(1)

text   = data.get("response", "{}")
parsed = None
try:
    parsed = json.loads(text)
except json.JSONDecodeError:
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try: parsed = json.loads(m.group(0))
        except json.JSONDecodeError: pass
if not parsed:
    parsed = {"status": "error", "raw": text}

# ── 4. Save JSON ──────────────────────────────────────────────────────────────
json_out = ROOT / f"{OUT_NAME}.json"
json_out.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Saved JSON  : {json_out}")

# ── 5. Build CSV ──────────────────────────────────────────────────────────────
fc_map  = {x["week_index"]: x["forecast"] for x in tool_result.get("forecast_by_week", [])}
dem_map = {x["week_index"]: x["demand"]   for x in tool_result.get("demand_by_week",   [])}
st_map  = {x["week_index"]: x["ststock"]  for x in tool_result.get("ststock_by_week",  [])}

ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_out = ROOT / f"{OUT_NAME}_{ts}.csv"
headers = ["warehouse_code", "week_index", "year_week", "forecast", "demand",
           "new_order", "whstock", "ststock"]
rows = []
for view in parsed.get("warehouse_views") or []:
    wh_code = view.get("warehouse_code", "")
    for p in view.get("weekly_projection") or []:
        wi = p.get("week_index")
        rows.append([
            wh_code, wi, p.get("year_week"),
            fc_map.get(wi, p.get("forecast")),
            dem_map.get(wi, p.get("demand")),
            p.get("new_order") or p.get("order") or p.get("quantity") or 0,
            p.get("whstock"),
            st_map.get(wi, p.get("ststock")),
        ])

with open(csv_out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(headers)
    w.writerows(rows)

print(f"Saved CSV   : {csv_out}  ({len(rows)} rows)")
print(f"Status      : {parsed.get('status')}")
print(f"\nRecommendations ({len(parsed.get('recommendations', []))}):")
for rec in parsed.get("recommendations") or []:
    print(f"  warehouse={rec.get('warehouse_code')}  week={rec.get('rec_order_week')}  "
          f"qty={rec.get('order_qty')}  cover={rec.get('weeks_cover')}wk  "
          f"delivery={rec.get('delivery_date')}")

print(f"\n--- Explanation ---")
explanation = parsed.get("explanation", "")[:1500]
print(explanation.encode("ascii", errors="replace").decode("ascii"))
