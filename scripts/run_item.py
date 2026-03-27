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

total_forecast = sum(x.get("forecast", 0) or 0 for x in tool_result.get("forecast_by_week",        []))
total_demand   = sum(x.get("demand",   0) or 0 for x in tool_result.get("demand_by_week",          []))
print(f"\nPre-aggregated series:")
print(f"  total_forecast        : {total_forecast:,.2f}")
print(f"  total_demand (DmndWk) : {total_demand:,.0f}")
for label, key in [
    ("demand",  "demand_by_week"),
    ("forecast","forecast_by_week"),
    ("ststock", "ststock_by_week"),
]:
    arr = tool_result.get(key, [])
    sample = [f"wk{r['week_index']}={r[label]}" for r in arr[:5]]
    print(f"  {key:<25} {', '.join(sample)}")

if total_forecast == 0:
    print("\nCANNOT RUN: forecast is all zeros — item has no demand data")
    raise SystemExit(1)

tables    = tool_result.get("tables", {})
oqty_rows = tables.get("fpo_tbl_ItemWarehouseOrderQty", [])
item_rows  = tables.get("bicache_tbl_Item", [])
oqty = oqty_rows[0] if oqty_rows else {}
item = item_rows[0]  if item_rows  else {}
print(f"\nOrder constraints: OrderQtyType={oqty.get('OrderQtyType')}, "
      f"AOQ={oqty.get('AOQ')}, EOQ={oqty.get('EOQ')}, LOQ={oqty.get('LOQ')}, "
      f"SOQ={oqty.get('SOQ')}, MOQ={item.get('MOQ')}")

# ── 2. Call the agent ─────────────────────────────────────────────────────────
print(f"\nRequesting agent reorder: item={ITEM} warehouse={WH} ...")
r = requests.post(
    "http://localhost:18010/api/query",
    json={"prompt": (
        f"Call get_item_ordering_data(item_number='{ITEM}', central_warehouse_code='{WH}') "
        "and run the full core reordering simulation."
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

# ── 3. Save raw agent JSON ────────────────────────────────────────────────────
json_out = ROOT / f"{OUT_NAME}.json"
json_out.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Saved JSON  : {json_out}")

# ── 4. Build CSV — post-process WhStock deterministically ─────────────────────
# The agent decides WHEN and HOW MUCH to order.
# Python recomputes warehouse stock using the documented parity path:
#   whstock[n] = whstock[n-1] + existing_inbound[n] + new_order[n] - store_stockin[n]
# Negative stock is valid and represents a deficit/backorder.
fc_map      = {x["week_index"]: x["forecast"] for x in tool_result.get("forecast_by_week", [])}
st_map      = {x["week_index"]: x["ststock"]  for x in tool_result.get("ststock_by_week",  [])}
stockin_map = {x["week_index"]: float(x.get("stockin") or 0) for x in tool_result.get("store_stockin_by_week", [])}

wh_tables = (tool_result.get("tables") or {}).get("fpo_tbl_CalcWarehouseStock") or []
wh_open   = float((wh_tables[0].get("CloseStockWk00") or 0) if wh_tables else 0)
existing_inbound_map = {
    wi: float((wh_tables[0].get(f"StockInWk{wi:02d}") or 0) if wh_tables else 0)
    for wi in range(1, 54)
}

ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_out = ROOT / f"{OUT_NAME}_{ts}.csv"
headers = ["warehouse_code","week_index","year_week","forecast","demand",
           "existing_inbound","new_order","whstock","ststock"]
rows = []
for view in parsed.get("warehouse_views") or []:
    wh_code  = view.get("warehouse_code", "")
    wh_stock = wh_open
    for p in view.get("weekly_projection") or []:
        wi = int(p.get("week_index") or 0)
        new_order = float(p.get("new_order") or p.get("order") or p.get("quantity") or 0)
        existing_inbound = float(p.get("existing_inbound") or existing_inbound_map.get(wi, 0))
        raw_demand = float(stockin_map.get(wi, 0))
        wh_stock = wh_stock + existing_inbound + new_order - raw_demand
        rows.append([
            wh_code, wi, p.get("year_week"),
            fc_map.get(wi, p.get("forecast")),
            round(raw_demand, 2),
            existing_inbound,
            new_order,
            round(wh_stock, 2),
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
