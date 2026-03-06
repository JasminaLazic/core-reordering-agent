import argparse
import json
from datetime import datetime
from pathlib import Path
from urllib import request


def run_query(api_base_url: str, prompt: str, store_id: str) -> dict:
    payload = {
        "prompt": prompt,
        "store_id": store_id,
        "start_date": None,
        "end_date": None,
    }
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=f"{api_base_url.rstrip('/')}/api/query",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=180) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Call core ordering agent through /api/query and save response."
    )
    parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8010",
        help="API base URL (default: http://127.0.0.1:8010)",
    )
    parser.add_argument(
        "--store-id",
        default="ALL",
        help="Store id passed to API payload (default: ALL)",
    )
    parser.add_argument(
        "--prompt",
        default=(
            "Give me CORE REORDERING for item number 3000393 for 53 calculation weeks, "
            "where calculation Week 1 = calendar Week 10 of 2026. "
            "Use 4-week cover and return a table per warehouse with columns: "
            "warehouse, calc_week, calendar_week, forecast, demand_quantity, "
            "order_quantity, whstock."
        ),
        help="Prompt sent to the core ordering agent.",
    )
    parser.add_argument(
        "--out",
        default=f"core_ordering_agent_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        help="Output JSON file path.",
    )
    args = parser.parse_args()

    response = run_query(args.api_base_url, args.prompt, args.store_id)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(response, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Saved: {out_path}")
    print(f"type={response.get('type')} agent={response.get('agent')}")


if __name__ == "__main__":
    main()

