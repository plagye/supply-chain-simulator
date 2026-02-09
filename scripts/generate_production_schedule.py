"""Generate production_schedule.json: empty or minimal active_jobs (product_id from D-101..D-303)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate production_schedule.json (empty active_jobs for 10-drone scenario).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "production_schedule.json", help="Output path.")
    parser.add_argument("--wip-jobs", type=int, default=0, help="Number of initial WIP jobs (default 0).")
    parser.add_argument("--seed", type=int, default=42, help="Ignored; kept for CLI compatibility.")
    args = parser.parse_args()
    schedule = {"active_jobs": []}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(schedule, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote production_schedule to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
