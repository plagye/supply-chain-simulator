"""Generate suppliers.json with the fixed 4 suppliers (SUP-001..SUP-004) for the 10-drone scenario."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Fixed 4 suppliers per plan. country used for inbound route lookup (destination_country).
SUPPLIERS_CATALOG: list[dict] = [
    {"id": "SUP-001", "name": "VoltStream Tech", "country": "China", "reliability_score": 0.82, "risk_factor": "High", "price_multiplier": 0.92},
    {"id": "SUP-002", "name": "CarbonFiber Works", "country": "Germany", "reliability_score": 0.90, "risk_factor": "Medium", "price_multiplier": 1.02},
    {"id": "SUP-003", "name": "LogicCore Systems", "country": "USA", "reliability_score": 0.96, "risk_factor": "Low", "price_multiplier": 1.08},
    {"id": "SUP-004", "name": "OmniMount Solutions", "country": "USA", "reliability_score": 0.98, "risk_factor": "Minimal", "price_multiplier": 1.05},
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate suppliers.json (4 fixed suppliers).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "suppliers.json", help="Output path.")
    parser.add_argument("--seed", type=int, default=None, help="Ignored; kept for CLI compatibility.")
    parser.add_argument("--count", type=int, default=4, help="Ignored; always 4.")
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(SUPPLIERS_CATALOG, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(SUPPLIERS_CATALOG)} suppliers to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
