"""Generate facilities.json: FAC-001 (SkyForge HQ plant) + delivery facilities with location_code (CODE -> CODE)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Plant: FAC-001 per plan. Delivery facilities for outbound routes (customers reference destination_facility_id).
FACILITIES = [
    {
        "facility_id": "FAC-001",
        "facility_name": "SkyForge HQ",
        "city": "Chicago",
        "state": "IL",
        "country": "USA",
        "facility_type": "plant",
        "region": "NA",
        "location_code": "USA_CHI",
    },
    {"facility_id": "dist_na_01", "facility_name": "NA DC Detroit", "city": "Detroit", "state": "MI", "country": "USA", "facility_type": "distribution", "region": "NA", "location_code": "USA_DET"},
    {"facility_id": "dist_na_02", "facility_name": "NA DC Dallas", "city": "Dallas", "state": "TX", "country": "USA", "facility_type": "distribution", "region": "NA", "location_code": "USA_DAL"},
    {"facility_id": "dist_emea_01", "facility_name": "EMEA DC Rotterdam", "city": "Rotterdam", "state": "", "country": "Netherlands", "facility_type": "distribution", "region": "EMEA", "location_code": "NLD_RTM"},
    {"facility_id": "dist_apac_01", "facility_name": "APAC DC Singapore", "city": "Singapore", "state": "", "country": "Singapore", "facility_type": "distribution", "region": "APAC", "location_code": "SGP_SIN"},
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate facilities.json (FAC-001 plant + DCs with location_code).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "facilities.json", help="Output path (default: data/facilities.json)")
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(FACILITIES, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(FACILITIES)} facilities to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
