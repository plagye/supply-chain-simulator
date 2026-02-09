"""Generate facilities.json with plant and distribution/delivery locations and location_code (CODE format: COUNTRY_CITY)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

FACILITIES = [
    {
        "facility_id": "skyforge_plant",
        "facility_name": "SkyForge Dynamics Chicago Plant",
        "city": "Chicago",
        "state": "IL",
        "country": "USA",
        "facility_type": "plant",
        "region": "NA",
        "location_code": "USA_CHI",
    },
    {
        "facility_id": "dist_na_01",
        "facility_name": "North America Distribution Center 1",
        "city": "Detroit",
        "state": "MI",
        "country": "USA",
        "facility_type": "distribution",
        "region": "NA",
        "location_code": "USA_DET",
    },
    {
        "facility_id": "dist_na_02",
        "facility_name": "North America Distribution Center 2",
        "city": "Dallas",
        "state": "TX",
        "country": "USA",
        "facility_type": "distribution",
        "region": "NA",
        "location_code": "USA_DAL",
    },
    {
        "facility_id": "dist_emea_01",
        "facility_name": "EMEA Distribution Center",
        "city": "Rotterdam",
        "state": "",
        "country": "Netherlands",
        "facility_type": "distribution",
        "region": "EMEA",
        "location_code": "NLD_RTM",
    },
    {
        "facility_id": "dist_emea_02",
        "facility_name": "EMEA Delivery Hub",
        "city": "Frankfurt",
        "state": "",
        "country": "Germany",
        "facility_type": "delivery",
        "region": "EMEA",
        "location_code": "DEU_FRA",
    },
    {
        "facility_id": "dist_apac_01",
        "facility_name": "APAC Distribution Center",
        "city": "Singapore",
        "state": "",
        "country": "Singapore",
        "facility_type": "distribution",
        "region": "APAC",
        "location_code": "SGP_SIN",
    },
    {
        "facility_id": "dist_apac_02",
        "facility_name": "APAC Delivery Hub",
        "city": "Sydney",
        "state": "NSW",
        "country": "Australia",
        "facility_type": "delivery",
        "region": "APAC",
        "location_code": "AUS_SYD",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate facilities.json (plant + DCs with location_code).")
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "facilities.json",
        help="Output path (default: data/facilities.json)",
    )
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(FACILITIES, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(FACILITIES)} facilities to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
