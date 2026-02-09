"""Generate routes.json with inbound and outbound routes keyed by location_code (CODE -> CODE)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

PLANT_FACILITY_ID = "skyforge_plant"
PLANT_LOCATION_CODE = "USA_CHI"

OUTBOUND_ROUTES = [
    {"destination_facility_id": "dist_na_01", "destination_location_code": "USA_DET", "typical_distance_miles": 283, "typical_transit_days": 2, "base_rate_per_mile": 0.12},
    {"destination_facility_id": "dist_na_02", "destination_location_code": "USA_DAL", "typical_distance_miles": 967, "typical_transit_days": 3, "base_rate_per_mile": 0.12},
    {"destination_facility_id": "dist_emea_01", "destination_location_code": "NLD_RTM", "typical_distance_miles": 4100, "typical_transit_days": 12, "base_rate_per_mile": 0.18},
    {"destination_facility_id": "dist_emea_02", "destination_location_code": "DEU_FRA", "typical_distance_miles": 4400, "typical_transit_days": 14, "base_rate_per_mile": 0.18},
    {"destination_facility_id": "dist_apac_01", "destination_location_code": "SGP_SIN", "typical_distance_miles": 9300, "typical_transit_days": 21, "base_rate_per_mile": 0.15},
    {"destination_facility_id": "dist_apac_02", "destination_location_code": "AUS_SYD", "typical_distance_miles": 9400, "typical_transit_days": 22, "base_rate_per_mile": 0.15},
]

INBOUND_ROUTES = [
    {"destination_country": "China", "typical_distance_miles": 7200, "typical_transit_days": 28, "base_rate_per_mile": 0.15},
    {"destination_country": "Taiwan", "typical_distance_miles": 7600, "typical_transit_days": 26, "base_rate_per_mile": 0.15},
    {"destination_country": "Germany", "typical_distance_miles": 4400, "typical_transit_days": 14, "base_rate_per_mile": 0.18},
    {"destination_country": "USA", "typical_distance_miles": 800, "typical_transit_days": 3, "base_rate_per_mile": 0.12},
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate routes.json (CODE -> CODE).")
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "routes.json",
        help="Output path (default: data/routes.json)",
    )
    args = parser.parse_args()

    outbound = []
    for r in OUTBOUND_ROUTES:
        dest_code = r["destination_location_code"]
        route_id = f"out_{PLANT_LOCATION_CODE}_{dest_code}"
        outbound.append({
            "route_id": route_id,
            "origin_facility_id": PLANT_FACILITY_ID,
            "destination_facility_id": r["destination_facility_id"],
            "origin_location_code": PLANT_LOCATION_CODE,
            "destination_location_code": dest_code,
            "typical_distance_miles": r["typical_distance_miles"],
            "typical_transit_days": r["typical_transit_days"],
            "base_rate_per_mile": r["base_rate_per_mile"],
        })

    inbound = []
    for r in INBOUND_ROUTES:
        country = r["destination_country"]
        route_id = f"in_{PLANT_LOCATION_CODE}_{country.lower().replace(' ', '_')}"
        inbound.append({
            "route_id": route_id,
            "origin_facility_id": PLANT_FACILITY_ID,
            "origin_location_code": PLANT_LOCATION_CODE,
            "destination_country": country,
            "typical_distance_miles": r["typical_distance_miles"],
            "typical_transit_days": r["typical_transit_days"],
            "base_rate_per_mile": r["base_rate_per_mile"],
        })

    data = {"inbound": inbound, "outbound": outbound}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(outbound)} outbound and {len(inbound)} inbound routes to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
