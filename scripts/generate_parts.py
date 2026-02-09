"""Generate parts.json with the fixed 10 shared components (P-001..P-010) for the 10-drone scenario."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Fixed catalog: 10 parts per plan. valid_supplier_ids match generate_suppliers output (SUP-001..SUP-004).
PARTS_CATALOG: list[dict] = [
    {"part_id": "P-001", "name": "Frame-Light (Composite)", "category": "Chassis", "standard_cost": 50.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-002"]},
    {"part_id": "P-002", "name": "Frame-Heavy (Alloy)", "category": "Chassis", "standard_cost": 120.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-002"]},
    {"part_id": "P-003", "name": "Motor-Std (800KV)", "category": "Propulsion", "standard_cost": 25.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-001"]},
    {"part_id": "P-004", "name": "Motor-Pro (1200KV)", "category": "Propulsion", "standard_cost": 45.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-001"]},
    {"part_id": "P-005", "name": "Propeller-Set (Generic)", "category": "Propulsion", "standard_cost": 10.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-002"]},
    {"part_id": "P-006", "name": "Battery-5Ah (LiPo)", "category": "Power", "standard_cost": 60.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-001"]},
    {"part_id": "P-007", "name": "Battery-10Ah (LiPo)", "category": "Power", "standard_cost": 110.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-001"]},
    {"part_id": "P-008", "name": "Flight-Controller-V1", "category": "Avionics", "standard_cost": 80.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-003"]},
    {"part_id": "P-009", "name": "Nav-Module-GPS", "category": "Avionics", "standard_cost": 40.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-003"]},
    {"part_id": "P-010", "name": "Payload-Interface", "category": "Accessories", "standard_cost": 30.0, "unit_of_measure": "pcs", "valid_supplier_ids": ["SUP-004"]},
]


def generate_parts() -> list[dict]:
    """Return the fixed 10 parts. No supplier file required; IDs match generate_suppliers output."""
    return list(PARTS_CATALOG)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate parts.json (10 fixed shared components P-001..P-010).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "parts.json", help="Output path.")
    parser.add_argument("--seed", type=int, default=None, help="Ignored; kept for CLI compatibility.")
    args = parser.parse_args()

    parts = generate_parts()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(parts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(parts)} parts to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
