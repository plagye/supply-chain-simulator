"""Generate bom.json with multi-product BOM (10 drones, 10 shared parts). Structure: { "products": { "D-101": { "bom": [ { "components": [...] } ] }, ... } }."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Per-product BOM: list of (component_id, qty). P-008 (Flight Controller) in all.
# Frame: P-001 Light / P-002 Heavy. Propulsion: P-003 Std, P-004 Pro; P-005 Propeller-Set per motor.
# Power: P-006 5Ah, P-007 10Ah. P-009 GPS where needed. P-010 Payload-Interface where needed.
PRODUCT_BOMS: dict[str, list[tuple[str, float]]] = {
    "D-101": [("P-001", 1), ("P-003", 4), ("P-005", 4), ("P-006", 1), ("P-008", 1)],  # Sparrow-S1 Quad
    "D-102": [("P-001", 1), ("P-004", 4), ("P-005", 4), ("P-006", 1), ("P-008", 1), ("P-009", 1)],  # Sparrow-Pro + GPS
    "D-103": [("P-001", 1), ("P-004", 4), ("P-005", 4), ("P-007", 1), ("P-008", 1)],  # Falcon-X 10Ah
    "D-201": [("P-002", 1), ("P-003", 4), ("P-005", 4), ("P-006", 1), ("P-008", 1), ("P-010", 1)],  # Courier-M Payload
    "D-202": [("P-002", 1), ("P-003", 6), ("P-005", 6), ("P-006", 2), ("P-008", 1)],  # Courier-L Hex 2x Batt
    "D-203": [("P-001", 1), ("P-003", 4), ("P-005", 4), ("P-006", 1), ("P-008", 1), ("P-009", 1), ("P-010", 1)],  # Surveyor-300
    "D-204": [("P-002", 1), ("P-004", 4), ("P-005", 4), ("P-006", 1), ("P-008", 1), ("P-009", 2)],  # Surveyor-500 2x GPS
    "D-301": [("P-002", 1), ("P-004", 8), ("P-005", 8), ("P-007", 2), ("P-008", 1)],  # Titan-Hauler Octo 2x Batt
    "D-302": [("P-002", 1), ("P-004", 6), ("P-005", 6), ("P-007", 2), ("P-008", 1)],  # Agri-Sprayer Hex 2x Batt
    "D-303": [("P-002", 1), ("P-004", 8), ("P-005", 8), ("P-007", 3), ("P-008", 1)],  # Sky-Crane Octo 3x Batt
}


def load_parts_by_id(parts_path: Path) -> dict[str, dict[str, Any]]:
    parts = json.loads(parts_path.read_text(encoding="utf-8"))
    if not isinstance(parts, list):
        raise ValueError(f"Expected parts JSON array in {parts_path}")
    by_id: dict[str, dict[str, Any]] = {}
    for p in parts:
        if not isinstance(p, dict):
            continue
        pid = p.get("part_id")
        if isinstance(pid, str) and pid:
            by_id[pid] = p
    if not by_id:
        raise ValueError(f"No parts found in {parts_path}")
    return by_id


def validate_component_ids(products_bom: dict[str, Any], parts_by_id: dict[str, dict[str, Any]]) -> None:
    missing: set[str] = set()
    for product_id, data in products_bom.items():
        if not isinstance(data, dict):
            continue
        for item in data.get("bom", []):
            if not isinstance(item, dict):
                continue
            for comp in item.get("components", []):
                if isinstance(comp, dict):
                    cid = comp.get("component_id")
                    if isinstance(cid, str) and cid and cid not in parts_by_id:
                        missing.add(cid)
    if missing:
        raise ValueError(f"BOM references unknown part_ids: {sorted(missing)}")


def build_multi_product_bom() -> dict[str, Any]:
    """Build { "products": { "D-101": { "bom": [ { "components": [ {"component_id", "qty"}, ... ] } ] }, ... } }."""
    products: dict[str, Any] = {}
    for product_id, comp_list in PRODUCT_BOMS.items():
        components = [{"component_id": cid, "qty": qty} for cid, qty in comp_list]
        products[product_id] = {"bom": [{"components": components}]}
    return {"products": products}


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate bom.json (10 products, 10 shared parts).")
    parser.add_argument(
        "--parts",
        type=Path,
        default=DATA_DIR / "parts.json",
        help="Path to parts JSON (default: parts.json).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "bom.json",
        help="Output JSON path (default: bom.json).",
    )
    args = parser.parse_args()

    parts_by_id = load_parts_by_id(args.parts)
    bom = build_multi_product_bom()
    validate_component_ids(bom["products"], parts_by_id)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(bom, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote BOM ({len(bom['products'])} products) to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
