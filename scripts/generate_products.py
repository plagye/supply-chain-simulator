"""Generate products.json: 10 drone models (D-101..D-303) for the 10-drone scenario."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

PRODUCTS_CATALOG: list[dict] = [
    {"product_id": "D-101", "name": "Sparrow-S1", "type": "Scout", "key_features": "Light Frame, Std Motor"},
    {"product_id": "D-102", "name": "Sparrow-Pro", "type": "Scout", "key_features": "Light Frame, Pro Motor, GPS"},
    {"product_id": "D-103", "name": "Falcon-X", "type": "Racing", "key_features": "Light Frame, Pro Motor, 10Ah Batt"},
    {"product_id": "D-201", "name": "Courier-M", "type": "Delivery", "key_features": "Heavy Frame, Std Motor, Payload Mnt"},
    {"product_id": "D-202", "name": "Courier-L", "type": "Delivery", "key_features": "Heavy Frame, 6x Std Motor, 2x Batt"},
    {"product_id": "D-203", "name": "Surveyor-300", "type": "Inspection", "key_features": "Light Frame, GPS, Payload Mnt"},
    {"product_id": "D-204", "name": "Surveyor-500", "type": "Inspection", "key_features": "Heavy Frame, Pro Motor, 2x GPS"},
    {"product_id": "D-301", "name": "Titan-Hauler", "type": "Heavy Lift", "key_features": "Heavy Frame, 8x Pro Motor, 2x Batt"},
    {"product_id": "D-302", "name": "Agri-Sprayer", "type": "Agriculture", "key_features": "Heavy Frame, 6x Pro Motor, 2x Batt"},
    {"product_id": "D-303", "name": "Sky-Crane", "type": "Construction", "key_features": "Heavy Frame, 8x Pro Motor, 3x Batt"},
]


def generate_products() -> list[dict]:
    """Return the 10 drone products. Single source of truth for product_id, name, type, key_features."""
    return list(PRODUCTS_CATALOG)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate products.json (10 drone models D-101..D-303).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "products.json", help="Output path.")
    parser.add_argument("--seed", type=int, default=None, help="Ignored; kept for CLI compatibility.")
    args = parser.parse_args()
    products = generate_products()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(products, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(products)} products to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
