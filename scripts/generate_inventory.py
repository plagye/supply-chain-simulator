"""Generate inventory.json: initial stock for all P-001..P-010 parts and D-101..D-303 products."""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

PRODUCT_IDS = ["D-101", "D-102", "D-103", "D-201", "D-202", "D-203", "D-204", "D-301", "D-302", "D-303"]


def load_part_ids(parts_path: Path) -> list[str]:
    parts = json.loads(parts_path.read_text(encoding="utf-8"))
    if not isinstance(parts, list):
        raise ValueError(f"Expected parts JSON array in {parts_path}")
    part_ids = [p.get("part_id") for p in parts if isinstance(p, dict) and p.get("part_id")]
    if not part_ids:
        raise ValueError(f"No part_ids found in {parts_path}")
    return part_ids


def load_product_ids(products_path: Path | None) -> list[str]:
    if products_path is None or not products_path.exists():
        return PRODUCT_IDS
    data = json.loads(products_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return PRODUCT_IDS
    ids = [p.get("product_id") for p in data if isinstance(p, dict) and p.get("product_id")]
    return ids if ids else PRODUCT_IDS


def inventory_levels_for_part(qty_on_hand: int) -> tuple[int, int]:
    safety_stock = max(20, int(round(qty_on_hand * 0.10)))
    reorder_point = max(safety_stock + 10, int(round(qty_on_hand * 0.25)))
    return reorder_point, safety_stock


def generate_inventory(
    *,
    parts_path: Path,
    products_path: Path | None = None,
    seed: int | None = 42,
    min_qty: int = 100,
    max_qty: int = 1000,
    finished_product_qty: int = 0,
) -> dict[str, dict[str, Any]]:
    rng = random.Random(seed)
    part_ids = load_part_ids(parts_path)
    product_ids = load_product_ids(products_path)

    inventory: dict[str, dict[str, Any]] = {}
    for pid in part_ids:
        qty_on_hand = rng.randint(min_qty, max_qty)
        reorder_point, safety_stock = inventory_levels_for_part(qty_on_hand)
        inventory[pid] = {"qty_on_hand": qty_on_hand, "reorder_point": reorder_point, "safety_stock": safety_stock}

    fp_reorder_point = 15
    fp_safety_stock = 5
    for product_id in product_ids:
        inventory[product_id] = {
            "qty_on_hand": finished_product_qty,
            "reorder_point": fp_reorder_point,
            "safety_stock": fp_safety_stock,
        }
    return inventory


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate inventory.json (10 parts + 10 products).")
    parser.add_argument("--parts", type=Path, default=DATA_DIR / "parts.json", help="Path to parts JSON.")
    parser.add_argument("--products", type=Path, default=DATA_DIR / "products.json", help="Path to products JSON (optional).")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "inventory.json", help="Output path.")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed.")
    parser.add_argument("--min-qty", type=int, default=100, help="Min starting qty per part.")
    parser.add_argument("--max-qty", type=int, default=1000, help="Max starting qty per part.")
    parser.add_argument("--finished-product-qty", type=int, default=0, help="Starting on-hand per finished product (default 0).")
    args = parser.parse_args()

    inventory = generate_inventory(
        parts_path=args.parts,
        products_path=args.products if args.products.exists() else None,
        seed=args.seed,
        min_qty=args.min_qty,
        max_qty=args.max_qty,
        finished_product_qty=args.finished_product_qty,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(inventory, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote inventory for {len(inventory)} items to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
