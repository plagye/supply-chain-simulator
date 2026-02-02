from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

def load_part_ids(parts_path: Path) -> list[str]:
    parts = json.loads(parts_path.read_text(encoding="utf-8"))
    if not isinstance(parts, list):
        raise ValueError(f"Expected parts JSON array in {parts_path}")
    part_ids: list[str] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        pid = p.get("part_id")
        if isinstance(pid, str) and pid:
            part_ids.append(pid)
    if not part_ids:
        raise ValueError(f"No part_ids found in {parts_path}")
    return part_ids


def inventory_levels_for_part(qty_on_hand: int) -> tuple[int, int]:
    # Keep it simple and consistent:
    # - safety_stock = 10% of starting inventory (min 20)
    # - reorder_point = 25% of starting inventory (at least safety_stock + 10)
    safety_stock = max(20, int(round(qty_on_hand * 0.10)))
    reorder_point = max(safety_stock + 10, int(round(qty_on_hand * 0.25)))
    return reorder_point, safety_stock


def generate_inventory(
    *,
    parts_path: Path,
    seed: int | None = 42,
    min_qty: int = 100,
    max_qty: int = 1000,
    finished_product_id: str = "DRONE-X1",
    finished_product_qty: int = 50,
) -> dict[str, dict[str, Any]]:
    if min_qty < 0 or max_qty < min_qty:
        raise ValueError("Invalid min/max qty range")

    rng = random.Random(seed)
    part_ids = load_part_ids(parts_path)

    inventory: dict[str, dict[str, Any]] = {}

    for pid in part_ids:
        qty_on_hand = rng.randint(min_qty, max_qty)
        reorder_point, safety_stock = inventory_levels_for_part(qty_on_hand)
        inventory[pid] = {
            "qty_on_hand": qty_on_hand,
            "reorder_point": reorder_point,
            "safety_stock": safety_stock,
        }

    # Finished goods inventory entry.
    fp_reorder_point = 15
    fp_safety_stock = 5
    inventory[finished_product_id] = {
        "qty_on_hand": finished_product_qty,
        "reorder_point": fp_reorder_point,
        "safety_stock": fp_safety_stock,
    }

    return inventory


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate inventory.json (initial simulation state).")
    parser.add_argument(
        "--parts",
        type=Path,
        default=DATA_DIR / "parts.json",
        help="Path to parts JSON (default: parts.json).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "inventory.json",
        help="Output JSON path (default: inventory.json).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Optional RNG seed (default: 42).")
    parser.add_argument("--min-qty", type=int, default=100, help="Minimum starting qty per part (default: 100).")
    parser.add_argument("--max-qty", type=int, default=1000, help="Maximum starting qty per part (default: 1000).")
    parser.add_argument(
        "--finished-product-id",
        type=str,
        default="DRONE-X1",
        help="Finished product id to include (default: DRONE-X1).",
    )
    parser.add_argument(
        "--finished-product-qty",
        type=int,
        default=50,
        help="Starting on-hand for finished product (default: 50).",
    )

    args = parser.parse_args()

    inventory = generate_inventory(
        parts_path=args.parts,
        seed=args.seed,
        min_qty=args.min_qty,
        max_qty=args.max_qty,
        finished_product_id=args.finished_product_id,
        finished_product_qty=args.finished_product_qty,
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(inventory, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote inventory for {len(inventory)} items to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

