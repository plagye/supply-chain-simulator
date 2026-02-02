from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


PartTemplate = tuple[str, str, str, tuple[float, float]]

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

PART_TEMPLATES: list[PartTemplate] = [
    # Electronics (pcs)
    ("Li-Ion Battery Cell (21700)", "Electronics", "pcs", (2.20, 6.50)),
    ("Battery Management IC", "Electronics", "pcs", (0.90, 4.50)),
    ("Microcontroller (32-bit)", "Electronics", "pcs", (1.80, 9.50)),
    ("Power MOSFET (60V)", "Electronics", "pcs", (0.40, 2.20)),
    ("DC-DC Converter Module", "Electronics", "pcs", (1.50, 8.00)),
    ("Temperature Sensor (NTC)", "Electronics", "pcs", (0.05, 0.45)),
    ("Hall Effect Sensor", "Electronics", "pcs", (0.20, 1.10)),
    ("CAN Transceiver", "Electronics", "pcs", (0.40, 1.80)),
    ("Ethernet PHY", "Electronics", "pcs", (1.20, 4.80)),
    ("Wi‑Fi Module", "Electronics", "pcs", (2.00, 9.00)),
    ("GNSS Receiver Module", "Electronics", "pcs", (3.50, 16.00)),
    ("OLED Status Display (0.96\")", "Electronics", "pcs", (1.00, 6.00)),
    ("Tactile Switch (SMD)", "Electronics", "pcs", (0.02, 0.15)),
    ("LED Indicator (Green)", "Electronics", "pcs", (0.01, 0.08)),
    ("Connector (XT60)", "Electronics", "pcs", (0.35, 1.40)),
    ("PCB (4-layer, 100x100)", "Electronics", "pcs", (1.50, 8.50)),
    ("ESD Protection Diode Array", "Electronics", "pcs", (0.05, 0.40)),
    ("Ceramic Capacitor 10uF", "Electronics", "pcs", (0.01, 0.12)),
    ("Shunt Resistor 1mΩ", "Electronics", "pcs", (0.10, 0.70)),
    ("Quartz Crystal 16MHz", "Electronics", "pcs", (0.08, 0.40)),
    # Raw Material (kg / m)
    ("Carbon Fiber Sheet (2mm)", "Raw Material", "m", (18.00, 65.00)),
    ("Aluminum Extrusion 6061-T6", "Raw Material", "m", (4.00, 18.00)),
    ("Copper Wire (AWG 14)", "Raw Material", "m", (0.35, 1.50)),
    ("Stainless Steel Rod (304)", "Raw Material", "m", (3.00, 14.00)),
    ("ABS Plastic Pellets", "Raw Material", "kg", (1.20, 3.20)),
    ("Polycarbonate Pellets", "Raw Material", "kg", (1.80, 4.80)),
    ("Epoxy Resin", "Raw Material", "kg", (6.00, 18.00)),
    ("Silicone Sealant", "Raw Material", "kg", (4.00, 14.00)),
    ("Nitrile Rubber Sheet", "Raw Material", "m", (2.50, 10.00)),
    ("Steel Sheet (Cold Rolled)", "Raw Material", "kg", (0.90, 2.20)),
    ("Aluminum Sheet (5052)", "Raw Material", "kg", (2.20, 5.50)),
    ("Neodymium Magnet (Grade N52)", "Raw Material", "kg", (45.00, 110.00)),
    ("Lithium Carbonate", "Raw Material", "kg", (9.00, 35.00)),
    ("Graphite Powder", "Raw Material", "kg", (3.00, 12.00)),
    ("Fiberglass Cloth", "Raw Material", "m", (1.50, 6.50)),
    ("Heat Shrink Tubing", "Raw Material", "m", (0.05, 0.35)),
    ("Industrial Adhesive Film", "Raw Material", "m", (0.80, 4.50)),
    # Assembly (pcs)
    ("Gearbox Assembly", "Assembly", "pcs", (28.00, 140.00)),
    ("Linear Actuator Assembly", "Assembly", "pcs", (35.00, 180.00)),
    ("Cooling Fan Assembly (120mm)", "Assembly", "pcs", (2.50, 12.00)),
    ("Wiring Harness Assembly", "Assembly", "pcs", (6.00, 30.00)),
    ("Bearing Set (6204)", "Assembly", "pcs", (1.20, 6.00)),
    ("Fastener Kit (M3/M4)", "Assembly", "pcs", (0.80, 4.00)),
    ("O‑Ring Kit (NBR)", "Assembly", "pcs", (0.60, 3.50)),
    ("Injection Molded Housing", "Assembly", "pcs", (1.50, 9.50)),
    ("Stamped Bracket", "Assembly", "pcs", (0.40, 2.50)),
    ("Motor Mount Plate", "Assembly", "pcs", (1.00, 6.50)),
    ("Heat Sink (Extruded)", "Assembly", "pcs", (0.80, 5.50)),
    ("Control Panel Assembly", "Assembly", "pcs", (12.00, 65.00)),
    ("Sensor Module Assembly", "Assembly", "pcs", (4.00, 22.00)),
]


def load_supplier_ids(path: Path) -> list[str]:
    suppliers = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(suppliers, list):
        raise ValueError(f"Expected suppliers JSON array in {path}")
    ids = [s.get("id") for s in suppliers if isinstance(s, dict)]
    ids = [i for i in ids if isinstance(i, str) and i]
    if not ids:
        raise ValueError(f"No supplier ids found in {path}")
    return ids


def generate_parts(
    *,
    suppliers_path: Path,
    count: int = 50,
    sku_start: int = 100,
    seed: int | None = None,
) -> list[dict]:
    rng = random.Random(seed)
    supplier_ids = load_supplier_ids(suppliers_path)

    if count > len(PART_TEMPLATES):
        raise ValueError(f"Requested {count} parts, but only {len(PART_TEMPLATES)} templates are defined.")

    templates = PART_TEMPLATES[:count]
    parts: list[dict] = []

    for idx, (name, category, uom, (cmin, cmax)) in enumerate(templates):
        part_num = sku_start + idx
        part_id = f"SKU-{part_num}"

        k = rng.randint(2, min(6, len(supplier_ids)))
        valid_supplier_ids = sorted(rng.sample(supplier_ids, k=k))

        standard_cost = round(rng.uniform(cmin, cmax), 2)

        parts.append(
            {
                "part_id": part_id,
                "name": name,
                "category": category,
                "standard_cost": standard_cost,
                "unit_of_measure": uom,
                "valid_supplier_ids": valid_supplier_ids,
            }
        )

    return parts


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a parts.json file with supplier relationships.")
    parser.add_argument("--count", type=int, default=50, help="Number of parts to generate (default: 50).")
    parser.add_argument(
        "--suppliers",
        type=Path,
        default=DATA_DIR / "suppliers.json",
        help="Path to suppliers JSON (default: suppliers.json).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "parts.json",
        help="Output JSON path (default: parts.json).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Optional RNG seed for reproducible output (default: 42).",
    )
    parser.add_argument(
        "--sku-start",
        type=int,
        default=100,
        help="Starting SKU number (default: 100 -> SKU-100).",
    )

    args = parser.parse_args()

    parts = generate_parts(suppliers_path=args.suppliers, count=args.count, sku_start=args.sku_start, seed=args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(parts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(parts)} parts to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

