from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

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


def validate_component_ids(bom: dict[str, Any], parts_by_id: dict[str, dict[str, Any]]) -> None:
    missing: set[str] = set()

    stack: list[Any] = [bom.get("bom", [])]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        if "component_id" in node:
            cid = node.get("component_id")
            if isinstance(cid, str) and cid and cid not in parts_by_id:
                missing.add(cid)
        if "components" in node:
            stack.append(node["components"])

    if missing:
        raise ValueError(f"BOM references unknown part_ids: {sorted(missing)}")


def build_drone_x1_bom() -> dict[str, Any]:
    # Realistic hierarchical BOM for a rugged quadcopter used in industrial inspection.
    # Leaf nodes are always parts.json SKUs.
    return {
        "product_id": "DRONE-X1",
        "bom": [
            {
                "sub_assembly": "Airframe & Structure",
                "components": [
                    {"component_id": "SKU-120", "qty": 3.0},  # Carbon Fiber Sheet (m)
                    {"component_id": "SKU-121", "qty": 4.0},  # Aluminum Extrusion (m)
                    {"component_id": "SKU-130", "qty": 2.0},  # Aluminum Sheet (kg)
                    {"component_id": "SKU-145", "qty": 8},  # Stamped Bracket
                    {"component_id": "SKU-146", "qty": 4},  # Motor Mount Plate
                    {"component_id": "SKU-142", "qty": 2},  # Fastener Kit
                    {"component_id": "SKU-136", "qty": 5.0},  # Industrial Adhesive Film (m)
                    {"component_id": "SKU-126", "qty": 1.2},  # Epoxy Resin (kg)
                    {"component_id": "SKU-128", "qty": 1.0},  # Nitrile Rubber Sheet (m)
                    {"component_id": "SKU-143", "qty": 1},  # O‑Ring Kit
                    {"component_id": "SKU-144", "qty": 1},  # Injection Molded Housing
                ],
            },
            {
                "sub_assembly": "Power System (6S2P Battery Pack)",
                "components": [
                    {"component_id": "SKU-100", "qty": 12},  # Li-Ion cells
                    {"component_id": "SKU-101", "qty": 1},  # BMS IC
                    {"component_id": "SKU-105", "qty": 2},  # Temperature sensors
                    {"component_id": "SKU-114", "qty": 2},  # XT60 connectors
                    {"component_id": "SKU-122", "qty": 10.0},  # Copper wire (m)
                    {"component_id": "SKU-135", "qty": 4.0},  # Heat shrink (m)
                    {"component_id": "SKU-127", "qty": 0.2},  # Silicone sealant (kg)
                    {"component_id": "SKU-126", "qty": 0.3},  # Epoxy resin (kg)
                ],
            },
            {
                "sub_assembly": "Propulsion System (4x Motor + ESC)",
                "components": [
                    {"component_id": "SKU-131", "qty": 0.35},  # Neodymium magnets (kg)
                    {"component_id": "SKU-122", "qty": 25.0},  # Copper wire (m)
                    {"component_id": "SKU-141", "qty": 4},  # Bearing sets
                    {"component_id": "SKU-103", "qty": 24},  # Power MOSFETs (ESCs)
                    {"component_id": "SKU-106", "qty": 4},  # Hall sensors
                    {"component_id": "SKU-118", "qty": 4},  # Shunt resistors
                    {"component_id": "SKU-104", "qty": 1},  # DC-DC module (aux rail)
                    {"component_id": "SKU-135", "qty": 6.0},  # Heat shrink (m)
                    {"component_id": "SKU-147", "qty": 2},  # Heat sinks
                    {"component_id": "SKU-139", "qty": 1},  # Cooling fan assembly
                ],
            },
            {
                "sub_assembly": "Flight Controller & Avionics",
                "components": [
                    {"component_id": "SKU-115", "qty": 1},  # PCB
                    {"component_id": "SKU-102", "qty": 1},  # MCU
                    {"component_id": "SKU-119", "qty": 1},  # Crystal
                    {"component_id": "SKU-117", "qty": 20},  # Caps
                    {"component_id": "SKU-116", "qty": 6},  # ESD arrays
                    {"component_id": "SKU-107", "qty": 2},  # CAN transceivers
                    {"component_id": "SKU-104", "qty": 2},  # DC-DC modules
                    {"component_id": "SKU-110", "qty": 1},  # GNSS module
                    {"component_id": "SKU-109", "qty": 1},  # Wi‑Fi module
                    {"component_id": "SKU-108", "qty": 1},  # Ethernet PHY
                    {"component_id": "SKU-112", "qty": 3},  # Buttons
                    {"component_id": "SKU-113", "qty": 4},  # LEDs
                ],
            },
            {
                "sub_assembly": "Camera & Payload Module",
                "components": [
                    {"component_id": "SKU-149", "qty": 1},  # Sensor module assembly
                    {"component_id": "SKU-144", "qty": 1},  # Housing
                    {"component_id": "SKU-138", "qty": 2},  # Linear actuators (gimbal-ish)
                    {"component_id": "SKU-141", "qty": 2},  # Bearings
                    {"component_id": "SKU-123", "qty": 1.2},  # Stainless rod (m)
                    {"component_id": "SKU-126", "qty": 0.1},  # Epoxy resin (kg)
                    {"component_id": "SKU-147", "qty": 1},  # Heat sink
                ],
            },
            {
                "sub_assembly": "Wiring & Connectors",
                "components": [
                    {"component_id": "SKU-140", "qty": 1},  # Wiring harness assembly
                    {"component_id": "SKU-122", "qty": 20.0},  # Copper wire (m)
                    {"component_id": "SKU-114", "qty": 4},  # XT60 connectors
                    {"component_id": "SKU-135", "qty": 8.0},  # Heat shrink (m)
                    {"component_id": "SKU-136", "qty": 2.0},  # Adhesive film (m)
                ],
            },
            {
                "sub_assembly": "Service / Status Interface",
                "components": [
                    {"component_id": "SKU-111", "qty": 1},  # OLED display
                    {"component_id": "SKU-148", "qty": 1},  # Control panel assembly
                    {"component_id": "SKU-112", "qty": 2},  # Buttons
                    {"component_id": "SKU-113", "qty": 2},  # LEDs
                ],
            },
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate bom.json for Industrial Drone X1.")
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
    bom = build_drone_x1_bom()
    validate_component_ids(bom, parts_by_id)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(bom, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote BOM to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

