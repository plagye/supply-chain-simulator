from __future__ import annotations

import argparse
import json
import random
import uuid
from pathlib import Path

from faker import Faker

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

REGION_WEIGHTS: list[tuple[str, float]] = [
    ("NA", 0.40),
    ("EMEA", 0.35),
    ("APAC", 0.25),
]

REGION_LOCALES: dict[str, list[str]] = {
    # North America
    "NA": ["en_US", "en_CA"],
    # Europe / Middle East / Africa
    "EMEA": ["en_GB", "de_DE", "fr_FR", "it_IT", "es_ES", "nl_NL", "pl_PL", "tr_TR", "en_ZA", "ar_SA"],
    # Asia-Pacific
    "APAC": ["en_AU", "en_IN", "ja_JP", "ko_KR", "zh_CN", "zh_TW"],
}


GOV_DEFENSE_TIER1_NAMES = [
    "National Aeronautics & Defense Works",
    "State Aerospace Systems Directorate",
    "Federal Ordnance Manufacturing Agency",
    "Government Defense Logistics Corporation",
    "Royal Naval Systems Authority",
]


B2B_PREFIXES = [
    "Apex",
    "Atlas",
    "Summit",
    "Vertex",
    "Helios",
    "Orion",
    "Keystone",
    "Midland",
    "Pacific",
    "Hansa",
    "Rhein",
    "Everbright",
]

B2B_CORES = [
    "Aerospace",
    "Defense",
    "Industrial",
    "Robotics",
    "Automation",
    "Energy",
    "Rail Systems",
    "Mining",
    "Logistics",
    "Maritime Systems",
    "Precision Engineering",
    "Infrastructure",
]

B2B_SUFFIXES = [
    "Group",
    "Holdings",
    "Systems",
    "Industries",
    "Manufacturing",
    "Corporation",
    "Ltd.",
    "Inc.",
    "GmbH",
    "AG",
]


def weighted_choice(rng: random.Random, items_with_weights: list[tuple[str, float]]) -> str:
    items = [i for i, _w in items_with_weights]
    weights = [w for _i, w in items_with_weights]
    return rng.choices(items, weights=weights, k=1)[0]


def b2b_company_name(fake: Faker, rng: random.Random) -> str:
    # Keep names industrial / enterprise-sounding.
    pattern = rng.choice(
        [
            "{prefix} {core}",
            "{prefix} {core} {suffix}",
            "{prefix}-{core} {suffix}",
        ]
    )
    prefix = rng.choice(B2B_PREFIXES)
    core = rng.choice(B2B_CORES)
    suffix = rng.choice(B2B_SUFFIXES)
    name = pattern.format(prefix=prefix, core=core, suffix=suffix).replace("  ", " ").strip()

    # Occasionally blend in a Faker suffix for variety, but avoid people-like phrases.
    if rng.random() < 0.20:
        fs = fake.company_suffix().strip()
        if (" " not in fs) and ("&" not in fs) and len(fs) <= 8:
            name = f"{name} {fs}".strip()
    return name


def shipping_address(fake: Faker) -> str:
    # Single-field address, compacted to one line for JSON consumers.
    return fake.address().replace("\n", ", ")


def shipping_address_for_region(*, region: str, rng: random.Random, seed: int | None, customer_index: int) -> str:
    locales = REGION_LOCALES.get(region, ["en_US"])
    locale = rng.choice(locales)
    try:
        addr_fake = Faker(locale)
    except AttributeError:
        # Locale not available in this Faker build; fall back to a safe default.
        addr_fake = Faker("en_US")
    if seed is not None:
        # Use a stable per-customer seed to keep output reproducible even with multiple Faker instances.
        addr_fake.seed_instance(seed + (customer_index + 1) * 101)
    return shipping_address(addr_fake)


def tier1_penalty_clauses(rng: random.Random) -> dict:
    # Strict late-delivery penalties suitable for critical contracts.
    return {
        "late_delivery": {
            "grace_period_days": 0,
            "penalty_rate": rng.choice(["2% of PO value per day", "1.5% of PO value per day"]),
            "max_penalty": rng.choice(["20% of PO value", "25% of PO value"]),
            "liquidated_damages": True,
            "expedite_at_supplier_cost": True,
            "on_time_delivery_sla": rng.choice([">= 98% monthly", ">= 97% quarterly"]),
        },
        "quality": {
            "incoming_inspection": "100% for critical components",
            "rework_at_supplier_cost": True,
            "rca_due_within_days": 5,
        },
    }


def generate_customers(count: int = 15, seed: int | None = 42) -> list[dict]:
    rng = random.Random(seed)
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)

    # Make a reasonable B2B mix: a handful of Tier 1, rest Tier 2.
    tier1_count = max(4, min(6, count // 3))

    customers: list[dict] = []

    # Ensure some Tier 1 customers look like government-owned defense entities.
    gov_names = GOV_DEFENSE_TIER1_NAMES[:]
    rng.shuffle(gov_names)

    for i in range(count):
        is_tier1 = i < tier1_count
        region = weighted_choice(rng, REGION_WEIGHTS)

        if is_tier1 and gov_names and rng.random() < 0.70:
            company_name = gov_names.pop()
        else:
            company_name = b2b_company_name(fake, rng)

        customer = {
            "customer_id": str(uuid.uuid4()),
            "company_name": company_name,
            "region": region,
            "contract_priority": "Tier 1" if is_tier1 else "Tier 2",
            "shipping_address": shipping_address_for_region(region=region, rng=rng, seed=seed, customer_index=i),
        }

        if is_tier1:
            customer["penalty_clauses"] = tier1_penalty_clauses(rng)

        customers.append(customer)

    return customers


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate customers.json (B2B clients).")
    parser.add_argument("--count", type=int, default=15, help="Number of customers to generate (default: 15).")
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "customers.json",
        help="Output JSON path (default: customers.json).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Optional RNG seed for reproducible output (default: 42).",
    )
    args = parser.parse_args()

    customers = generate_customers(count=args.count, seed=args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(customers, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(customers)} customers to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

