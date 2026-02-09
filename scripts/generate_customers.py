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

# Default destination facility and location_code per region (must match data/facilities.json)
REGION_TO_DESTINATION_FACILITY: dict[str, str] = {
    "NA": "dist_na_01",
    "EMEA": "dist_emea_01",
    "APAC": "dist_apac_01",
}

REGION_TO_DELIVERY_LOCATION_CODE: dict[str, str] = {
    "NA": "USA_DET",
    "EMEA": "NLD_RTM",
    "APAC": "SGP_SIN",
}

# (locale, country_name) with weight for weighted choice per region. Realistic addresses via Faker(locale).
REGION_COUNTRY_OPTIONS: dict[str, list[tuple[str, str, float]]] = {
    "NA": [
        ("en_US", "USA", 0.85),
        ("en_CA", "Canada", 0.15),
    ],
    "EMEA": [
        ("de_DE", "Germany", 0.20),
        ("en_GB", "United Kingdom", 0.18),
        ("nl_NL", "Netherlands", 0.15),
        ("fr_FR", "France", 0.14),
        ("it_IT", "Italy", 0.10),
        ("es_ES", "Spain", 0.08),
        ("pl_PL", "Poland", 0.06),
        ("en_ZA", "South Africa", 0.05),
        ("tr_TR", "Turkey", 0.04),
    ],
    "APAC": [
        ("en_AU", "Australia", 0.25),
        ("ja_JP", "Japan", 0.22),
        ("en_IN", "India", 0.18),
        ("ko_KR", "South Korea", 0.12),
        ("zh_CN", "China", 0.10),
        ("zh_TW", "Taiwan", 0.08),
        ("en_SG", "Singapore", 0.05),
    ],
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


def weighted_choice_country(
    region: str, rng: random.Random
) -> tuple[str, str]:
    """Return (locale, country_name) for the given region with weighted random choice."""
    options = REGION_COUNTRY_OPTIONS.get(region, [("en_US", "USA", 1.0)])
    locales = [o[0] for o in options]
    countries = [o[1] for o in options]
    weights = [o[2] for o in options]
    idx = rng.choices(range(len(options)), weights=weights, k=1)[0]
    return locales[idx], countries[idx]


def structured_address_for_locale(
    locale: str, country_name: str, rng: random.Random, seed: int | None, customer_index: int
) -> dict:
    """Generate street, city, state, postal_code, country using Faker(locale)."""
    try:
        fake = Faker(locale)
    except (AttributeError, TypeError):
        fake = Faker("en_US")
    if seed is not None:
        fake.seed_instance(seed + (customer_index + 1) * 101)
    street = fake.street_address()
    city = fake.city()
    try:
        state = fake.state() if locale in ("en_US", "en_CA", "en_AU") else ""
    except Exception:
        state = ""
    postal_code = fake.postcode()
    return {
        "street": street,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "country": country_name,
    }


def format_shipping_address_line(addr: dict) -> str:
    """Single line derived from structured address (for convenience)."""
    parts = [addr["street"], addr["city"]]
    if addr.get("state"):
        parts.append(addr["state"])
    parts.append(addr["postal_code"])
    parts.append(addr["country"])
    return ", ".join(str(p) for p in parts if p)


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
        locale, country_name = weighted_choice_country(region, rng)
        addr = structured_address_for_locale(locale, country_name, rng, seed, i)

        if is_tier1 and gov_names and rng.random() < 0.70:
            company_name = gov_names.pop()
        else:
            company_name = b2b_company_name(fake, rng)

        destination_facility_id = REGION_TO_DESTINATION_FACILITY.get(region, "dist_na_01")
        delivery_location_code = REGION_TO_DELIVERY_LOCATION_CODE.get(region, "USA_DET")

        customer = {
            "customer_id": str(uuid.uuid4()),
            "company_name": company_name,
            "region": region,
            "country": addr["country"],
            "street": addr["street"],
            "city": addr["city"],
            "state": addr["state"],
            "postal_code": addr["postal_code"],
            "shipping_address": format_shipping_address_line(addr),
            "destination_facility_id": destination_facility_id,
            "delivery_location_code": delivery_location_code,
            "contract_priority": "Tier 1" if is_tier1 else "Tier 2",
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

