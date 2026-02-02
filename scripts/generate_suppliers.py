from __future__ import annotations

import argparse
import json
import random
import uuid
from pathlib import Path

from faker import Faker

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

COUNTRY_WEIGHTS = [
    ("China", 0.40),
    ("Taiwan", 0.20),
    ("Germany", 0.20),
    ("USA", 0.20),
]

INDUSTRIAL_PREFIXES = [
    "Apex",
    "Sino",
    "Nova",
    "Vertex",
    "Titan",
    "Summit",
    "Iron",
    "Blue Ridge",
    "Hansa",
    "Helios",
    "Orion",
    "Atlas",
    "Everbright",
    "Pacific",
    "Rhein",
    "Midland",
    "Keystone",
    "Boreal",
    "Kestrel",
    "Shenzhen",
    "Taipei",
]

INDUSTRIAL_CORES = [
    "Steel",
    "Dynamics",
    "Industrial",
    "Industries",
    "Manufacturing",
    "Metals",
    "Alloy",
    "Components",
    "Machinery",
    "Foundry",
    "Precision",
    "Bearings",
    "Logistics",
    "Electromech",
    "Materials",
    "Fabrication",
    "Forge",
    "Engineering",
    "Automation",
    "Systems",
]

INDUSTRIAL_SUFFIXES = [
    "Works",
    "Group",
    "Holdings",
    "Co.",
    "Ltd.",
    "GmbH",
    "AG",
    "Inc.",
]

LEGAL_SUFFIX_ALLOWLIST = {
    "Inc",
    "Inc.",
    "LLC",
    "Ltd",
    "Ltd.",
    "Co",
    "Co.",
    "Corp",
    "Corp.",
    "GmbH",
    "AG",
    "S.A.",
    "BV",
    "KK",
    "PLC",
    "Group",
    "Holdings",
}


def weighted_choice(rng: random.Random, items_with_weights: list[tuple[str, float]]) -> str:
    items = [i for i, _w in items_with_weights]
    weights = [w for _i, w in items_with_weights]
    return rng.choices(items, weights=weights, k=1)[0]


def risk_factor_from_reliability(reliability_score: float) -> str:
    # Lower reliability => higher risk. Thresholds chosen to map the 0.7–1.0 range into 3 buckets.
    if reliability_score >= 0.90:
        return "Low"
    if reliability_score >= 0.80:
        return "Medium"
    return "High"


def company_suffix_industrial(fake: Faker, rng: random.Random) -> str:
    # Faker may return human/legacy forms like "and Sons". Filter those out to keep names industrial.
    suffix = fake.company_suffix().strip()
    if suffix in LEGAL_SUFFIX_ALLOWLIST:
        return suffix
    if " " in suffix or "&" in suffix:
        return rng.choice(INDUSTRIAL_SUFFIXES)
    # Allow short one-token suffixes (e.g. "Inc") even if not in allowlist, otherwise fall back.
    return suffix if len(suffix) <= 6 else rng.choice(INDUSTRIAL_SUFFIXES)


def industrial_name(fake: Faker, rng: random.Random) -> str:
    # Blend Faker with deterministic industrial templates to avoid "consumer" sounding names.
    # Examples produced: "Apex Dynamics", "Sino-Steel Works", "Hansa Precision GmbH"
    pattern = rng.choice(
        [
            "{prefix} {core}",
            "{prefix}-{core}",
            "{prefix} {core} {suffix}",
            "{prefix}-{core} {suffix}",
            "{prefix} {core} {legal}",
        ]
    )

    prefix = rng.choice(INDUSTRIAL_PREFIXES)
    core = rng.choice(INDUSTRIAL_CORES)
    suffix = rng.choice(INDUSTRIAL_SUFFIXES)
    legal = company_suffix_industrial(fake, rng)

    return pattern.format(prefix=prefix, core=core, suffix=suffix, legal=legal).strip()


def price_multiplier_from_reliability(reliability: float, rng: random.Random) -> float:
    """
    Generate a price multiplier based on reliability.
    
    More reliable suppliers tend to charge more (premium for quality).
    Less reliable suppliers are cheaper (discount for risk).
    
    - Reliability 0.95-1.00: price mult 1.05-1.15 (premium)
    - Reliability 0.85-0.95: price mult 0.95-1.05 (normal)
    - Reliability 0.70-0.85: price mult 0.80-0.95 (discount)
    """
    if reliability >= 0.95:
        return round(rng.uniform(1.05, 1.15), 2)
    elif reliability >= 0.85:
        return round(rng.uniform(0.95, 1.05), 2)
    else:
        return round(rng.uniform(0.80, 0.95), 2)


def generate_suppliers(count: int, seed: int | None = None) -> list[dict]:
    rng = random.Random(seed)
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)

    suppliers: list[dict] = []
    for _ in range(count):
        reliability = round(rng.uniform(0.70, 1.00), 3)
        suppliers.append(
            {
                "id": str(uuid.uuid4()),
                "name": industrial_name(fake, rng),
                "country": weighted_choice(rng, COUNTRY_WEIGHTS),
                "reliability_score": reliability,
                "risk_factor": risk_factor_from_reliability(reliability),
                "price_multiplier": price_multiplier_from_reliability(reliability, rng),
            }
        )

    return suppliers


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a suppliers.json file (industrial-sounding names).")
    parser.add_argument("--count", type=int, default=30, help="Number of suppliers to generate (default: 30).")
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "suppliers.json",
        help="Output JSON path (default: suppliers.json).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible output.",
    )

    args = parser.parse_args()

    suppliers = generate_suppliers(count=args.count, seed=args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(suppliers, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(suppliers)} suppliers to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
