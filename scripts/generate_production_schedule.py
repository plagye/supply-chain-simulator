from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

JOB_STATUSES = {"Planned", "WIP", "Completed"}


def iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def generate_wip_jobs(*, count: int, seed: int | None = 42, product_id: str = "DRONE-X1") -> list[dict[str, Any]]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)

    jobs: list[dict[str, Any]] = []
    for i in range(count):
        # Stagger start times in the recent past; due dates in the near future.
        start = now - timedelta(hours=rng.randint(2, 36), minutes=rng.randint(0, 59))
        due = now + timedelta(days=rng.randint(1, 7), hours=rng.randint(0, 12))

        jobs.append(
            {
                "job_id": str(uuid.uuid4()),
                "product_id": product_id,
                "status": "WIP",
                "start_date": iso_utc(start),
                "due_date": iso_utc(due),
                "assigned_worker_id": f"WORKER-{i+1:03d}",
            }
        )

    return jobs


def generate_production_schedule(*, wip_jobs: int = 3, seed: int | None = 42) -> dict[str, Any]:
    return {
        "active_jobs": generate_wip_jobs(count=wip_jobs, seed=seed, product_id="DRONE-X1"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate production_schedule.json (initial simulation workload).")
    parser.add_argument(
        "--out",
        type=Path,
        default=DATA_DIR / "production_schedule.json",
        help="Output JSON path (default: production_schedule.json).",
    )
    parser.add_argument(
        "--wip-jobs",
        type=int,
        default=3,
        help="Number of initial WIP jobs to generate (default: 3).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Optional RNG seed for reproducible output (default: 42).",
    )
    args = parser.parse_args()

    if args.wip_jobs < 0:
        raise SystemExit("--wip-jobs must be >= 0")

    schedule = generate_production_schedule(wip_jobs=args.wip_jobs, seed=args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(schedule, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(schedule['active_jobs'])} active jobs to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

