from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.world_engine import WorldEngine, SimulationError, DataLoadError, ConfigValidationError


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = BASE_DIR / "scripts"
DEFAULT_CONFIG_PATH = BASE_DIR / "config.json"


def parse_start_time(value: str) -> datetime:
    """Parse ISO 8601 datetime string with optional trailing 'Z'."""
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError as e:
        raise ValueError(f"Invalid start time format '{value}': {e}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def load_config(path: Path | None) -> dict[str, Any]:
    """Load configuration from JSON file."""
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file {path}: {e}", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error: Cannot read config file {path}: {e}", file=sys.stderr)
        sys.exit(1)


def resolve_value(cli_value: Any, cfg_section: dict[str, Any], key: str, fallback: Any) -> Any:
    """Resolve configuration value with priority: CLI > config file > fallback."""
    if cli_value is not None:
        return cli_value
    if key in cfg_section:
        return cfg_section[key]
    return fallback


def run_script(script_name: str, args: list[str]) -> None:
    """Run a generator script as a subprocess."""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"Error: Script not found: {script_path}", file=sys.stderr)
        sys.exit(1)
    
    cmd = [sys.executable, str(script_path), *args]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: Script {script_name} failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"Error: Cannot execute {cmd[0]}: {e}", file=sys.stderr)
        sys.exit(1)


def generate_all(seed: int | None = None) -> None:
    """Generate all data files using generator scripts."""
    seed_args = ["--seed", str(seed)] if seed is not None else []

    print("Generating suppliers...")
    run_script("generate_suppliers.py", seed_args)
    print("Generating parts...")
    run_script("generate_parts.py", seed_args)
    print("Generating BOM...")
    run_script("generate_bom.py", [])
    print("Generating customers...")
    run_script("generate_customers.py", seed_args)
    print("Generating inventory...")
    run_script("generate_inventory.py", seed_args)
    print("Generating production schedule...")
    run_script("generate_production_schedule.py", seed_args)
    print("Data generation complete.")


def validate_simulation_params(ticks: int, seed: int | None) -> None:
    """Validate simulation parameters."""
    if ticks <= 0:
        print(f"Error: ticks must be positive, got {ticks}", file=sys.stderr)
        sys.exit(1)
    if seed is not None and seed < 0:
        print(f"Error: seed must be non-negative, got {seed}", file=sys.stderr)
        sys.exit(1)


def run_simulation(
    ticks: int,
    seed: int | None,
    start_time: datetime | None,
    engine_config: dict[str, Any] | None = None,
) -> None:
    """Run the supply chain simulation."""
    validate_simulation_params(ticks, seed)
    
    try:
        engine = WorldEngine(
            data_dir=DATA_DIR,
            seed=seed,
            start_time=start_time,
            config=engine_config,
        )
    except DataLoadError as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        print("Hint: Run 'python main.py generate' first to create data files.", file=sys.stderr)
        sys.exit(1)
    except ConfigValidationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Running simulation for {ticks} ticks...")
    for i in range(ticks):
        engine.tick()
        # Progress indicator every 24 ticks (1 simulated day)
        if (i + 1) % 24 == 0:
            print(f"  Completed {i + 1} ticks ({(i + 1) // 24} days)")
    
    engine.save_state()
    print(f"Simulation complete. Events logged to: {engine.log_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Supply chain simulator CLI")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config file (default: ./config.json if present).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate all data files")
    gen.add_argument("--seed", type=int, default=None, help="Seed for generators.")

    sim = sub.add_parser("simulate", help="Run the simulator")
    sim.add_argument("--ticks", type=int, default=None, help="Number of hourly ticks to run.")
    sim.add_argument("--seed", type=int, default=None, help="Simulation RNG seed.")
    sim.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="ISO-8601 start time (e.g., 2026-02-02T08:00:00Z).",
    )

    both = sub.add_parser("all", help="Generate all data then run the simulator")
    both.add_argument("--ticks", type=int, default=None, help="Number of hourly ticks to run.")
    both.add_argument("--seed", type=int, default=None, help="Seed for generators and simulator.")
    both.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="ISO-8601 start time (e.g., 2026-02-02T08:00:00Z).",
    )

    args = parser.parse_args()
    config_path = args.config or (DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None)
    config = load_config(config_path)

    if args.command == "generate":
        section = config.get("generate", {})
        seed = resolve_value(args.seed, section, "seed", 42)
        generate_all(seed=seed)
        return 0

    if args.command == "simulate":
        section = config.get("simulate", {})
        ticks = resolve_value(args.ticks, section, "ticks", 24)
        seed = resolve_value(args.seed, section, "seed", 42)
        start_raw = resolve_value(args.start_time, section, "start_time", None)
        engine_config = section.get("engine", {})
        try:
            start_time = parse_start_time(start_raw) if start_raw else None
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        run_simulation(ticks=ticks, seed=seed, start_time=start_time, engine_config=engine_config)
        return 0

    if args.command == "all":
        section = config.get("all", {})
        ticks = resolve_value(args.ticks, section, "ticks", 24)
        seed = resolve_value(args.seed, section, "seed", 42)
        start_raw = resolve_value(args.start_time, section, "start_time", None)
        engine_config = section.get("engine", {})
        try:
            start_time = parse_start_time(start_raw) if start_raw else None
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        generate_all(seed=seed)
        run_simulation(ticks=ticks, seed=seed, start_time=start_time, engine_config=engine_config)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

