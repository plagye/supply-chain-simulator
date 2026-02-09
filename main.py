from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from scripts.world_engine import WorldEngine, SimulationError, DataLoadError, ConfigValidationError


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = BASE_DIR / "scripts"
DEFAULT_CONFIG_PATH = BASE_DIR / "config.json"
LOG_FILE = BASE_DIR / "simulation.log"

# Global logger
logger: logging.Logger | None = None


def setup_logging(log_file: Path = LOG_FILE, level: int = logging.INFO) -> logging.Logger:
    """Set up rotating file-based logging for background service operation."""
    log = logging.getLogger("simulation")
    log.setLevel(level)
    
    # Avoid duplicate handlers
    if log.handlers:
        return log
    
    # File handler with rotation (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Format: timestamp - level - message
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    log.addHandler(file_handler)
    log.addHandler(console_handler)
    
    return log


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
    print("Generating facilities...")
    run_script("generate_facilities.py", [])
    print("Generating routes...")
    run_script("generate_routes.py", [])
    print("Generating customers...")
    run_script("generate_customers.py", seed_args)
    print("Generating inventory...")
    run_script("generate_inventory.py", seed_args)
    # production_schedule.json is optional; write minimal file so it exists after generate
    (DATA_DIR / "production_schedule.json").write_text(
        '{"active_jobs": []}\n', encoding="utf-8"
    )
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
    print(f"Simulation complete. Events logged to: date-partitioned files in {engine.events_dir}")


def run_history_generation(
    years: int,
    seed: int | None,
    start_time: datetime | None,
    engine_config: dict[str, Any] | None = None,
) -> None:
    """Generate N years of historical data to a single JSONL file.
    
    This runs the simulation in accelerated mode (no delays) to build
    up historical data that can later be manually transferred to PostgreSQL.
    Events are written to a single file for speed (no per-day rollover).
    """
    if years < 1 or years > 3:
        print(f"Error: years must be between 1 and 3, got {years}", file=sys.stderr)
        sys.exit(1)
    
    # Calculate total ticks (hours in N years)
    # Using 365.25 days/year to account for leap years
    ticks = int(years * 365.25 * 24)
    include_black_swan = (years == 3)
    history_output_path = DATA_DIR / "events" / "history.jsonl"
    
    print(f"Generating {years} year(s) of historical data...")
    print(f"  Total ticks: {ticks:,} ({ticks // 24:,} days)")
    if include_black_swan:
        print("  Black swan event: ENABLED (3-year mode)")
    else:
        print("  Black swan event: disabled (only enabled for 3-year history)")
    
    try:
        engine = WorldEngine(
            data_dir=DATA_DIR,
            seed=seed,
            start_time=start_time,
            config=engine_config,
            include_black_swan=include_black_swan,
            simulation_years=years,
            events_single_file=True,
            events_single_file_path=history_output_path,
        )
    except DataLoadError as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        print("Hint: Run 'python main.py generate' first to create data files.", file=sys.stderr)
        sys.exit(1)
    except ConfigValidationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Track progress
    start_real_time = time.time()
    last_progress = 0
    
    print("\nRunning accelerated simulation...")
    for i in range(ticks):
        engine.tick()
        
        # Progress indicator every simulated week (168 ticks)
        progress_pct = int((i + 1) / ticks * 100)
        if progress_pct >= last_progress + 5:  # Every 5%
            elapsed = time.time() - start_real_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_seconds = (ticks - i - 1) / rate if rate > 0 else 0
            
            sim_date = engine.current_time.strftime("%Y-%m-%d")
            print(f"  {progress_pct:3d}% | Sim date: {sim_date} | "
                  f"Rate: {rate:.0f} ticks/sec | ETA: {eta_seconds / 60:.1f} min")
            last_progress = progress_pct
    
    engine.save_state()
    
    elapsed_total = time.time() - start_real_time
    print(f"\nHistorical data generation complete!")
    print(f"  Time elapsed: {elapsed_total / 60:.1f} minutes")
    print(f"  Events logged to: {history_output_path}")
    print(f"  Final sim date: {engine.current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("\nNext steps:")
    print("  1. Review the generated JSON files in the data/ directory")
    print("  2. Transfer historical data to PostgreSQL using your preferred method")
    print("  3. Run 'python main.py run-service' to start 24/7 simulation")


def run_continuous_service(
    tick_interval: float,
    resume: bool,
    seed: int | None,
    engine_config: dict[str, Any] | None = None,
    api_host: str = "127.0.0.1",
    api_port: int = 8010,
    api_enabled: bool = True,
) -> None:
    """Run simulation as a continuous 24/7 service.
    
    Events are written to date-partitioned JSONL (data/events/). State is
    persisted to PostgreSQL after each tick for resume capability.
    """
    global logger
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("Starting Supply Chain Simulation Service")
    logger.info("=" * 60)
    
    start_time: datetime | None = None
    initial_tick_count = 0
    
    # Try to resume from database state
    if resume:
        try:
            from scripts.db_manager import load_system_state, test_connection
            
            if not test_connection():
                logger.error("Database connection failed. Check your .env configuration.")
                logger.info("Hint: Ensure DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD are set.")
                sys.exit(1)
            
            state = load_system_state()
            if state:
                start_time = state["current_simulation_time"]
                initial_tick_count = state["tick_count"]
                logger.info(f"Resuming from saved state:")
                logger.info(f"  Simulation time: {start_time}")
                logger.info(f"  Tick count: {initial_tick_count:,}")
            else:
                logger.info("No saved state found. Starting fresh simulation.")
        except ImportError as e:
            logger.warning(f"Database module not available: {e}")
            logger.info("Starting fresh simulation without resume capability.")
        except Exception as e:
            logger.warning(f"Could not load state from database: {e}")
            logger.info("Starting fresh simulation.")
    else:
        logger.info("Starting fresh simulation (--fresh mode)")
    
    # Create engine
    try:
        engine = WorldEngine(
            data_dir=DATA_DIR,
            seed=seed,
            start_time=start_time,
            config=engine_config,
        )
        engine.tick_count = initial_tick_count
    except DataLoadError as e:
        logger.error(f"Error loading data: {e}")
        logger.info("Hint: Run 'python main.py generate' first to create data files.")
        sys.exit(1)
    except ConfigValidationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Set up graceful shutdown
    def handle_shutdown(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        engine.shutdown()
    
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    # Start live API in daemon thread (read-only; call and get values)
    if api_enabled:
        try:
            import threading
            from scripts.api import create_app
            import uvicorn
            app = create_app(engine)
            thread = threading.Thread(
                target=uvicorn.run,
                kwargs={"app": app, "host": api_host, "port": api_port},
                daemon=True,
            )
            thread.start()
            logger.info(f"  API: http://{api_host}:{api_port} (GET /status, /inventory, /backorders, /deliveries)")
        except Exception as e:
            logger.warning(f"Could not start API server: {e}")
    
    logger.info(f"Service configuration:")
    logger.info(f"  Tick interval: {tick_interval} seconds")
    logger.info(f"  Events: JSONL (date-partitioned in data/events/)")
    logger.info(f"  Starting simulation time: {engine.current_time}")
    logger.info("")
    logger.info("Service is running. Press Ctrl+C to stop.")
    
    # Import db_manager for state persistence (optional)
    db_manager = None
    try:
        from scripts import db_manager as dbm
        db_manager = dbm
    except ImportError:
        logger.warning("Database module not available. State will not be persisted.")
    
    # Main service loop
    ticks_since_log = 0
    while engine.running:
        try:
            tick_start = time.time()
            
            # Run one simulation tick
            engine.tick()
            ticks_since_log += 1
            
            # Save state to database
            if db_manager:
                try:
                    db_manager.save_system_state(
                        engine.current_time,
                        engine.tick_count,
                        status="running"
                    )
                except Exception as e:
                    logger.warning(f"Failed to save state: {e}")
            
            # Log progress every 24 ticks (1 simulated day)
            if ticks_since_log >= 24:
                logger.info(
                    f"Tick {engine.tick_count:,} | "
                    f"Sim time: {engine.current_time.strftime('%Y-%m-%d %H:%M')} | "
                    f"Day {engine.tick_count // 24:,}"
                )
                ticks_since_log = 0
            
            # Sleep for the configured interval
            tick_duration = time.time() - tick_start
            sleep_time = max(0, tick_interval - tick_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"Error during tick {engine.tick_count}: {e}", exc_info=True)
            # Continue running despite errors
            time.sleep(tick_interval)
    
    # Graceful shutdown
    logger.info("Shutting down...")
    
    # Save final state
    if db_manager:
        try:
            db_manager.save_system_state(
                engine.current_time,
                engine.tick_count,
                status="stopped"
            )
            logger.info("Final state saved to database.")
        except Exception as e:
            logger.error(f"Failed to save final state: {e}")
    
    # Save to JSON as backup
    try:
        engine.save_state()
        logger.info(f"State saved to JSON: {engine.data_dir}")
    except Exception as e:
        logger.error(f"Failed to save JSON state: {e}")
    
    logger.info(f"Service stopped. Final tick count: {engine.tick_count:,}")
    logger.info(f"Final simulation time: {engine.current_time}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Supply chain simulator CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  generate         Generate all master data files
  simulate         Run simulation for a fixed number of ticks
  all              Generate data then run simulation
  generate-history Generate N years of historical data to JSON
  run-service      Run as a continuous 24/7 service (writes to PostgreSQL)

Examples:
  python main.py generate --seed 42
  python main.py simulate --ticks 720 --seed 42
  python main.py generate-history --years 3 --seed 42
  python main.py run-service --tick-interval 5 --resume
        """
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config file (default: ./config.json if present).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # generate command
    gen = sub.add_parser("generate", help="Generate all data files")
    gen.add_argument("--seed", type=int, default=None, help="Seed for generators.")

    # simulate command
    sim = sub.add_parser("simulate", help="Run the simulator for fixed ticks")
    sim.add_argument("--ticks", type=int, default=None, help="Number of hourly ticks to run.")
    sim.add_argument("--seed", type=int, default=None, help="Simulation RNG seed.")
    sim.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="ISO-8601 start time (e.g., 2026-02-02T08:00:00Z).",
    )

    # all command (generate + simulate)
    both = sub.add_parser("all", help="Generate all data then run the simulator")
    both.add_argument("--ticks", type=int, default=None, help="Number of hourly ticks to run.")
    both.add_argument("--seed", type=int, default=None, help="Seed for generators and simulator.")
    both.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="ISO-8601 start time (e.g., 2026-02-02T08:00:00Z).",
    )

    # generate-history command
    hist = sub.add_parser(
        "generate-history",
        help="Generate N years of historical data to JSON files"
    )
    hist.add_argument(
        "--years",
        type=int,
        required=True,
        choices=[1, 2, 3],
        help="Number of years of history to generate (1-3). Black swan event only when generating 3 years.",
    )
    hist.add_argument("--seed", type=int, default=None, help="Simulation RNG seed.")
    hist.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="ISO-8601 start time for historical data (default: N years before now).",
    )

    # run-service command
    svc = sub.add_parser(
        "run-service",
        help="Run as a continuous 24/7 service (writes to PostgreSQL)"
    )
    svc.add_argument(
        "--tick-interval",
        type=float,
        default=5.0,
        help="Seconds between simulation ticks (default: 5.0).",
    )
    svc.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from saved database state (default).",
    )
    svc.add_argument(
        "--fresh",
        action="store_true",
        help="Start fresh, ignoring any saved state.",
    )
    svc.add_argument("--seed", type=int, default=None, help="Simulation RNG seed.")

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

    if args.command == "generate-history":
        section = config.get("generate-history", {})
        years = args.years  # Required, no fallback
        seed = resolve_value(args.seed, section, "seed", 42)
        start_raw = resolve_value(args.start_time, section, "start_time", None)
        engine_config = section.get("engine", {})

        # Default start time: N years before now
        if start_raw:
            try:
                start_time = parse_start_time(start_raw)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                return 1
        else:
            start_time = datetime.now(timezone.utc) - timedelta(days=years * 365)
        
        run_history_generation(
            years=years,
            seed=seed,
            start_time=start_time,
            engine_config=engine_config
        )
        return 0

    if args.command == "run-service":
        section = config.get("run-service", {})
        tick_interval = resolve_value(args.tick_interval, section, "tick_interval", 5.0)
        seed = resolve_value(args.seed, section, "seed", 42)
        engine_config = section.get("engine", {})
        api_host = section.get("api_host", "127.0.0.1")
        api_port = section.get("api_port", 8010)
        api_enabled = section.get("api_enabled", True)
        # --fresh overrides --resume
        resume = not args.fresh
        run_continuous_service(
            tick_interval=tick_interval,
            resume=resume,
            seed=seed,
            engine_config=engine_config,
            api_host=api_host,
            api_port=api_port,
            api_enabled=api_enabled,
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
