"""World Engine - Core simulation engine for supply chain events.

This module implements the main simulation logic for the supply chain simulator.
It handles demand generation, inventory management, production scheduling,
procurement, and supplier interactions.

Key Classes:
    WorldEngine: Main simulation engine that orchestrates all events
    BlackSwanEvent: Represents major supply chain disruptions
    SalesOrder, PendingPurchaseOrder, PendingBackorder: Data classes for tracking orders

Key Features:
    - Hourly tick-based simulation
    - Configurable seasonality (monthly, day-of-week, period-end)
    - Supplier reliability and lead time modeling
    - Production job management with BOM consumption
    - Automatic reorder point triggers
    - Black swan event support for historical data generation
    - Events written to date-partitioned JSONL (data/events/YYYY-MM-DD.jsonl)

Usage:
    from scripts.world_engine import WorldEngine
    
    # Basic usage
    engine = WorldEngine(seed=42)
    for _ in range(24):  # Simulate 1 day
        engine.tick()
    engine.save_state()
    
    # 24/7 service mode (events to JSONL; state to PostgreSQL)
    engine = WorldEngine()
    while engine.running:
        engine.tick()
        time.sleep(5)

Configuration:
    See DEFAULT_CONFIG dict for all configurable parameters.
    Override via the config parameter in __init__.

Author: SkyForge Dynamics Data Engineering Team
"""

from __future__ import annotations

import atexit
import io
import json
import os
import random
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Default simulation parameters (can be overridden via config)
DEFAULT_CONFIG = {
    # Demand settings
    "demand_probability_base": 0.05,
    "demand_probability_business_hours": 0.12,
    "business_hours_start": 8,
    "business_hours_end": 18,
    "bulk_order_probability": 0.08,
    "bulk_order_qty_min": 10,
    "bulk_order_qty_max": 20,
    "normal_order_qty_min": 1,
    "normal_order_qty_max": 5,
    # Production settings
    "production_duration_hours_min": 8,
    "production_duration_hours_max": 24,
    # Supplier/procurement settings
    "base_lead_time_hours_min": 24,
    "base_lead_time_hours_max": 168,
    "partial_shipment_probability": 0.15,
    "partial_shipment_min_pct": 0.80,
    "partial_shipment_max_pct": 0.95,
    "quality_reject_rate_min": 0.01,
    "quality_reject_rate_max": 0.05,
    # Data corruption settings (for error handling practice)
    "data_corruption_enabled": True,
    "data_corruption_probability": 0.01,
    # Cost variation settings
    "cost_drift_enabled": True,
    "cost_drift_daily_pct": 0.005,
    "cost_drift_max_pct": 0.20,
    # Seasonality settings
    "seasonality_enabled": True,
    "demand_seasonality_strength": 1.0,
    "supplier_seasonality_strength": 1.0,
}

# Demand seasonality by month (multipliers)
DEMAND_SEASONALITY = {
    1: 0.8,    # January - post-holiday slump
    2: 0.85,   # February
    3: 1.0,    # March
    4: 1.0,    # April
    5: 1.05,   # May
    6: 0.9,    # June - summer lull starts
    7: 0.85,   # July - vacation season
    8: 0.85,   # August
    9: 1.1,    # September - back to business
    10: 1.2,   # October - Q4 ramp
    11: 1.4,   # November - peak season
    12: 1.3,   # December - holiday orders
}

# Day-of-week demand multipliers (0=Monday, 6=Sunday)
DAY_OF_WEEK_DEMAND = {
    0: 0.85,   # Monday - slow start to the week
    1: 0.95,   # Tuesday - ramping up
    2: 1.0,    # Wednesday - mid-week baseline
    3: 1.05,   # Thursday - building momentum
    4: 1.25,   # Friday - end-of-week rush, orders before weekend
    5: 0.6,    # Saturday - reduced business activity
    6: 0.4,    # Sunday - minimal activity
}

# Supplier seasonality by country and date range
# Format: (start_month, start_day, end_month, end_day): {lead_time_mult, reliability_mult}
SUPPLIER_SEASONALITY = {
    "China": [
        # Chinese New Year (late Jan - mid Feb) - major disruption
        ((1, 15), (1, 31), {"lead_time_mult": 2.5, "reliability_mult": 0.7}),
        ((2, 1), (2, 15), {"lead_time_mult": 3.0, "reliability_mult": 0.5}),
        ((2, 16), (2, 28), {"lead_time_mult": 1.5, "reliability_mult": 0.8}),
        # October Golden Week
        ((10, 1), (10, 7), {"lead_time_mult": 1.8, "reliability_mult": 0.75}),
    ],
    "Taiwan": [
        # Chinese New Year impact
        ((1, 15), (1, 31), {"lead_time_mult": 2.0, "reliability_mult": 0.75}),
        ((2, 1), (2, 15), {"lead_time_mult": 2.5, "reliability_mult": 0.6}),
        ((2, 16), (2, 28), {"lead_time_mult": 1.3, "reliability_mult": 0.85}),
    ],
    "Germany": [
        # August vacation season
        ((8, 1), (8, 31), {"lead_time_mult": 1.5, "reliability_mult": 0.85}),
        # Christmas/New Year
        ((12, 15), (12, 31), {"lead_time_mult": 1.8, "reliability_mult": 0.8}),
        ((1, 1), (1, 6), {"lead_time_mult": 1.5, "reliability_mult": 0.85}),
    ],
    "USA": [
        # Thanksgiving week
        ((11, 20), (11, 30), {"lead_time_mult": 1.3, "reliability_mult": 0.9}),
        # Christmas/New Year
        ((12, 20), (12, 31), {"lead_time_mult": 1.5, "reliability_mult": 0.85}),
        ((1, 1), (1, 3), {"lead_time_mult": 1.3, "reliability_mult": 0.9}),
        # July 4th week
        ((7, 1), (7, 7), {"lead_time_mult": 1.2, "reliability_mult": 0.92}),
    ],
}


class SimulationError(Exception):
    """Raised when simulation encounters an unrecoverable error."""
    pass


class DataLoadError(SimulationError):
    """Raised when required data files cannot be loaded."""
    pass


class ConfigValidationError(SimulationError):
    """Raised when configuration values are invalid."""
    pass


def iso_utc(dt: datetime) -> str:
    """Convert datetime to ISO 8601 UTC string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_json(path: Path) -> Any:
    """Load JSON file with error handling."""
    if not path.exists():
        raise DataLoadError(f"Required data file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise DataLoadError(f"Invalid JSON in {path}: {e}") from e


@dataclass
class SalesOrder:
    order_id: str
    customer_id: str
    product_id: str
    qty: int
    created_at: str


@dataclass
class PendingPurchaseOrder:
    """Tracks a purchase order awaiting delivery."""
    purchase_order_id: str
    part_id: str
    qty: float
    supplier_id: str | None
    eta: datetime
    created_at: datetime
    unit_cost: float = 0.0  # Actual cost at time of order


@dataclass
class PendingBackorder:
    """Tracks a backorder awaiting fulfillment."""
    order_id: str
    customer_id: str
    product_id: str
    qty_remaining: int
    original_qty: int
    created_at: datetime


@dataclass
class BlackSwanEvent:
    """Represents a major supply chain disruption event.
    
    Black swan events cause significant but temporary disruptions to
    demand and/or supplier lead times for affected regions.
    """
    name: str
    start_date: datetime
    duration_days: int
    demand_multiplier: float       # e.g., 0.7 for 30% demand drop
    lead_time_multiplier: float    # e.g., 2.5 for 2.5x longer lead times
    affected_countries: list[str]  # e.g., ["China", "Taiwan"]
    
    @property
    def end_date(self) -> datetime:
        """Calculate the end date of the event."""
        return self.start_date + timedelta(days=self.duration_days)
    
    def is_active(self, current_time: datetime) -> bool:
        """Check if the event is currently active."""
        return self.start_date <= current_time < self.end_date


# Templates for black swan events (used when generating 5-year history)
BLACK_SWAN_TEMPLATES = [
    {
        "name": "Supply Chain Crisis",
        "duration_days": 21,
        "demand_multiplier": 0.7,
        "lead_time_multiplier": 2.5,
        "affected_countries": ["China", "Taiwan"],
    },
    {
        "name": "Port Congestion Event",
        "duration_days": 30,
        "demand_multiplier": 0.9,
        "lead_time_multiplier": 2.0,
        "affected_countries": ["China", "USA"],
    },
    {
        "name": "Regional Natural Disaster",
        "duration_days": 14,
        "demand_multiplier": 0.5,
        "lead_time_multiplier": 3.0,
        "affected_countries": ["Taiwan"],
    },
    {
        "name": "Global Logistics Disruption",
        "duration_days": 28,
        "demand_multiplier": 0.8,
        "lead_time_multiplier": 2.2,
        "affected_countries": ["China", "Germany", "USA"],
    },
    {
        "name": "Semiconductor Shortage",
        "duration_days": 25,
        "demand_multiplier": 1.1,  # Demand actually increases (panic buying)
        "lead_time_multiplier": 3.5,
        "affected_countries": ["Taiwan", "China"],
    },
]


class WorldEngine:
    """
    Core simulation engine for supply chain events.
    
    Simulates demand generation, inventory management, production,
    and procurement with realistic variability.
    """

    def __init__(
        self,
        *,
        data_dir: Path | None = None,
        seed: int | None = 42,
        start_time: datetime | None = None,
        config: dict[str, Any] | None = None,
        include_black_swan: bool = False,
        simulation_years: int = 1,
    ) -> None:
        self.data_dir = data_dir or DATA_DIR
        self.rng = random.Random(seed)
        self.current_time = start_time or datetime.now(timezone.utc)
        self.tick_count = 0
        self.running = True  # For service mode graceful shutdown
        
        # Merge provided config with defaults
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._validate_config()

        # Events directory (date-partitioned JSONL); config or env EVENTS_DIR, default data/events
        events_dir_raw = self.config.get("events_dir") or os.environ.get("EVENTS_DIR", "data/events")
        self.events_dir = Path(events_dir_raw) if Path(events_dir_raw).is_absolute() else BASE_DIR / events_dir_raw
        self._events_current_day: date | None = None
        self._events_file: io.TextIOWrapper | None = None
        self._corruption_log_path = self.events_dir / "_meta" / "corruption_meta_log.jsonl"

        # Master data (loaded once)
        self.suppliers = load_json(self.data_dir / "suppliers.json")
        self.parts = load_json(self.data_dir / "parts.json")
        self.bom = load_json(self.data_dir / "bom.json")
        self.customers = load_json(self.data_dir / "customers.json")

        # Dynamic state
        self.inventory = load_json(self.data_dir / "inventory.json")
        self.production_schedule = load_json(self.data_dir / "production_schedule.json")

        # Index master data for quick lookup
        self.parts_by_id = {p["part_id"]: p for p in self.parts if isinstance(p, dict) and "part_id" in p}
        self.suppliers_by_id = {s["id"]: s for s in self.suppliers if isinstance(s, dict) and "id" in s}

        # Pending purchase orders awaiting delivery
        self._pending_purchase_orders: list[PendingPurchaseOrder] = []
        
        # Pending backorders awaiting fulfillment
        self._pending_backorders: list[PendingBackorder] = []
        
        # Track parts with pending reorders to avoid duplicate POs
        self._parts_on_order: set[str] = set()
        
        # Cost drift tracking (random walk for each part)
        self._cost_drift: dict[str, float] = {}  # part_id -> drift multiplier (-0.2 to +0.2)
        self._last_cost_drift_day: int = -1  # Track last day we applied drift
        
        # Black swan event (only for 5-year historical generation)
        self._black_swan_event: BlackSwanEvent | None = None
        if include_black_swan and simulation_years >= 5:
            self._black_swan_event = self._generate_black_swan_event(simulation_years)
            if self._black_swan_event:
                # Log the black swan event at startup
                self._log_black_swan_start()

        self._ensure_schedule_shape()
        self._ensure_inventory_shape()

        atexit.register(self.save_state)
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        cfg = self.config
        errors = []
        
        if cfg["demand_probability_base"] < 0 or cfg["demand_probability_base"] > 1:
            errors.append("demand_probability_base must be between 0 and 1")
        if cfg["demand_probability_business_hours"] < 0 or cfg["demand_probability_business_hours"] > 1:
            errors.append("demand_probability_business_hours must be between 0 and 1")
        if cfg["business_hours_start"] < 0 or cfg["business_hours_start"] > 23:
            errors.append("business_hours_start must be between 0 and 23")
        if cfg["business_hours_end"] < 0 or cfg["business_hours_end"] > 23:
            errors.append("business_hours_end must be between 0 and 23")
        if cfg["production_duration_hours_min"] <= 0:
            errors.append("production_duration_hours_min must be positive")
        if cfg["production_duration_hours_max"] < cfg["production_duration_hours_min"]:
            errors.append("production_duration_hours_max must be >= min")
        if cfg["partial_shipment_probability"] < 0 or cfg["partial_shipment_probability"] > 1:
            errors.append("partial_shipment_probability must be between 0 and 1")
            
        if errors:
            raise ConfigValidationError("Invalid configuration: " + "; ".join(errors))

    def _ensure_schedule_shape(self) -> None:
        """Ensure production_schedule has the expected structure."""
        if not isinstance(self.production_schedule, dict):
            self.production_schedule = {}
        self.production_schedule.setdefault("active_jobs", [])

    def _ensure_inventory_shape(self) -> None:
        """Ensure inventory has the expected structure."""
        if not isinstance(self.inventory, dict):
            self.inventory = {}

    def _log_event(self, event_type: str, payload: dict[str, Any]) -> None:
        """Log an event to date-partitioned JSONL (data/events/YYYY-MM-DD.jsonl)."""
        event = {
            "timestamp": iso_utc(self.current_time),
            "event_type": event_type,
            "payload": payload,
        }
        self._log_event_to_json(event)

    def _log_event_to_json(self, event: dict[str, Any]) -> None:
        """Append an event to the date-partitioned JSONL file for the current simulation day."""
        json_line = json.dumps(event, ensure_ascii=False)
        if (self.config.get("data_corruption_enabled", False) and
            self.rng.random() < self.config.get("data_corruption_probability", 0.01)):
            json_line, corruption_type = self._corrupt_json_line(json_line, event["event_type"])
            self._log_corruption_meta(event["event_type"], corruption_type)
        try:
            self.events_dir.mkdir(parents=True, exist_ok=True)
            day = self.current_time.date()
            if self._events_current_day != day:
                if self._events_file is not None:
                    self._events_file.close()
                    self._events_file = None
                self._events_current_day = day
                path = self.events_dir / f"{day:%Y-%m-%d}.jsonl"
                self._events_file = path.open("a", encoding="utf-8")
            self._events_file.write(json_line + "\n")
            self._events_file.flush()
        except IOError as e:
            import sys
            print(f"Warning: Failed to write event log: {e}", file=sys.stderr)

    def _corrupt_json_line(self, json_line: str, event_type: str) -> tuple[str, str]:
        """
        Corrupt a JSON line in various ways for error handling practice.
        
        Returns (corrupted_line, corruption_type)
        """
        corruption_methods = [
            self._corrupt_invalid_timestamp,
            self._corrupt_missing_comma,
            self._corrupt_truncated_line,
            self._corrupt_wrong_type,
            self._corrupt_null_injection,
        ]
        
        method = self.rng.choice(corruption_methods)
        return method(json_line)

    def _corrupt_invalid_timestamp(self, json_line: str) -> tuple[str, str]:
        """Replace timestamp with invalid date."""
        invalid_timestamps = [
            '"2026-02-30T12:00:00Z"',  # Feb 30 doesn't exist
            '"2026-13-01T12:00:00Z"',  # Month 13
            '"2026-01-01T25:00:00Z"',  # Hour 25
            '"2026-01-01T12:60:00Z"',  # Minute 60
            '"not-a-date"',
            '""',
        ]
        invalid = self.rng.choice(invalid_timestamps)
        # Replace the timestamp value
        corrupted = re.sub(r'"timestamp":\s*"[^"]*"', f'"timestamp": {invalid}', json_line)
        return corrupted, "invalid_timestamp"

    def _corrupt_missing_comma(self, json_line: str) -> tuple[str, str]:
        """Remove a comma from the JSON."""
        # Find positions of commas and remove one
        comma_positions = [i for i, c in enumerate(json_line) if c == ',']
        if comma_positions:
            pos = self.rng.choice(comma_positions)
            corrupted = json_line[:pos] + json_line[pos+1:]
            return corrupted, "missing_comma"
        return json_line, "missing_comma_failed"

    def _corrupt_truncated_line(self, json_line: str) -> tuple[str, str]:
        """Truncate the line at a random point."""
        # Cut somewhere between 30% and 80% of the line
        cut_point = int(len(json_line) * self.rng.uniform(0.3, 0.8))
        return json_line[:cut_point], "truncated_line"

    def _corrupt_wrong_type(self, json_line: str) -> tuple[str, str]:
        """Replace a number with a string or vice versa."""
        # Find a number and replace with string
        corrupted = re.sub(r'"qty":\s*(\d+)', '"qty": "not_a_number"', json_line, count=1)
        if corrupted != json_line:
            return corrupted, "wrong_type_qty"
        # Or replace a string field with a number
        corrupted = re.sub(r'"order_id":\s*"[^"]*"', '"order_id": 12345', json_line, count=1)
        if corrupted != json_line:
            return corrupted, "wrong_type_order_id"
        return json_line, "wrong_type_failed"

    def _corrupt_null_injection(self, json_line: str) -> tuple[str, str]:
        """Replace a value with null where it shouldn't be."""
        fields_to_null = [
            (r'"customer_id":\s*"[^"]*"', '"customer_id": null'),
            (r'"supplier_id":\s*"[^"]*"', '"supplier_id": null'),
            (r'"product_id":\s*"[^"]*"', '"product_id": null'),
            (r'"part_id":\s*"[^"]*"', '"part_id": null'),
        ]
        pattern, replacement = self.rng.choice(fields_to_null)
        corrupted = re.sub(pattern, replacement, json_line, count=1)
        return corrupted, "null_injection"

    def _log_corruption_meta(self, event_type: str, corruption_type: str) -> None:
        """Log corruption to separate meta file for verification."""
        meta_event = {
            "timestamp": iso_utc(self.current_time),
            "corrupted_event_type": event_type,
            "corruption_type": corruption_type,
        }
        try:
            self._corruption_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._corruption_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(meta_event, ensure_ascii=False) + "\n")
        except IOError:
            pass  # Silent fail for meta log

    def tick(self) -> None:
        """
        Advance simulation by one hour.
        
        Each tick:
        1. Advance time
        2. Check for black swan event transitions
        3. Apply daily cost drift (once per day)
        4. Process incoming purchase orders (receive parts)
        5. Try to fulfill pending backorders
        6. Check reorder points and trigger automatic POs
        7. Generate customer demand
        8. Run production (start jobs, complete jobs)
        """
        self.current_time += timedelta(hours=1)
        self.tick_count += 1
        self._check_black_swan_events()
        self._apply_daily_cost_drift()
        self._process_pending_purchase_orders()
        self._process_pending_backorders()
        self._check_reorder_points()
        self.generate_demand()
        self.run_production()

    def _is_business_hours(self) -> bool:
        """Check if current simulation time is within business hours."""
        hour = self.current_time.hour
        start = self.config["business_hours_start"]
        end = self.config["business_hours_end"]
        return start <= hour < end

    def _is_end_of_quarter(self) -> bool:
        """Check if we're in end-of-quarter rush period (last 10 days of quarter)."""
        month = self.current_time.month
        day = self.current_time.day
        return month in [3, 6, 9, 12] and day >= 20

    def _get_day_of_week_factor(self) -> float:
        """Get demand multiplier based on day of week (Friday rush, weekend lull)."""
        if not self.config.get("seasonality_enabled", True):
            return 1.0
        
        day_of_week = self.current_time.weekday()  # 0=Monday, 6=Sunday
        base_factor = DAY_OF_WEEK_DEMAND.get(day_of_week, 1.0)
        
        # Apply strength modifier
        strength = self.config.get("demand_seasonality_strength", 1.0)
        return 1.0 + (base_factor - 1.0) * strength

    def _get_period_end_factor(self) -> float:
        """Get demand multiplier for month-end and quarter-end spikes.
        
        Month-end (last 3 days): 20% boost - financial closing pressure
        Quarter-end (Mar, Jun, Sep, Dec, last 5 days): additional 15% boost
        """
        if not self.config.get("seasonality_enabled", True):
            return 1.0
        
        day = self.current_time.day
        month = self.current_time.month
        factor = 1.0
        
        # Last 3 days of month: 20% boost (financial month-end pressure)
        # Using day >= 28 as approximation for "last 3 days"
        if day >= 28:
            factor = 1.2
        
        # Quarter-end months (Mar, Jun, Sep, Dec) get extra 15% in last 5 days
        if month in (3, 6, 9, 12) and day >= 26:
            factor *= 1.15
        
        return factor

    def _get_demand_seasonality_factor(self) -> float:
        """Get combined demand multiplier from all seasonality factors.
        
        Combines:
        - Monthly seasonality (e.g., November peak, January slump)
        - Day-of-week effects (Friday rush, weekend lull)
        - Period-end spikes (month-end, quarter-end)
        - Black swan events (major disruptions)
        """
        if not self.config.get("seasonality_enabled", True):
            return 1.0
        
        strength = self.config.get("demand_seasonality_strength", 1.0)
        month = self.current_time.month
        base_factor = DEMAND_SEASONALITY.get(month, 1.0)
        
        # Apply strength (1.0 = full effect, 0.5 = half effect, 0 = no effect)
        monthly_factor = 1.0 + (base_factor - 1.0) * strength
        
        # Get day-of-week factor
        dow_factor = self._get_day_of_week_factor()
        
        # Get period-end factor (month-end / quarter-end)
        period_factor = self._get_period_end_factor()
        
        # Get black swan factor (if active)
        black_swan_factor = self._get_black_swan_demand_factor()
        
        # Combine all factors multiplicatively
        return monthly_factor * dow_factor * period_factor * black_swan_factor

    def _get_supplier_seasonality_factor(self, supplier_id: str | None) -> dict[str, float]:
        """
        Get lead time and reliability multipliers for a supplier based on seasonality.
        
        Includes:
        - Regular seasonality (holidays, vacation periods)
        - Black swan events (major disruptions)
        
        Returns dict with 'lead_time_mult' and 'reliability_mult'.
        """
        default = {"lead_time_mult": 1.0, "reliability_mult": 1.0}
        
        if not self.config.get("seasonality_enabled", True):
            return default
        
        if not supplier_id:
            return default
        
        supplier = self.suppliers_by_id.get(supplier_id, {})
        country = supplier.get("country")
        
        result = {"lead_time_mult": 1.0, "reliability_mult": 1.0}
        
        # Check regular seasonality
        if country and country in SUPPLIER_SEASONALITY:
            strength = self.config.get("supplier_seasonality_strength", 1.0)
            current_month = self.current_time.month
            current_day = self.current_time.day
            
            for (start_month, start_day), (end_month, end_day), factors in SUPPLIER_SEASONALITY[country]:
                if self._date_in_period(current_month, current_day, 
                                        start_month, start_day, end_month, end_day):
                    result["lead_time_mult"] = 1.0 + (factors["lead_time_mult"] - 1.0) * strength
                    result["reliability_mult"] = 1.0 + (factors["reliability_mult"] - 1.0) * strength
                    break
        
        # Apply black swan effects (multiplicative on top of regular seasonality)
        black_swan_mult = self._get_black_swan_supplier_factor(supplier_id)
        result["lead_time_mult"] *= black_swan_mult
        
        return result

    def _generate_black_swan_event(self, simulation_years: int) -> BlackSwanEvent | None:
        """Generate a random black swan event for historical simulation.
        
        Places the event randomly in years 2-4 of a 5-year simulation,
        avoiding the first year (to establish baseline) and last year
        (to show recovery).
        """
        if simulation_years < 5:
            return None
        
        # Pick a random template
        template = self.rng.choice(BLACK_SWAN_TEMPLATES)
        
        # Calculate the event start date (randomly in years 2-4)
        # Year 1 starts at self.current_time, so year 2 starts 365 days later
        min_offset_days = 365       # Start of year 2
        max_offset_days = 365 * 4   # End of year 4
        
        # Random day offset
        offset_days = self.rng.randint(min_offset_days, max_offset_days)
        start_date = self.current_time + timedelta(days=offset_days)
        
        return BlackSwanEvent(
            name=template["name"],
            start_date=start_date,
            duration_days=template["duration_days"],
            demand_multiplier=template["demand_multiplier"],
            lead_time_multiplier=template["lead_time_multiplier"],
            affected_countries=template["affected_countries"],
        )

    def _log_black_swan_start(self) -> None:
        """Log the creation of a black swan event."""
        if not self._black_swan_event:
            return
        
        self._log_event(
            "BlackSwanEventScheduled",
            {
                "name": self._black_swan_event.name,
                "start_date": iso_utc(self._black_swan_event.start_date),
                "end_date": iso_utc(self._black_swan_event.end_date),
                "duration_days": self._black_swan_event.duration_days,
                "demand_multiplier": self._black_swan_event.demand_multiplier,
                "lead_time_multiplier": self._black_swan_event.lead_time_multiplier,
                "affected_countries": self._black_swan_event.affected_countries,
            }
        )

    def _get_black_swan_demand_factor(self) -> float:
        """Get demand multiplier from active black swan event."""
        if not self._black_swan_event:
            return 1.0
        
        if self._black_swan_event.is_active(self.current_time):
            return self._black_swan_event.demand_multiplier
        
        return 1.0

    def _get_black_swan_supplier_factor(self, supplier_id: str | None) -> float:
        """Get lead time multiplier from active black swan event for a supplier's country."""
        if not self._black_swan_event or not supplier_id:
            return 1.0
        
        if not self._black_swan_event.is_active(self.current_time):
            return 1.0
        
        supplier = self.suppliers_by_id.get(supplier_id, {})
        country = supplier.get("country")
        
        if country and country in self._black_swan_event.affected_countries:
            return self._black_swan_event.lead_time_multiplier
        
        return 1.0

    def _check_black_swan_events(self) -> None:
        """Check and log black swan event start/end transitions."""
        if not self._black_swan_event:
            return
        
        # Check if event just started (within the last hour)
        event_start = self._black_swan_event.start_date
        if event_start <= self.current_time < event_start + timedelta(hours=1):
            self._log_event(
                "BlackSwanEventStarted",
                {
                    "name": self._black_swan_event.name,
                    "affected_countries": self._black_swan_event.affected_countries,
                    "demand_multiplier": self._black_swan_event.demand_multiplier,
                    "lead_time_multiplier": self._black_swan_event.lead_time_multiplier,
                }
            )
        
        # Check if event just ended (within the last hour)
        event_end = self._black_swan_event.end_date
        if event_end <= self.current_time < event_end + timedelta(hours=1):
            self._log_event(
                "BlackSwanEventEnded",
                {
                    "name": self._black_swan_event.name,
                    "duration_days": self._black_swan_event.duration_days,
                }
            )

    def shutdown(self) -> None:
        """Signal the engine to stop (for graceful shutdown in service mode)."""
        self.running = False

    def _date_in_period(self, month: int, day: int, 
                        start_month: int, start_day: int, 
                        end_month: int, end_day: int) -> bool:
        """Check if month/day is within a period (handles year wraparound)."""
        current = (month, day)
        start = (start_month, start_day)
        end = (end_month, end_day)
        
        if start <= end:
            return start <= current <= end
        else:
            # Period wraps around year (e.g., Dec 15 to Jan 6)
            return current >= start or current <= end

    def _apply_daily_cost_drift(self) -> None:
        """
        Apply daily cost drift to parts (random walk).
        Only runs once per simulated day.
        """
        if not self.config.get("cost_drift_enabled", True):
            return
        
        current_day = self.current_time.timetuple().tm_yday
        if current_day == self._last_cost_drift_day:
            return  # Already applied today
        
        self._last_cost_drift_day = current_day
        drift_pct = self.config.get("cost_drift_daily_pct", 0.005)
        max_drift = self.config.get("cost_drift_max_pct", 0.20)
        
        # Apply drift to each part
        for part_id in self.parts_by_id:
            if part_id not in self._cost_drift:
                self._cost_drift[part_id] = 0.0
            
            # Random walk: add or subtract daily drift
            change = self.rng.uniform(-drift_pct, drift_pct)
            self._cost_drift[part_id] += change
            
            # Clamp to max drift
            self._cost_drift[part_id] = max(-max_drift, 
                                            min(max_drift, self._cost_drift[part_id]))

    def _get_current_part_cost(self, part_id: str, supplier_id: str | None = None) -> tuple[float, float, float]:
        """
        Get current cost for a part including drift and supplier pricing.
        
        Returns (unit_cost, base_cost, variance_pct)
        """
        part = self.parts_by_id.get(part_id, {})
        base_cost = part.get("standard_cost", 10.0)
        
        # Apply cost drift
        drift = self._cost_drift.get(part_id, 0.0)
        drifted_cost = base_cost * (1.0 + drift)
        
        # Apply supplier-specific pricing
        supplier_mult = 1.0
        if supplier_id:
            supplier = self.suppliers_by_id.get(supplier_id, {})
            supplier_mult = supplier.get("price_multiplier", 1.0)
        
        unit_cost = drifted_cost * supplier_mult
        variance_pct = ((unit_cost - base_cost) / base_cost) * 100 if base_cost > 0 else 0
        
        return round(unit_cost, 2), round(base_cost, 2), round(variance_pct, 2)

    def _process_pending_backorders(self) -> None:
        """Try to fulfill pending backorders from available inventory."""
        still_pending = []
        
        for backorder in self._pending_backorders:
            stock = self.inventory.get(backorder.product_id, {}).get("qty_on_hand", 0)
            
            if stock <= 0:
                still_pending.append(backorder)
                continue
            
            # Fulfill as much as we can
            qty_to_ship = min(stock, backorder.qty_remaining)
            self.inventory[backorder.product_id]["qty_on_hand"] = stock - qty_to_ship
            backorder.qty_remaining -= qty_to_ship
            
            self._log_event(
                "BackorderFulfilled",
                {
                    "order_id": backorder.order_id,
                    "customer_id": backorder.customer_id,
                    "product_id": backorder.product_id,
                    "qty_shipped": qty_to_ship,
                    "qty_still_pending": backorder.qty_remaining,
                    "original_order_qty": backorder.original_qty,
                    "remaining_stock": self.inventory[backorder.product_id]["qty_on_hand"],
                },
            )
            
            # If still has remaining, keep in pending
            if backorder.qty_remaining > 0:
                still_pending.append(backorder)
        
        self._pending_backorders = still_pending

    def _get_demand_probability(self) -> float:
        """Get demand probability based on time of day and seasonality."""
        if self._is_business_hours():
            base_prob = self.config["demand_probability_business_hours"]
        else:
            base_prob = self.config["demand_probability_base"]
        
        # Apply seasonality
        seasonal_factor = self._get_demand_seasonality_factor()
        return base_prob * seasonal_factor

    def generate_demand(self) -> None:
        """
        Generate customer demand with realistic variability.
        
        - Higher probability during business hours
        - Occasional bulk orders
        - Customer-specific patterns (Tier 1 customers order more frequently)
        """
        demand_prob = self._get_demand_probability()
        
        if self.rng.random() >= demand_prob:
            return

        # Select customer with bias toward Tier 1 customers (they order more)
        tier1_customers = [c for c in self.customers if c.get("contract_priority") == "Tier 1"]
        tier2_customers = [c for c in self.customers if c.get("contract_priority") != "Tier 1"]
        
        # Tier 1 customers have 60% chance of being selected if they exist
        if tier1_customers and self.rng.random() < 0.6:
            customer = self.rng.choice(tier1_customers)
        elif tier2_customers:
            customer = self.rng.choice(tier2_customers)
        else:
            customer = self.rng.choice(self.customers)

        # Determine order quantity - occasional bulk orders
        if self.rng.random() < self.config["bulk_order_probability"]:
            qty = self.rng.randint(
                self.config["bulk_order_qty_min"],
                self.config["bulk_order_qty_max"]
            )
        else:
            qty = self.rng.randint(
                self.config["normal_order_qty_min"],
                self.config["normal_order_qty_max"]
            )

        order = SalesOrder(
            order_id=str(uuid.uuid4()),
            customer_id=customer["customer_id"],
            product_id="DRONE-X1",
            qty=qty,
            created_at=iso_utc(self.current_time),
        )
        self._log_event(
            "SalesOrderCreated",
            {
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "product_id": order.product_id,
                "qty": order.qty,
            },
        )
        self.check_inventory(order)

    def check_inventory(self, order: SalesOrder) -> None:
        """
        Check if order can be fulfilled from stock.
        
        - Full stock: Ship everything
        - Partial stock: Ship what we have, backorder the rest
        - No stock: Backorder entire order, create production job
        """
        stock = self.inventory.get(order.product_id, {}).get("qty_on_hand", 0)
        
        if stock >= order.qty:
            # Full fulfillment
            self.inventory[order.product_id]["qty_on_hand"] = stock - order.qty
            self._log_event(
                "ShipmentCreated",
                {
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "product_id": order.product_id,
                    "qty": order.qty,
                    "qty_ordered": order.qty,
                    "fulfillment_type": "full",
                    "remaining_stock": self.inventory[order.product_id]["qty_on_hand"],
                },
            )
            return
        
        if stock > 0:
            # Partial fulfillment - ship what we have, backorder the rest
            qty_shipped = stock
            qty_backordered = order.qty - stock
            
            self.inventory[order.product_id]["qty_on_hand"] = 0
            
            self._log_event(
                "PartialShipmentCreated",
                {
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "product_id": order.product_id,
                    "qty_shipped": qty_shipped,
                    "qty_backordered": qty_backordered,
                    "qty_ordered": order.qty,
                    "remaining_stock": 0,
                },
            )
            
            # Create backorder for remaining
            self._create_backorder(order, qty_backordered)
        else:
            # No stock - full backorder
            self._log_event(
                "BackorderCreated",
                {
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "product_id": order.product_id,
                    "qty_backordered": order.qty,
                    "original_order_qty": order.qty,
                    "reason": "no_stock",
                },
            )
            
            self._create_backorder(order, order.qty)
        
        # Ensure we have production to fulfill backorders
        self.create_production_job(product_id=order.product_id)

    def _create_backorder(self, order: SalesOrder, qty: int) -> None:
        """Create a pending backorder entry."""
        backorder = PendingBackorder(
            order_id=order.order_id,
            customer_id=order.customer_id,
            product_id=order.product_id,
            qty_remaining=qty,
            original_qty=order.qty,
            created_at=self.current_time,
        )
        self._pending_backorders.append(backorder)

    def create_production_job(self, *, product_id: str) -> None:
        """Create a new production job for the specified product."""
        # Calculate production duration based on config
        duration_hours = self.rng.randint(
            self.config["production_duration_hours_min"],
            self.config["production_duration_hours_max"]
        )
        
        job = {
            "job_id": str(uuid.uuid4()),
            "product_id": product_id,
            "status": "Planned",
            "created_at": iso_utc(self.current_time),
            "start_date": None,  # Set when production actually starts
            "due_date": iso_utc(self.current_time + timedelta(days=3)),
            "expected_completion": None,  # Set when production starts
            "production_duration_hours": duration_hours,
            "assigned_worker_id": f"WORKER-{self.rng.randint(1, 25):03d}",
        }
        self.production_schedule["active_jobs"].append(job)
        self._log_event(
            "ProductionJobCreated",
            {
                "job_id": job["job_id"],
                "product_id": product_id,
                "status": job["status"],
                "production_duration_hours": duration_hours,
            },
        )

    def _check_reorder_points(self) -> None:
        """
        Check inventory levels against reorder points and trigger automatic POs.
        
        Uses the reorder_point and safety_stock fields from inventory data.
        """
        for part_id, inv_data in self.inventory.items():
            if not isinstance(inv_data, dict):
                continue
            
            # Skip finished goods (only reorder parts)
            if part_id not in self.parts_by_id:
                continue
                
            # Skip if already on order
            if part_id in self._parts_on_order:
                continue
            
            qty_on_hand = inv_data.get("qty_on_hand", 0)
            reorder_point = inv_data.get("reorder_point", 0)
            safety_stock = inv_data.get("safety_stock", 0)
            
            if qty_on_hand <= reorder_point:
                # Order enough to get back above reorder point + safety buffer
                order_qty = (reorder_point - qty_on_hand) + safety_stock + 50
                
                self._log_event(
                    "ReorderTriggered",
                    {
                        "part_id": part_id,
                        "qty_on_hand": qty_on_hand,
                        "reorder_point": reorder_point,
                        "order_qty": order_qty,
                    },
                )
                
                self.order_parts_from_supplier(part_id=part_id, qty=order_qty, is_reorder=True)

    def _process_pending_purchase_orders(self) -> None:
        """
        Process pending purchase orders - receive parts when ETA is reached.
        
        Applies supplier reliability for:
        - Partial shipments (unreliable suppliers may deliver less)
        - Quality rejections (incoming inspection based on supplier quality)
        """
        still_pending = []
        
        for po in self._pending_purchase_orders:
            if self.current_time >= po.eta:
                # PO has arrived - process it
                self._receive_purchase_order(po)
                # Remove from on-order tracking
                self._parts_on_order.discard(po.part_id)
            else:
                still_pending.append(po)
        
        self._pending_purchase_orders = still_pending

    def _receive_purchase_order(self, po: PendingPurchaseOrder) -> None:
        """
        Receive a purchase order into inventory.
        
        Applies supplier reliability factors:
        - Partial shipments for unreliable suppliers
        - Quality rejections based on supplier reliability
        """
        supplier = self.suppliers_by_id.get(po.supplier_id, {}) if po.supplier_id else {}
        reliability = supplier.get("reliability_score", 0.9)
        
        received_qty = po.qty
        is_partial = False
        
        # Partial shipment check - more likely for unreliable suppliers
        # Probability scales inversely with reliability
        partial_prob = self.config["partial_shipment_probability"] * (1.1 - reliability)
        if self.rng.random() < partial_prob:
            pct = self.rng.uniform(
                self.config["partial_shipment_min_pct"],
                self.config["partial_shipment_max_pct"]
            )
            received_qty = po.qty * pct
            is_partial = True
            
            self._log_event(
                "PartialShipment",
                {
                    "purchase_order_id": po.purchase_order_id,
                    "part_id": po.part_id,
                    "ordered_qty": po.qty,
                    "received_qty": received_qty,
                    "supplier_id": po.supplier_id,
                    "shortfall_pct": round((1 - pct) * 100, 1),
                },
            )
        
        # Quality rejection check - rate inversely proportional to reliability
        reject_rate_base = self.rng.uniform(
            self.config["quality_reject_rate_min"],
            self.config["quality_reject_rate_max"]
        )
        # Less reliable suppliers have higher reject rates
        reject_rate = reject_rate_base * (1.2 - reliability)
        
        qty_rejected = 0
        if self.rng.random() < 0.3:  # 30% chance of any quality issues
            qty_rejected = int(received_qty * reject_rate)
            if qty_rejected > 0:
                received_qty -= qty_rejected
                self._log_event(
                    "QualityRejection",
                    {
                        "purchase_order_id": po.purchase_order_id,
                        "part_id": po.part_id,
                        "qty_rejected": qty_rejected,
                        "supplier_id": po.supplier_id,
                        "reject_rate_pct": round(reject_rate * 100, 2),
                    },
                )
        
        # Add to inventory
        if po.part_id not in self.inventory:
            self.inventory[po.part_id] = {"qty_on_hand": 0, "reorder_point": 50, "safety_stock": 20}
        
        self.inventory[po.part_id]["qty_on_hand"] = (
            self.inventory[po.part_id].get("qty_on_hand", 0) + int(received_qty)
        )
        
        # Log receipt event
        self._log_event(
            "PurchaseOrderReceived",
            {
                "purchase_order_id": po.purchase_order_id,
                "part_id": po.part_id,
                "qty_ordered": po.qty,
                "qty_received": int(received_qty),
                "qty_rejected": qty_rejected,
                "supplier_id": po.supplier_id,
                "was_partial_shipment": is_partial,
                "new_qty_on_hand": self.inventory[po.part_id]["qty_on_hand"],
            },
        )

    def _bom_components_for_product(self, product_id: str) -> list[dict[str, Any]]:
        """
        Get BOM components for a specific product.
        
        Fixed: Now filters by product_id instead of returning all components.
        """
        # Check if BOM is for the requested product
        if self.bom.get("product_id") != product_id:
            return []
        
        components: list[dict[str, Any]] = []
        for item in self.bom.get("bom", []):
            if not isinstance(item, dict):
                continue
            components.extend(item.get("components", []))
        return components

    def run_production(self) -> None:
        """
        Run production logic for all active jobs.
        
        - Planned jobs: Check for parts, start if available, order if not
        - WIP jobs: Check if production duration has elapsed, complete if so
        """
        completed_job_ids = set()
        
        for job in self.production_schedule.get("active_jobs", []):
            status = job.get("status")
            product_id = job.get("product_id", "DRONE-X1")
            
            if status == "Planned":
                # Try to start production
                missing = self._missing_parts_for_job(product_id)
                if not missing:
                    self._consume_parts_for_job(product_id)
                    job["status"] = "WIP"
                    job["start_date"] = iso_utc(self.current_time)
                    
                    # Calculate expected completion
                    duration = job.get("production_duration_hours", 
                                      self.rng.randint(
                                          self.config["production_duration_hours_min"],
                                          self.config["production_duration_hours_max"]
                                      ))
                    completion_time = self.current_time + timedelta(hours=duration)
                    job["expected_completion"] = iso_utc(completion_time)
                    
                    self._log_event(
                        "ProductionStarted",
                        {
                            "job_id": job["job_id"],
                            "product_id": product_id,
                            "status": job["status"],
                            "expected_completion": job["expected_completion"],
                        },
                    )
                else:
                    for part_id, qty_needed in missing.items():
                        # Only order if not already on order
                        if part_id not in self._parts_on_order:
                            self.order_parts_from_supplier(part_id=part_id, qty=qty_needed)
                            
            elif status == "WIP":
                # Check if production is complete
                expected_str = job.get("expected_completion")
                if expected_str:
                    # Parse the expected completion time
                    expected = datetime.fromisoformat(expected_str.replace("Z", "+00:00"))
                    if self.current_time >= expected:
                        # Production complete - add finished goods to inventory
                        self._complete_production_job(job)
                        completed_job_ids.add(job["job_id"])
        
        # Remove completed jobs from active list
        self.production_schedule["active_jobs"] = [
            job for job in self.production_schedule["active_jobs"]
            if job["job_id"] not in completed_job_ids
        ]

    def _complete_production_job(self, job: dict) -> None:
        """Complete a production job and add finished goods to inventory."""
        product_id = job.get("product_id", "DRONE-X1")
        
        # Add 1 unit of finished product to inventory
        if product_id not in self.inventory:
            self.inventory[product_id] = {"qty_on_hand": 0, "reorder_point": 15, "safety_stock": 5}
        
        self.inventory[product_id]["qty_on_hand"] = (
            self.inventory[product_id].get("qty_on_hand", 0) + 1
        )
        
        job["status"] = "Completed"
        job["actual_completion"] = iso_utc(self.current_time)
        
        self._log_event(
            "ProductionCompleted",
            {
                "job_id": job["job_id"],
                "product_id": product_id,
                "status": job["status"],
                "new_qty_on_hand": self.inventory[product_id]["qty_on_hand"],
            },
        )

    def _missing_parts_for_job(self, product_id: str) -> dict[str, float]:
        """
        Check which parts are missing for a production job.
        
        Fixed: Now takes product_id parameter to filter BOM correctly.
        """
        missing: dict[str, float] = {}
        for comp in self._bom_components_for_product(product_id):
            part_id = comp.get("component_id")
            qty = comp.get("qty", 0)
            if not part_id:
                continue
            on_hand = self.inventory.get(part_id, {}).get("qty_on_hand", 0)
            if on_hand < qty:
                missing[part_id] = qty - on_hand
        return missing

    def _consume_parts_for_job(self, product_id: str) -> None:
        """
        Consume parts from inventory for a production job.
        
        Fixed: Now takes product_id parameter to filter BOM correctly.
        """
        for comp in self._bom_components_for_product(product_id):
            part_id = comp.get("component_id")
            qty = comp.get("qty", 0)
            if not part_id:
                continue
            entry = self.inventory.get(part_id)
            if not entry:
                continue
            entry["qty_on_hand"] = max(0, entry.get("qty_on_hand", 0) - qty)

    def order_parts_from_supplier(self, *, part_id: str, qty: float, is_reorder: bool = False) -> None:
        """
        Create a purchase order for parts.
        
        Lead time is affected by:
        - Supplier reliability (less reliable = longer lead times)
        - Seasonality (holidays = longer lead times)
        
        Cost is affected by:
        - Commodity price drift (random walk over time)
        - Supplier-specific pricing (price_multiplier)
        """
        part = self.parts_by_id.get(part_id, {})
        suppliers = part.get("valid_supplier_ids") or []
        supplier_id = self.rng.choice(suppliers) if suppliers else None
        
        # Get supplier info
        supplier = self.suppliers_by_id.get(supplier_id, {}) if supplier_id else {}
        reliability = supplier.get("reliability_score", 0.9)
        country = supplier.get("country", "Unknown")
        
        # Get seasonality factors
        seasonal = self._get_supplier_seasonality_factor(supplier_id)
        
        # Apply seasonality to reliability
        effective_reliability = reliability * seasonal["reliability_mult"]
        
        # Calculate lead time based on reliability
        base_min = self.config["base_lead_time_hours_min"]
        base_max = self.config["base_lead_time_hours_max"]
        
        # Unreliable suppliers have longer, more variable lead times
        reliability_factor = 1.1 - effective_reliability  # Higher = worse
        
        adjusted_min = int(base_min + (base_max - base_min) * reliability_factor * 0.5)
        adjusted_max = int(base_min + (base_max - base_min) * reliability_factor * 1.5)
        adjusted_max = min(adjusted_max, base_max * 2)
        
        # Apply seasonality lead time multiplier
        adjusted_min = int(adjusted_min * seasonal["lead_time_mult"])
        adjusted_max = int(adjusted_max * seasonal["lead_time_mult"])
        
        lead_time_hours = self.rng.randint(adjusted_min, max(adjusted_min + 1, adjusted_max))
        eta = self.current_time + timedelta(hours=lead_time_hours)
        
        # Calculate cost with drift and supplier pricing
        unit_cost, base_cost, cost_variance_pct = self._get_current_part_cost(part_id, supplier_id)
        total_cost = round(unit_cost * qty, 2)
        
        po_id = str(uuid.uuid4())
        
        # Track the pending PO for later receipt
        pending_po = PendingPurchaseOrder(
            purchase_order_id=po_id,
            part_id=part_id,
            qty=qty,
            supplier_id=supplier_id,
            eta=eta,
            created_at=self.current_time,
            unit_cost=unit_cost,
        )
        self._pending_purchase_orders.append(pending_po)
        self._parts_on_order.add(part_id)

        self._log_event(
            "PurchaseOrderCreated",
            {
                "purchase_order_id": po_id,
                "part_id": part_id,
                "qty": qty,
                "supplier_id": supplier_id,
                "supplier_country": country,
                "supplier_reliability": reliability,
                "effective_reliability": round(effective_reliability, 3),
                "lead_time_hours": lead_time_hours,
                "eta": iso_utc(eta),
                "is_reorder": is_reorder,
                # Cost fields for margin analysis
                "unit_cost": unit_cost,
                "total_cost": total_cost,
                "base_cost": base_cost,
                "cost_variance_pct": cost_variance_pct,
                # Seasonality info
                "seasonal_lead_time_mult": seasonal["lead_time_mult"],
                "seasonal_reliability_mult": seasonal["reliability_mult"],
            },
        )

    def save_state(self) -> None:
        """Persist dynamic state to disk on exit. Close events file handle if open."""
        if self._events_file is not None:
            try:
                self._events_file.close()
            except IOError:
                pass
            self._events_file = None
            self._events_current_day = None
        try:
            (self.data_dir / "inventory.json").write_text(
                json.dumps(self.inventory, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            (self.data_dir / "production_schedule.json").write_text(
                json.dumps(self.production_schedule, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        except IOError as e:
            import sys
            print(f"Warning: Failed to save state: {e}", file=sys.stderr)

