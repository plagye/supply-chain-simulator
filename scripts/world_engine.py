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
    - Events: single JSONL file for historical generation (data/events/history.jsonl), date-partitioned JSONL (YYYY-MM-DD.jsonl) for simulate and run-service

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
    "production_batch_size_min": 5,
    "production_batch_size_max": 15,
    "finished_goods_reorder_enabled": True,
    "finished_goods_max_jobs_per_product_per_day": 2,
    # Supplier/procurement settings
    "base_lead_time_hours_min": 24,
    "base_lead_time_hours_max": 168,
    "partial_shipment_probability": 0.15,
    "partial_shipment_min_pct": 0.80,
    "partial_shipment_max_pct": 0.95,
    "quality_reject_rate_min": 0.01,
    "quality_reject_rate_max": 0.05,
    "supplier_lead_time_variance_hours": 48,
    # Cost variation settings
    "cost_drift_enabled": True,
    "cost_drift_daily_pct": 0.005,
    "cost_drift_max_pct": 0.20,
    # Seasonality settings
    "seasonality_enabled": True,
    "demand_seasonality_strength": 1.0,
    "supplier_seasonality_strength": 1.0,
    # Invoicing & payment (Order to Cash)
    "invoice_enabled": True,
    "invoice_payment_days_min": 14,
    "invoice_payment_days_max": 30,
    "payment_late_probability": 0.1,
    "payment_late_days_extra": 5,
    "default_unit_price": 1250.0,
    # Forecasting (Tactical)
    "forecast_enabled": True,
    "forecast_horizon_days": 7,
    "forecast_window_days": 14,
    "forecast_bias_correction_mult": 1.15,
    # Requirement planning (MRP-style)
    "requirements_lead_time_days": 30,
    # S&OP snapshots (Tactical)
    "sop_enabled": True,
    "sop_frequency": "monthly",
    # Active demand management (promos)
    "promo_enabled": True,
    "promo_probability": 0.05,
    "promo_duration_days": 7,
    "promo_demand_multiplier_min": 1.2,
    "promo_demand_multiplier_max": 1.8,
    # Delivery (loads and delivery events)
    "delivery_enabled": True,
    "delivery_transit_delay_max_hours": 12,
    "delivery_disruption_probability": 0.05,
    "delivery_disruption_days_min": 1,
    "delivery_disruption_days_max": 3,
    "delivery_grace_hours": 12,
    "load_consolidation_enabled": True,
    "load_weight_limit_lbs": 500,
    "load_flush_days": 3,
    "drone_x1_weight_lbs": 5.0,
    "drone_x1_pieces_per_unit": 1,
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


def load_json_or_default(path: Path, default: Any) -> Any:
    """Load JSON file if it exists; otherwise return default."""
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


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
    actual_arrival: datetime | None = None  # ETA + stochastic variance (when set, receive at this time)


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
class PendingInvoice:
    """Tracks an invoice awaiting payment."""
    invoice_id: str
    order_id: str
    customer_id: str
    product_id: str
    qty: int
    amount: float
    currency: str
    due_date: datetime


@dataclass
class ReadyForShippingItem:
    """One fulfilled line waiting to be consolidated into a load."""
    order_id: str
    customer_id: str
    product_id: str
    qty: int
    destination_facility_id: str
    ready_at: datetime


@dataclass
class PendingDelivery:
    """Tracks a load scheduled for delivery (Pickup/Delivery events when due)."""
    load_id: str
    order_id: str  # Primary (first order; for backward compat)
    order_ids: list[str]  # All orders on this load (consolidated)
    customer_id: str
    route_id: str
    product_id: str
    qty: int
    weight_lbs: float
    pieces: int
    scheduled_pickup: datetime
    scheduled_delivery: datetime
    actual_delivery: datetime  # When delivery actually occurs (may be late)
    origin_facility_id: str
    destination_facility_id: str


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


# Templates for black swan events (used when generating 3-year history)
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
        events_single_file: bool = False,
        events_single_file_path: Path | None = None,
    ) -> None:
        self.data_dir = data_dir or DATA_DIR
        self.rng = random.Random(seed)
        self.current_time = start_time or datetime.now(timezone.utc)
        self.tick_count = 0
        self.running = True  # For service mode graceful shutdown
        
        # Merge provided config with defaults
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._validate_config()

        # Events: either single file (historical) or date-partitioned (run-service / simulate)
        self._events_single_file = events_single_file
        self._events_single_file_path = events_single_file_path
        events_dir_raw = self.config.get("events_dir") or os.environ.get("EVENTS_DIR", "data/events")
        self.events_dir = Path(events_dir_raw) if Path(events_dir_raw).is_absolute() else BASE_DIR / events_dir_raw
        self._events_current_day: date | None = None
        self._events_file: io.TextIOWrapper | None = None

        # Master data (loaded once)
        self.suppliers = load_json(self.data_dir / "suppliers.json")
        self.parts = load_json(self.data_dir / "parts.json")
        self.bom = load_json(self.data_dir / "bom.json")
        self.bom_by_product: dict[str, list[dict[str, Any]]] = {}
        if isinstance(self.bom, dict) and "products" in self.bom:
            for pid, pdata in self.bom.get("products", {}).items():
                if not isinstance(pdata, dict):
                    continue
                comps: list[dict[str, Any]] = []
                for item in pdata.get("bom", []):
                    if isinstance(item, dict):
                        comps.extend(item.get("components", []))
                self.bom_by_product[pid] = comps
        else:
            # Legacy single-product format
            pid = self.bom.get("product_id") if isinstance(self.bom, dict) else None
            if pid:
                comps = []
                for item in (self.bom.get("bom", []) if isinstance(self.bom, dict) else []):
                    if isinstance(item, dict):
                        comps.extend(item.get("components", []))
                self.bom_by_product[pid] = comps
        self.customers = load_json(self.data_dir / "customers.json")
        self.facilities = load_json_or_default(self.data_dir / "facilities.json", [])
        products_data = load_json_or_default(self.data_dir / "products.json", [])
        if isinstance(products_data, list):
            self.product_ids = [p.get("product_id") for p in products_data if isinstance(p, dict) and p.get("product_id")]
        else:
            self.product_ids = []
        if not self.product_ids and self.bom_by_product:
            self.product_ids = list(self.bom_by_product.keys())
        if not self.product_ids:
            self.product_ids = ["D-101"]
        routes_data = load_json_or_default(self.data_dir / "routes.json", {"inbound": [], "outbound": []})
        self.routes_inbound = routes_data.get("inbound", [])
        self.routes_outbound = routes_data.get("outbound", [])

        # Dynamic state
        self.inventory = load_json(self.data_dir / "inventory.json")
        self.production_schedule = load_json_or_default(
            self.data_dir / "production_schedule.json",
            {"active_jobs": []},
        )

        # Index master data for quick lookup
        self.parts_by_id = {p["part_id"]: p for p in self.parts if isinstance(p, dict) and "part_id" in p}
        self.suppliers_by_id = {s["id"]: s for s in self.suppliers if isinstance(s, dict) and "id" in s}

        # Pending purchase orders awaiting delivery
        self._pending_purchase_orders: list[PendingPurchaseOrder] = []
        
        # Pending backorders awaiting fulfillment
        self._pending_backorders: list[PendingBackorder] = []
        
        # Pending invoices awaiting payment (Order to Cash)
        self._pending_invoices: list[PendingInvoice] = []
        
        # Demand history for forecasting: list of (date, product_id, qty) or dict by (date, product_id)
        self._demand_history: list[tuple[date, str, int]] = []
        self._last_forecast_date: date | None = None  # last sim date we emitted forecast
        
        # Allocation: FIFO queue of (job_id, qty) per product for traceability
        self._finished_good_sources: dict[str, list[tuple[str, int]]] = {}
        
        # S&OP: last period we emitted a snapshot (first day of month or week)
        self._last_sop_period: tuple[int, int] | None = None  # (year, month) or (year, week)
        self._last_forecast_by_product: dict[str, float] = {}  # for S&OP snapshot
        
        # CTC: last month we emitted metrics (year, month)
        self._last_ctc_month: tuple[int, int] | None = None
        
        # Active promos: list of {promo_id, end_time, multiplier}
        self._active_promos: list[dict[str, Any]] = []
        
        # Pending deliveries (loads awaiting delivery completion)
        self._pending_deliveries: list[PendingDelivery] = []
        
        # Ready-for-shipping staging (for load consolidation)
        self._ready_for_shipping: list[ReadyForShippingItem] = []
        
        # Track parts with pending reorders to avoid duplicate POs
        self._parts_on_order: set[str] = set()
        
        # Throttle: FG jobs created per product per day (reset when day changes)
        self._last_fg_reorder_date: date | None = None
        self._jobs_created_today_by_product: dict[str, int] = {}
        
        # Cost drift tracking (random walk for each part)
        self._cost_drift: dict[str, float] = {}  # part_id -> drift multiplier (-0.2 to +0.2)
        self._last_cost_drift_day: int = -1  # Track last day we applied drift
        
        # Black swan event (only for 3-year historical generation)
        self._black_swan_event: BlackSwanEvent | None = None
        if include_black_swan and simulation_years >= 3:
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
        """Append an event to JSONL: single file (historical) or date-partitioned (run-service/simulate)."""
        json_line = json.dumps(event, ensure_ascii=False)
        try:
            if self._events_single_file and self._events_single_file_path is not None:
                self._events_single_file_path.parent.mkdir(parents=True, exist_ok=True)
                if self._events_file is None:
                    self._events_file = self._events_single_file_path.open("a", encoding="utf-8")
                self._events_file.write(json_line + "\n")
            else:
                self.events_dir.mkdir(parents=True, exist_ok=True)
                day = self.current_time.date()
                if self._events_current_day != day:
                    if self._events_file is not None:
                        self._events_file.flush()
                        self._events_file.close()
                        self._events_file = None
                    self._events_current_day = day
                    path = self.events_dir / f"{day:%Y-%m-%d}.jsonl"
                    self._events_file = path.open("a", encoding="utf-8")
                self._events_file.write(json_line + "\n")
        except IOError as e:
            import sys
            print(f"Warning: Failed to write event log: {e}", file=sys.stderr)

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
        self._emit_daily_forecast()
        self._emit_sop_snapshot()
        self._emit_monthly_ctc()
        self._expire_promos()
        self._maybe_start_promo()
        self._check_black_swan_events()
        self._apply_daily_cost_drift()
        self._process_pending_purchase_orders()
        self._process_pending_backorders()
        self._process_pending_invoices()
        self._process_ready_for_shipping()
        self._process_pending_deliveries()
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
        """Generate a random black swan event for 3-year historical simulation.
        
        Places the event randomly in year 2 of a 3-year run (after baseline,
        before final year recovery).
        """
        if simulation_years < 3:
            return None
        
        # Pick a random template
        template = self.rng.choice(BLACK_SWAN_TEMPLATES)
        
        # Event start date: randomly in year 2 of 3-year simulation
        min_offset_days = 365       # Start of year 2
        max_offset_days = 365 * 2   # End of year 2
        
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
            
            job_id = self._allocated_job_for_fulfillment(backorder.product_id, qty_to_ship)
            unit_price = self.config.get("default_unit_price", 1250.0)
            amount = round(unit_price * qty_to_ship, 2)
            payload = {
                "order_id": backorder.order_id,
                "customer_id": backorder.customer_id,
                "product_id": backorder.product_id,
                "qty_shipped": qty_to_ship,
                "qty_still_pending": backorder.qty_remaining,
                "original_order_qty": backorder.original_qty,
                "remaining_stock": self.inventory[backorder.product_id]["qty_on_hand"],
                "allocation_source": "production_job" if job_id else "on_hand",
                "unit_price": unit_price,
                "amount": amount,
            }
            if job_id:
                payload["allocated_from_production_job_id"] = job_id
            self._log_event("BackorderFulfilled", payload)
            self._create_invoice(
                order_id=backorder.order_id,
                customer_id=backorder.customer_id,
                product_id=backorder.product_id,
                qty=qty_to_ship,
            )
            self._schedule_fulfillment_delivery(
                order_id=backorder.order_id,
                customer_id=backorder.customer_id,
                product_id=backorder.product_id,
                qty=qty_to_ship,
            )
            # If still has remaining, keep in pending
            if backorder.qty_remaining > 0:
                still_pending.append(backorder)
        
        self._pending_backorders = still_pending

    def _expire_promos(self) -> None:
        """Remove promos that have ended."""
        if not self.config.get("promo_enabled", False):
            return
        self._active_promos = [p for p in self._active_promos if self.current_time < p["end_time"]]

    def _maybe_start_promo(self) -> None:
        """With probability start a new promo (demand multiplier for a period)."""
        if not self.config.get("promo_enabled", False):
            return
        if self.rng.random() >= self.config.get("promo_probability", 0.05):
            return
        duration_days = self.config.get("promo_duration_days", 7)
        end_time = self.current_time + timedelta(days=duration_days)
        mult_min = self.config.get("promo_demand_multiplier_min", 1.2)
        mult_max = self.config.get("promo_demand_multiplier_max", 1.8)
        multiplier = self.rng.uniform(mult_min, mult_max)
        promo_id = str(uuid.uuid4())
        self._active_promos.append({
            "promo_id": promo_id,
            "end_time": end_time,
            "multiplier": multiplier,
        })
        self._log_event(
            "PromoActive",
            {
                "promo_id": promo_id,
                "start_time": iso_utc(self.current_time),
                "end_time": iso_utc(end_time),
                "demand_multiplier": round(multiplier, 2),
            },
        )

    def _get_promo_multiplier(self) -> float:
        """Product of active promo multipliers (1.0 if none)."""
        if not self._active_promos:
            return 1.0
        result = 1.0
        for p in self._active_promos:
            result *= p.get("multiplier", 1.0)
        return result

    def _get_active_promo_id(self) -> str | None:
        """Return one active promo_id if any (for attribution)."""
        if not self._active_promos:
            return None
        return self._active_promos[0].get("promo_id")

    def _get_demand_probability(self) -> float:
        """Get demand probability based on time of day, seasonality, and promos."""
        if self._is_business_hours():
            base_prob = self.config["demand_probability_business_hours"]
        else:
            base_prob = self.config["demand_probability_base"]
        
        # Apply seasonality
        seasonal_factor = self._get_demand_seasonality_factor()
        # Apply promo multiplier
        promo_factor = self._get_promo_multiplier()
        return base_prob * seasonal_factor * promo_factor

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

        product_id = self.rng.choice(self.product_ids)
        order = SalesOrder(
            order_id=str(uuid.uuid4()),
            customer_id=customer["customer_id"],
            product_id=product_id,
            qty=qty,
            created_at=iso_utc(self.current_time),
        )
        unit_price = self.config.get("default_unit_price", 1250.0)
        line_total = round(unit_price * order.qty, 2)
        payload: dict[str, Any] = {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "product_id": order.product_id,
            "qty": order.qty,
            "unit_price": unit_price,
            "line_total": line_total,
        }
        promo_id = self._get_active_promo_id()
        if promo_id:
            payload["promo_id"] = promo_id
        self._log_event("SalesOrderCreated", payload)
        self._emit_material_requirements(order.product_id, order.qty, order.order_id, "order")
        if self.config.get("forecast_enabled", False):
            self._demand_history.append((self.current_time.date(), order.product_id, order.qty))
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
            job_id = self._allocated_job_for_fulfillment(order.product_id, order.qty)
            unit_price = self.config.get("default_unit_price", 1250.0)
            amount = round(unit_price * order.qty, 2)
            payload = {
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "product_id": order.product_id,
                "qty": order.qty,
                "qty_ordered": order.qty,
                "fulfillment_type": "full",
                "remaining_stock": self.inventory[order.product_id]["qty_on_hand"],
                "allocation_source": "production_job" if job_id else "on_hand",
                "unit_price": unit_price,
                "amount": amount,
            }
            if job_id:
                payload["allocated_from_production_job_id"] = job_id
            self._log_event("ShipmentCreated", payload)
            self._create_invoice(
                order_id=order.order_id,
                customer_id=order.customer_id,
                product_id=order.product_id,
                qty=order.qty,
            )
            self._schedule_fulfillment_delivery(
                order_id=order.order_id,
                customer_id=order.customer_id,
                product_id=order.product_id,
                qty=order.qty,
            )
            return
        
        if stock > 0:
            # Partial fulfillment - ship what we have, backorder the rest
            qty_shipped = stock
            qty_backordered = order.qty - stock
            
            self.inventory[order.product_id]["qty_on_hand"] = 0
            
            job_id = self._allocated_job_for_fulfillment(order.product_id, qty_shipped)
            unit_price = self.config.get("default_unit_price", 1250.0)
            amount = round(unit_price * qty_shipped, 2)
            payload = {
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "product_id": order.product_id,
                "qty_shipped": qty_shipped,
                "qty_backordered": qty_backordered,
                "qty_ordered": order.qty,
                "remaining_stock": 0,
                "allocation_source": "production_job" if job_id else "on_hand",
                "unit_price": unit_price,
                "amount": amount,
            }
            if job_id:
                payload["allocated_from_production_job_id"] = job_id
            self._log_event("PartialShipmentCreated", payload)
            self._create_invoice(
                order_id=order.order_id,
                customer_id=order.customer_id,
                product_id=order.product_id,
                qty=qty_shipped,
            )
            self._schedule_fulfillment_delivery(
                order_id=order.order_id,
                customer_id=order.customer_id,
                product_id=order.product_id,
                qty=qty_shipped,
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

    def _get_plant_facility_id(self) -> str:
        """Return the facility_id of the SkyForge plant (first facility with type 'plant')."""
        for f in self.facilities:
            if isinstance(f, dict) and f.get("facility_type") == "plant":
                return f.get("facility_id", "FAC-001")
        return "FAC-001"

    def _facility_location_code(self, facility_id: str) -> str | None:
        """Return location_code for the facility, or None if not found."""
        for f in self.facilities:
            if isinstance(f, dict) and f.get("facility_id") == facility_id:
                return f.get("location_code")
        return None

    def _get_route_outbound(self, origin_facility_id: str, destination_facility_id: str) -> dict[str, Any] | None:
        """Look up outbound route by origin and destination location_code (CODE -> CODE)."""
        origin_code = self._facility_location_code(origin_facility_id)
        dest_code = self._facility_location_code(destination_facility_id)
        if not origin_code or not dest_code:
            return None
        for r in self.routes_outbound:
            if isinstance(r, dict) and r.get("origin_location_code") == origin_code and r.get("destination_location_code") == dest_code:
                return r
        return None

    def _get_route_inbound(self, origin_facility_id: str, destination_country: str) -> dict[str, Any] | None:
        """Look up inbound route by origin facility and destination country."""
        for r in self.routes_inbound:
            if isinstance(r, dict) and r.get("origin_facility_id") == origin_facility_id and r.get("destination_country") == destination_country:
                return r
        return None

    def _schedule_fulfillment_delivery(
        self,
        *,
        order_id: str,
        customer_id: str,
        product_id: str,
        qty: int,
    ) -> None:
        """Either stage for load consolidation or create a single load immediately."""
        if not self.config.get("delivery_enabled", True):
            return
        dest_facility_id = self._destination_facility_for_customer(customer_id)
        if not dest_facility_id:
            return
        if self.config.get("load_consolidation_enabled", False):
            self._ready_for_shipping.append(
                ReadyForShippingItem(
                    order_id=order_id,
                    customer_id=customer_id,
                    product_id=product_id,
                    qty=qty,
                    destination_facility_id=dest_facility_id,
                    ready_at=self.current_time,
                )
            )
        else:
            self._create_load_and_schedule_delivery(
                order_id=order_id,
                customer_id=customer_id,
                product_id=product_id,
                qty=qty,
            )

    def _create_load_and_schedule_delivery(
        self,
        *,
        order_id: str,
        customer_id: str,
        product_id: str,
        qty: int,
        order_ids: list[str] | None = None,
    ) -> None:
        """Create a load (single or consolidated), emit LoadCreated, and schedule Pickup/Delivery events."""
        if not self.config.get("delivery_enabled", True):
            return
        dest_facility_id = self._destination_facility_for_customer(customer_id)
        if not dest_facility_id:
            return
        plant_id = self._get_plant_facility_id()
        route = self._get_route_outbound(plant_id, dest_facility_id)
        if not route:
            return
        oids = order_ids if order_ids is not None else [order_id]
        load_id = str(uuid.uuid4())
        route_id = route.get("route_id", load_id)
        typical_transit_days = route.get("typical_transit_days", 3)
        scheduled_pickup = self.current_time
        scheduled_delivery = self.current_time + timedelta(days=typical_transit_days)
        disruption_prob = self.config.get("delivery_disruption_probability", 0.05)
        if self.rng.random() < disruption_prob:
            d_min = self.config.get("delivery_disruption_days_min", 1)
            d_max = self.config.get("delivery_disruption_days_max", 3)
            disruption_days = self.rng.randint(d_min, d_max)
            actual_delivery = scheduled_delivery + timedelta(days=disruption_days)
        else:
            variance_hours = self.rng.randint(0, min(24, self.config.get("delivery_transit_delay_max_hours", 12)))
            actual_delivery = scheduled_delivery + timedelta(hours=variance_hours)
        weight_lbs = qty * self.config.get("drone_x1_weight_lbs", 5.0)
        pieces = qty * self.config.get("drone_x1_pieces_per_unit", 1)
        distance_miles = route.get("typical_distance_miles", 0)
        self._log_event(
            "LoadCreated",
            {
                "load_id": load_id,
                "order_id": order_id,
                "order_ids": oids,
                "customer_id": customer_id,
                "route_id": route_id,
                "product_id": product_id,
                "qty": qty,
                "weight_lbs": round(weight_lbs, 2),
                "pieces": pieces,
                "load_status": "dispatched",
                "scheduled_pickup": iso_utc(scheduled_pickup),
                "scheduled_delivery": iso_utc(scheduled_delivery),
                "actual_delivery": iso_utc(actual_delivery),
                "created_at": iso_utc(self.current_time),
                "distance_miles": distance_miles,
            },
        )
        self._pending_deliveries.append(
            PendingDelivery(
                load_id=load_id,
                order_id=order_id,
                order_ids=oids,
                customer_id=customer_id,
                route_id=route_id,
                product_id=product_id,
                qty=qty,
                weight_lbs=weight_lbs,
                pieces=pieces,
                scheduled_pickup=scheduled_pickup,
                scheduled_delivery=scheduled_delivery,
                actual_delivery=actual_delivery,
                origin_facility_id=plant_id,
                destination_facility_id=dest_facility_id,
            )
        )

    def _process_ready_for_shipping(self) -> None:
        """Group staged items by (destination_facility_id, product_id); create loads when full or flush_days reached."""
        if not self.config.get("delivery_enabled", True) or not self.config.get("load_consolidation_enabled", False):
            return
        weight_limit = self.config.get("load_weight_limit_lbs", 500)
        flush_days = self.config.get("load_flush_days", 3)
        flush_cutoff = self.current_time - timedelta(days=flush_days)
        weight_per_unit = self.config.get("drone_x1_weight_lbs", 5.0)

        # Group by (destination_facility_id, product_id)
        groups: dict[tuple[str, str], list[ReadyForShippingItem]] = {}
        for item in self._ready_for_shipping:
            key = (item.destination_facility_id, item.product_id)
            groups.setdefault(key, []).append(item)

        to_remove: list[ReadyForShippingItem] = []
        for (dest_facility_id, product_id), items in groups.items():
            total_qty = sum(i.qty for i in items)
            total_weight = total_qty * weight_per_unit
            oldest_ready = min(i.ready_at for i in items)
            flush = total_weight >= weight_limit or oldest_ready <= flush_cutoff
            if not flush or not items:
                continue
            order_ids = [i.order_id for i in items]
            first = items[0]
            self._create_load_and_schedule_delivery(
                order_id=first.order_id,
                customer_id=first.customer_id,
                product_id=product_id,
                qty=total_qty,
                order_ids=order_ids,
            )
            to_remove.extend(items)

        for item in to_remove:
            self._ready_for_shipping.remove(item)

    def _process_pending_deliveries(self) -> None:
        """Emit DeliveryEvent (Pickup and Delivery) for loads whose actual_delivery time has passed."""
        if not self.config.get("delivery_enabled", True):
            return
        still_pending = []
        grace_hours = self.config.get("delivery_grace_hours", 12)
        for pd in self._pending_deliveries:
            if self.current_time < pd.actual_delivery:
                still_pending.append(pd)
                continue
            # Small variance for actual timestamp (0-120 min) for realism
            actual_datetime = pd.actual_delivery + timedelta(minutes=self.rng.randint(0, 120))
            on_time = actual_datetime <= pd.scheduled_delivery + timedelta(hours=grace_hours)
            self._log_event(
                "DeliveryEvent",
                {
                    "event_id": str(uuid.uuid4()),
                    "load_id": pd.load_id,
                    "event_type": "Pickup",
                    "facility_id": pd.origin_facility_id,
                    "scheduled_datetime": iso_utc(pd.scheduled_pickup),
                    "actual_datetime": iso_utc(pd.scheduled_pickup),
                    "detention_minutes": 0,
                    "on_time_flag": True,
                },
            )
            self._log_event(
                "DeliveryEvent",
                {
                    "event_id": str(uuid.uuid4()),
                    "load_id": pd.load_id,
                    "event_type": "Delivery",
                    "facility_id": pd.destination_facility_id,
                    "scheduled_datetime": iso_utc(pd.scheduled_delivery),
                    "actual_datetime": iso_utc(actual_datetime),
                    "detention_minutes": 0,
                    "on_time_flag": on_time,
                },
            )
        self._pending_deliveries = still_pending

    def _destination_facility_for_customer(self, customer_id: str) -> str | None:
        """Return destination_facility_id for customer (every customer has one from generator)."""
        customer = next((c for c in self.customers if isinstance(c, dict) and c.get("customer_id") == customer_id), None)
        if not customer:
            return None
        return customer.get("destination_facility_id")

    def _emit_material_requirements(
        self,
        product_id: str,
        order_qty: int,
        order_id: str,
        source: str,
    ) -> None:
        """Emit one MaterialRequirementsCreated event per order with aggregated requirements (BOM explosion)."""
        lead_days = self.config.get("requirements_lead_time_days", 30)
        required_by_date = self.current_time + timedelta(days=lead_days)
        required_by_iso = required_by_date.date().isoformat()
        requirements: list[dict[str, Any]] = []
        for comp in self._bom_components_for_product(product_id):
            part_id = comp.get("component_id")
            qty_per = comp.get("qty", 0)
            if not part_id:
                continue
            required_qty = order_qty * qty_per
            if required_qty <= 0:
                continue
            requirements.append({
                "part_id": part_id,
                "required_qty": required_qty,
                "required_by_date": required_by_iso,
            })
        if not requirements:
            return
        self._log_event(
            "MaterialRequirementsCreated",
            {
                "order_id": order_id,
                "product_id": product_id,
                "source": source,
                "required_by_date": required_by_iso,
                "requirements": requirements,
            },
        )

    def _allocated_job_for_fulfillment(self, product_id: str, qty: int) -> str | None:
        """Pop up to qty from finished-good source queue; return job_id if any was allocated."""
        queue = self._finished_good_sources.setdefault(product_id, [])
        if not queue or qty <= 0:
            return None
        remaining = qty
        job_id_used: str | None = None
        new_queue: list[tuple[str, int]] = []
        for job_id, available in queue:
            if remaining <= 0:
                new_queue.append((job_id, available))
                continue
            take = min(available, remaining)
            if job_id_used is None:
                job_id_used = job_id
            remaining -= take
            left = available - take
            if left > 0:
                new_queue.append((job_id, left))
        self._finished_good_sources[product_id] = new_queue
        return job_id_used

    def _create_invoice(
        self,
        *,
        order_id: str,
        customer_id: str,
        product_id: str,
        qty: int,
        amount: float | None = None,
        currency: str = "USD",
    ) -> None:
        """Create an invoice for a shipment and append to pending invoices."""
        if not self.config.get("invoice_enabled", True):
            return
        unit_price = self.config.get("default_unit_price", 1250.0)
        if amount is None:
            amount = round(unit_price * qty, 2)
        days_min = self.config.get("invoice_payment_days_min", 14)
        days_max = self.config.get("invoice_payment_days_max", 30)
        payment_days = self.rng.randint(days_min, days_max)
        due_date = self.current_time + timedelta(days=payment_days)
        invoice_id = str(uuid.uuid4())
        self._log_event(
            "InvoiceCreated",
            {
                "invoice_id": invoice_id,
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "qty": qty,
                "amount": amount,
                "currency": currency,
                "due_date": iso_utc(due_date),
                "timestamp": iso_utc(self.current_time),
            },
        )
        self._pending_invoices.append(
            PendingInvoice(
                invoice_id=invoice_id,
                order_id=order_id,
                customer_id=customer_id,
                product_id=product_id,
                qty=qty,
                amount=amount,
                currency=currency,
                due_date=due_date,
            )
        )

    def _process_pending_invoices(self) -> None:
        """Process due invoices: emit PaymentReceived with optional late payment."""
        if not self.config.get("invoice_enabled", True):
            return
        still_pending = []
        late_prob = self.config.get("payment_late_probability", 0.1)
        late_days = self.config.get("payment_late_days_extra", 5)
        for inv in self._pending_invoices:
            if self.current_time < inv.due_date:
                still_pending.append(inv)
                continue
            # Due: with probability (1 - late_prob) pay on time; else pay late
            is_late = self.rng.random() < late_prob
            if is_late:
                paid_at = inv.due_date + timedelta(days=late_days)
                on_time = False
            else:
                paid_at = self.current_time
                on_time = True
            self._log_event(
                "PaymentReceived",
                {
                    "invoice_id": inv.invoice_id,
                    "order_id": inv.order_id,
                    "amount": inv.amount,
                    "paid_at": iso_utc(paid_at),
                    "on_time": on_time,
                },
            )
        self._pending_invoices = still_pending

    def _emit_daily_forecast(self) -> None:
        """Emit demand forecast once per simulation day (naive: rolling avg + seasonality)."""
        if not self.config.get("forecast_enabled", False):
            return
        today = self.current_time.date()
        if self._last_forecast_date == today:
            return
        self._last_forecast_date = today
        window_days = self.config.get("forecast_window_days", 14)
        horizon_days = self.config.get("forecast_horizon_days", 7)
        cutoff = today - timedelta(days=window_days)
        # Trim history to window and aggregate by product
        recent = [(d, p, q) for d, p, q in self._demand_history if d >= cutoff]
        self._demand_history = [(d, p, q) for d, p, q in self._demand_history if d >= cutoff]
        if not recent:
            return
        days_in_window = max(1, (today - cutoff).days)
        by_product: dict[str, int] = {}
        for _d, product_id, qty in recent:
            by_product[product_id] = by_product.get(product_id, 0) + qty
        seasonal = self._get_demand_seasonality_factor()
        bias_mult = self.config.get("forecast_bias_correction_mult", 1.0)
        for product_id, total_qty in by_product.items():
            avg_daily = total_qty / days_in_window
            forecast_qty = avg_daily * horizon_days * seasonal * bias_mult
            forecast_qty = max(0, round(forecast_qty, 2))
            self._last_forecast_by_product[product_id] = forecast_qty
            self._log_event(
                "DemandForecastCreated",
                {
                    "snapshot_date": today.isoformat(),
                    "product_id": product_id,
                    "forecast_qty": forecast_qty,
                    "horizon_days": horizon_days,
                    "forecast_date": (today + timedelta(days=horizon_days)).isoformat(),
                },
            )

    def _emit_monthly_ctc(self) -> None:
        """Emit cash-to-cash metrics snapshot once per month (for pipeline analytics)."""
        today = self.current_time.date()
        if today.day != 1:
            return
        prev_month = today.month - 1 if today.month > 1 else 12
        prev_year = today.year if today.month > 1 else today.year - 1
        period = (prev_year, prev_month)
        if self._last_ctc_month == period:
            return
        self._last_ctc_month = period
        period_start = date(prev_year, prev_month, 1)
        if prev_month == 12:
            period_end = date(prev_year, 12, 31)
        else:
            period_end = date(prev_year, prev_month + 1, 1) - timedelta(days=1)
        self._log_event(
            "CTCMetricsEmitted",
            {
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "avg_days_receivables": None,
                "avg_days_payables": None,
                "avg_days_inventory": None,
            },
        )

    def _emit_sop_snapshot(self) -> None:
        """Emit S&OP snapshot on first tick of each planning period (monthly or weekly)."""
        if not self.config.get("sop_enabled", False):
            return
        today = self.current_time.date()
        freq = self.config.get("sop_frequency", "monthly")
        if freq == "monthly":
            period = (today.year, today.month)
        else:
            # ISO week
            period = (today.isocalendar().year, today.isocalendar().week)
        if self._last_sop_period == period:
            return
        self._last_sop_period = period
        jobs = self.production_schedule.get("active_jobs", [])
        wip_by_product: dict[str, int] = {}
        for job in jobs:
            if job.get("status") == "WIP":
                pid = job.get("product_id") or (self.product_ids[0] if self.product_ids else "D-101")
                wip_by_product[pid] = wip_by_product.get(pid, 0) + 1
        product_ids = set(self.inventory.keys()) | set(wip_by_product.keys()) | set(self._last_forecast_by_product.keys())
        for product_id in product_ids:
            inv_data = self.inventory.get(product_id, {})
            inventory_plan_qty = inv_data.get("qty_on_hand", 0)
            supply_plan_qty = wip_by_product.get(product_id, 0)
            demand_forecast_qty = self._last_forecast_by_product.get(product_id, 0)
            self._log_event(
                "SOPSnapshotCreated",
                {
                    "plan_date": today.isoformat(),
                    "scenario": "baseline",
                    "product_id": product_id,
                    "demand_forecast_qty": demand_forecast_qty,
                    "supply_plan_qty": supply_plan_qty,
                    "inventory_plan_qty": inventory_plan_qty,
                },
            )

    def create_production_job(self, *, product_id: str, qty: int | None = None) -> None:
        """Create a new production job for the specified product with batch size."""
        # Batch size from config (e.g. 5-15 units per job)
        batch_min = self.config.get("production_batch_size_min", 1)
        batch_max = self.config.get("production_batch_size_max", 1)
        qty_per_job = int(qty) if qty is not None else self.rng.randint(
            max(1, batch_min),
            max(1, batch_max),
        )
        # Calculate production duration based on config (scale slightly with batch size)
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
            "qty_per_job": qty_per_job,
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
                "qty_per_job": qty_per_job,
            },
        )

    def _incoming_production_by_product(self) -> dict[str, int]:
        """Sum of qty_per_job for all WIP jobs per product."""
        result: dict[str, int] = {}
        for job in self.production_schedule.get("active_jobs", []):
            if job.get("status") != "WIP":
                continue
            pid = job.get("product_id")
            if not pid:
                continue
            qty = job.get("qty_per_job", 1)
            result[pid] = result.get(pid, 0) + qty
        return result

    def _part_demand_from_wip_jobs(self) -> dict[str, float]:
        """Sum of parts required by all Planned/WIP jobs (BOM * batch_size)."""
        demand: dict[str, float] = {}
        for job in self.production_schedule.get("active_jobs", []):
            if job.get("status") not in ("Planned", "WIP"):
                continue
            product_id = job.get("product_id")
            if not product_id:
                continue
            batch_size = job.get("qty_per_job", 1)
            for comp in self._bom_components_for_product(product_id):
                part_id = comp.get("component_id")
                qty_per = comp.get("qty", 0)
                if not part_id:
                    continue
                demand[part_id] = demand.get(part_id, 0) + qty_per * batch_size
        return demand

    def _check_reorder_points(self) -> None:
        """
        Check inventory levels against reorder points and trigger automatic POs (parts)
        or production jobs (finished goods using net inventory position).
        """
        today = self.current_time.date()
        # Reset per-day throttle for finished goods when day rolls over
        if self._last_fg_reorder_date != today:
            self._last_fg_reorder_date = today
            self._jobs_created_today_by_product = {}

        incoming_production = self._incoming_production_by_product()
        backorder_qty_by_product: dict[str, int] = {}
        for bo in self._pending_backorders:
            pid = bo.product_id
            backorder_qty_by_product[pid] = backorder_qty_by_product.get(pid, 0) + bo.qty_remaining

        # --- Finished goods: net position reorder (proactive production) ---
        if self.config.get("finished_goods_reorder_enabled", True):
            max_jobs_per_day = self.config.get("finished_goods_max_jobs_per_product_per_day", 2)
            for product_id in self.product_ids:
                if product_id not in self.inventory or product_id in self.parts_by_id:
                    continue
                inv_data = self.inventory.get(product_id, {})
                if not isinstance(inv_data, dict):
                    continue
                on_hand = inv_data.get("qty_on_hand", 0)
                reorder_point = inv_data.get("reorder_point", 15)
                safety_stock = inv_data.get("safety_stock", 5)
                backorder_qty = backorder_qty_by_product.get(product_id, 0)
                incoming = incoming_production.get(product_id, 0)
                net_position = on_hand + incoming - backorder_qty

                if net_position >= reorder_point:
                    continue
                jobs_today = self._jobs_created_today_by_product.get(product_id, 0)
                if jobs_today >= max_jobs_per_day:
                    continue

                # Target: cover backorders + reorder_point + safety_stock
                shortfall = (reorder_point + safety_stock + backorder_qty) - net_position
                if shortfall <= 0:
                    continue
                batch_max = self.config.get("production_batch_size_max", 15)
                job_qty = min(max(1, shortfall), max(1, batch_max))
                self._jobs_created_today_by_product[product_id] = jobs_today + 1
                self.create_production_job(product_id=product_id, qty=job_qty)

        # --- Parts: reorder when net position <= reorder point ---
        part_demand_wip = self._part_demand_from_wip_jobs()
        incoming_parts: dict[str, float] = {}
        for po in self._pending_purchase_orders:
            incoming_parts[po.part_id] = incoming_parts.get(po.part_id, 0) + po.qty

        for part_id, inv_data in self.inventory.items():
            if not isinstance(inv_data, dict):
                continue
            if part_id not in self.parts_by_id:
                continue
            if part_id in self._parts_on_order:
                continue

            qty_on_hand = inv_data.get("qty_on_hand", 0)
            reorder_point = inv_data.get("reorder_point", 0)
            safety_stock = inv_data.get("safety_stock", 0)
            incoming = incoming_parts.get(part_id, 0)
            demand_wip = part_demand_wip.get(part_id, 0)
            net_position = qty_on_hand + incoming - demand_wip

            if net_position <= reorder_point:
                target_max = reorder_point + safety_stock + 50
                order_qty = max(0, target_max - net_position)
                if order_qty <= 0:
                    continue
                self._log_event(
                    "ReorderTriggered",
                    {
                        "part_id": part_id,
                        "qty_on_hand": qty_on_hand,
                        "reorder_point": reorder_point,
                        "net_position": net_position,
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
        receive_at = lambda po: po.actual_arrival if po.actual_arrival is not None else po.eta
        for po in self._pending_purchase_orders:
            if self.current_time >= receive_at(po):
                self._receive_purchase_order(po)
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
        
        # Log receipt event (with projected vs actual for lead time analytics)
        actual_receipt_time = po.actual_arrival if po.actual_arrival is not None else self.current_time
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
                "projected_eta": iso_utc(po.eta),
                "actual_receipt_time": iso_utc(actual_receipt_time),
            },
        )

    def _bom_components_for_product(self, product_id: str) -> list[dict[str, Any]]:
        """Get BOM components for a specific product (from bom_by_product index)."""
        return self.bom_by_product.get(product_id, [])

    def run_production(self) -> None:
        """
        Run production logic for all active jobs.
        
        - Planned jobs: Check for parts, start if available, order if not
        - WIP jobs: Check if production duration has elapsed, complete if so
        """
        completed_job_ids = set()
        
        for job in self.production_schedule.get("active_jobs", []):
            status = job.get("status")
            product_id = job.get("product_id") or (self.product_ids[0] if self.product_ids else "D-101")
            
            if status == "Planned":
                # Try to start production (batch_size from job)
                batch_size = job.get("qty_per_job", 1)
                missing = self._missing_parts_for_job(product_id, batch_size)
                if not missing:
                    self._consume_parts_for_job(product_id, batch_size)
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
        product_id = job.get("product_id") or (self.product_ids[0] if self.product_ids else "D-101")
        qty_per_job = job.get("qty_per_job", 1)
        
        if product_id not in self.inventory:
            self.inventory[product_id] = {"qty_on_hand": 0, "reorder_point": 15, "safety_stock": 5}
        
        self.inventory[product_id]["qty_on_hand"] = (
            self.inventory[product_id].get("qty_on_hand", 0) + qty_per_job
        )
        
        job["status"] = "Completed"
        job["actual_completion"] = iso_utc(self.current_time)
        
        self._log_event(
            "ProductionCompleted",
            {
                "job_id": job["job_id"],
                "product_id": product_id,
                "status": job["status"],
                "qty_produced": qty_per_job,
                "new_qty_on_hand": self.inventory[product_id]["qty_on_hand"],
            },
        )
        # Allocation: track this job as source for future shipments (FIFO)
        self._finished_good_sources.setdefault(product_id, []).append((job["job_id"], qty_per_job))

    def _missing_parts_for_job(self, product_id: str, batch_size: int = 1) -> dict[str, float]:
        """
        Check which parts are missing for a production job.
        batch_size multiplies BOM qty (for multi-unit jobs).
        """
        missing: dict[str, float] = {}
        for comp in self._bom_components_for_product(product_id):
            part_id = comp.get("component_id")
            qty_per_unit = comp.get("qty", 0)
            if not part_id:
                continue
            qty_needed = qty_per_unit * batch_size
            on_hand = self.inventory.get(part_id, {}).get("qty_on_hand", 0)
            if on_hand < qty_needed:
                missing[part_id] = qty_needed - on_hand
        return missing

    def _consume_parts_for_job(self, product_id: str, batch_size: int = 1) -> None:
        """
        Consume parts from inventory for a production job.
        batch_size multiplies BOM qty (for multi-unit jobs).
        """
        for comp in self._bom_components_for_product(product_id):
            part_id = comp.get("component_id")
            qty_per_unit = comp.get("qty", 0)
            if not part_id:
                continue
            qty = qty_per_unit * batch_size
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
        # Stochastic variance on actual arrival (e.g. +/- 48 hours)
        variance_hours = self.config.get("supplier_lead_time_variance_hours", 0)
        if variance_hours > 0:
            v = self.rng.randint(-variance_hours, variance_hours)
            actual_arrival = eta + timedelta(hours=v)
        else:
            actual_arrival = eta

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
            actual_arrival=actual_arrival,
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
                self._events_file.flush()
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

