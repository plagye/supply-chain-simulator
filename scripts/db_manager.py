"""Database connection and operations for PostgreSQL integration.

This module provides centralized database management for the supply chain simulation,
including connection handling, event logging, and state persistence.

Key Functions:
    - get_engine(): Create/return cached SQLAlchemy engine
    - get_session(): Context manager for database sessions
    - insert_event(): Insert single event to fact_events table
    - insert_events_batch(): Bulk insert for better performance
    - save_system_state(): Persist simulation state for resume capability
    - load_system_state(): Load state for resuming simulation

Usage:
    from scripts.db_manager import get_session, insert_event
    
    # Insert an event
    event = {"timestamp": "...", "event_type": "...", "payload": {...}}
    insert_event(event)
    
    # Direct session access
    with get_session() as session:
        result = session.execute(text("SELECT COUNT(*) FROM fact_events"))

Note:
    Requires a .env file with database credentials. See .env.example for template.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError


# Module-level engine cache for connection reuse
_engine: Engine | None = None

# Logger for database operations
_logger = logging.getLogger("simulation.db")


def get_engine() -> Engine:
    """Create or return cached SQLAlchemy engine from .env credentials.
    
    Returns:
        SQLAlchemy Engine instance connected to PostgreSQL.
        
    Raises:
        ValueError: If required environment variables are missing.
    """
    global _engine
    if _engine is not None:
        return _engine
    
    load_dotenv()
    
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE", "require")
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"
    
    _engine = create_engine(
        url,
        pool_pre_ping=True,  # Verify connections before use
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,  # Recycle connections after 1 hour
    )
    return _engine


def reset_engine() -> None:
    """Reset the cached engine (useful for testing or reconnection)."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager for database sessions.
    
    Yields:
        SQLAlchemy Session instance.
        
    Example:
        with get_session() as session:
            session.execute(text("SELECT 1"))
    """
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def test_connection() -> bool:
    """Test database connectivity.
    
    Returns:
        True if connection successful, False otherwise.
    """
    try:
        with get_session() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def insert_event(event: dict[str, Any]) -> int | None:
    """Insert a single event into the fact_events table.
    
    Args:
        event: Dictionary with keys: timestamp, event_type, payload
        
    Returns:
        The event_id of the inserted row, or None on failure.
        
    Note:
        Errors are logged but not raised to avoid crashing the simulation.
        The caller should handle None returns appropriately.
    """
    try:
        with get_session() as session:
            result = session.execute(
                text("""
                    INSERT INTO fact_events (timestamp, event_type, payload)
                    VALUES (:timestamp, :event_type, :payload)
                    RETURNING event_id
                """),
                {
                    "timestamp": event["timestamp"],
                    "event_type": event["event_type"],
                    "payload": json.dumps(event["payload"]),
                }
            )
            row = result.fetchone()
            return row[0] if row else None
    except OperationalError as e:
        _logger.error(f"Database connection error inserting event: {e}")
        return None
    except SQLAlchemyError as e:
        _logger.warning(f"Failed to insert event '{event.get('event_type', 'unknown')}': {e}")
        return None
    except KeyError as e:
        _logger.error(f"Missing required key in event dict: {e}")
        return None


def insert_events_batch(events: list[dict[str, Any]]) -> int:
    """Bulk insert events for better performance.
    
    Args:
        events: List of event dictionaries with keys: timestamp, event_type, payload
        
    Returns:
        Number of successfully inserted events (0 on failure).
        
    Note:
        This is an all-or-nothing operation. If any event fails,
        the entire batch is rolled back.
    """
    if not events:
        return 0
    
    try:
        with get_session() as session:
            # Prepare batch data with validation
            batch_data = [
                {
                    "timestamp": e["timestamp"],
                    "event_type": e["event_type"],
                    "payload": json.dumps(e["payload"]),
                }
                for e in events
            ]
            
            session.execute(
                text("""
                    INSERT INTO fact_events (timestamp, event_type, payload)
                    VALUES (:timestamp, :event_type, :payload)
                """),
                batch_data
            )
            return len(events)
    except OperationalError as e:
        _logger.error(f"Database connection error in batch insert ({len(events)} events): {e}")
        return 0
    except SQLAlchemyError as e:
        _logger.warning(f"Failed to batch insert {len(events)} events: {e}")
        return 0
    except (KeyError, TypeError) as e:
        _logger.error(f"Invalid event data in batch: {e}")
        return 0


def save_system_state(
    current_time: datetime,
    tick_count: int,
    status: str = "running"
) -> bool:
    """Persist simulation state for resume capability.
    
    Uses upsert pattern to maintain a single row in system_state table.
    This allows the simulation to resume from the exact point it stopped.
    
    Args:
        current_time: Current simulation datetime (timezone-aware).
        tick_count: Number of ticks completed.
        status: Current status ('running', 'paused', 'stopped').
        
    Returns:
        True if save successful, False otherwise.
        
    Example:
        save_system_state(datetime.now(timezone.utc), 1000, "running")
    """
    try:
        with get_session() as session:
            # Upsert pattern: insert or update the single row
            session.execute(
                text("""
                    INSERT INTO system_state (id, current_simulation_time, tick_count, status, last_updated)
                    VALUES (1, :current_time, :tick_count, :status, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        current_simulation_time = :current_time,
                        tick_count = :tick_count,
                        status = :status,
                        last_updated = NOW()
                """),
                {
                    "current_time": current_time,
                    "tick_count": tick_count,
                    "status": status,
                }
            )
            return True
    except OperationalError as e:
        _logger.error(f"Database connection error saving state: {e}")
        return False
    except SQLAlchemyError as e:
        _logger.warning(f"Failed to save system state (tick {tick_count}): {e}")
        return False


def load_system_state() -> dict[str, Any] | None:
    """Load simulation state for resuming.
    
    Retrieves the last saved state from the system_state table.
    Use this to resume a simulation from where it stopped.
    
    Returns:
        Dictionary with keys:
            - current_simulation_time: datetime of last tick
            - tick_count: number of completed ticks
            - status: last known status
            - last_updated: when state was saved
        Returns None if no state exists or on database error.
        
    Example:
        state = load_system_state()
        if state:
            resume_from = state["current_simulation_time"]
    """
    try:
        with get_session() as session:
            result = session.execute(
                text("""
                    SELECT current_simulation_time, tick_count, status, last_updated
                    FROM system_state
                    WHERE id = 1
                """)
            )
            row = result.fetchone()
            if row:
                _logger.info(f"Loaded system state: tick {row[1]}, status '{row[2]}'")
                return {
                    "current_simulation_time": row[0],
                    "tick_count": row[1],
                    "status": row[2],
                    "last_updated": row[3],
                }
            _logger.info("No existing system state found in database")
            return None
    except OperationalError as e:
        _logger.error(f"Database connection error loading state: {e}")
        return None
    except SQLAlchemyError as e:
        _logger.warning(f"Failed to load system state: {e}")
        return None


def save_inventory_snapshot(timestamp: datetime, inventory: dict[str, dict]) -> int:
    """Save inventory snapshot for historical tracking.
    
    Args:
        timestamp: Snapshot timestamp.
        inventory: Dictionary mapping item_id to {qty_on_hand, reorder_point, safety_stock}.
        
    Returns:
        Number of rows inserted.
    """
    if not inventory:
        return 0
    
    try:
        with get_session() as session:
            batch_data = [
                {
                    "timestamp": timestamp,
                    "item_id": item_id,
                    "qty_on_hand": data.get("qty_on_hand", 0),
                    "reorder_point": data.get("reorder_point", 0),
                    "safety_stock": data.get("safety_stock", 0),
                }
                for item_id, data in inventory.items()
            ]
            
            session.execute(
                text("""
                    INSERT INTO fact_inventory_snapshots 
                    (timestamp, item_id, qty_on_hand, reorder_point, safety_stock)
                    VALUES (:timestamp, :item_id, :qty_on_hand, :reorder_point, :safety_stock)
                """),
                batch_data
            )
            return len(batch_data)
    except SQLAlchemyError:
        return 0


def upsert_dimension_suppliers(suppliers: list[dict]) -> int:
    """Upsert supplier dimension data.
    
    Args:
        suppliers: List of supplier dictionaries.
        
    Returns:
        Number of rows affected.
    """
    if not suppliers:
        return 0
    
    try:
        with get_session() as session:
            for supplier in suppliers:
                session.execute(
                    text("""
                        INSERT INTO dim_suppliers (supplier_id, name, country, reliability_score, risk_factor, price_multiplier)
                        VALUES (:id, :name, :country, :reliability_score, :risk_factor, :price_multiplier)
                        ON CONFLICT (supplier_id) DO UPDATE SET
                            name = :name,
                            country = :country,
                            reliability_score = :reliability_score,
                            risk_factor = :risk_factor,
                            price_multiplier = :price_multiplier
                    """),
                    supplier
                )
            return len(suppliers)
    except SQLAlchemyError:
        return 0


def upsert_dimension_parts(parts: list[dict]) -> int:
    """Upsert parts dimension data.
    
    Args:
        parts: List of part dictionaries.
        
    Returns:
        Number of rows affected.
    """
    if not parts:
        return 0
    
    try:
        with get_session() as session:
            for part in parts:
                session.execute(
                    text("""
                        INSERT INTO dim_parts (part_id, name, category, standard_cost, unit_of_measure)
                        VALUES (:part_id, :name, :category, :standard_cost, :unit_of_measure)
                        ON CONFLICT (part_id) DO UPDATE SET
                            name = :name,
                            category = :category,
                            standard_cost = :standard_cost,
                            unit_of_measure = :unit_of_measure
                    """),
                    part
                )
            return len(parts)
    except SQLAlchemyError:
        return 0


def upsert_dimension_customers(customers: list[dict]) -> int:
    """Upsert customer dimension data.
    
    Args:
        customers: List of customer dictionaries.
        
    Returns:
        Number of rows affected.
    """
    if not customers:
        return 0
    
    try:
        with get_session() as session:
            for customer in customers:
                penalty_clauses = customer.get("penalty_clauses")
                session.execute(
                    text("""
                        INSERT INTO dim_customers (customer_id, company_name, region, contract_priority, penalty_clauses)
                        VALUES (:customer_id, :company_name, :region, :contract_priority, :penalty_clauses)
                        ON CONFLICT (customer_id) DO UPDATE SET
                            company_name = :company_name,
                            region = :region,
                            contract_priority = :contract_priority,
                            penalty_clauses = :penalty_clauses
                    """),
                    {
                        "customer_id": customer["customer_id"],
                        "company_name": customer["company_name"],
                        "region": customer["region"],
                        "contract_priority": customer["contract_priority"],
                        "penalty_clauses": json.dumps(penalty_clauses) if penalty_clauses else None,
                    }
                )
            return len(customers)
    except SQLAlchemyError:
        return 0


def get_event_count() -> int:
    """Get total number of events in the database.
    
    Returns:
        Count of events or -1 on error.
    """
    try:
        with get_session() as session:
            result = session.execute(text("SELECT COUNT(*) FROM fact_events"))
            row = result.fetchone()
            return row[0] if row else 0
    except SQLAlchemyError:
        return -1


def get_latest_event_timestamp() -> datetime | None:
    """Get timestamp of the most recent event.
    
    Returns:
        Datetime of latest event or None if no events exist.
    """
    try:
        with get_session() as session:
            result = session.execute(
                text("SELECT MAX(timestamp) FROM fact_events")
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except SQLAlchemyError:
        return None
