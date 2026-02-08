"""Database connection and state persistence for the supply chain simulation.

This module provides connection handling and system state save/load for
run-service resume capability. Events are written to JSONL by the simulation;
this module does not insert events.

Key Functions:
    - get_engine(): Create/return cached SQLAlchemy engine
    - get_session(): Context manager for database sessions
    - test_connection(): Test database connectivity
    - save_system_state(): Persist simulation state for resume capability
    - load_system_state(): Load state for resuming simulation

Usage:
    from scripts.db_manager import load_system_state, save_system_state, test_connection
    if test_connection():
        state = load_system_state()
    save_system_state(current_time, tick_count, "running")

Note:
    Requires a .env file with database credentials. See .env.example for template.
"""

from __future__ import annotations

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

TABLE_SYSTEM_STATE = "system_state"


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
                text(f"""
                    INSERT INTO {TABLE_SYSTEM_STATE} (id, current_simulation_time, tick_count, status, last_updated)
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
                text(f"""
                    SELECT current_simulation_time, tick_count, status, last_updated
                    FROM {TABLE_SYSTEM_STATE}
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
