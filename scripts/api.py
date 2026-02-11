"""Minimal live API: read-only GET endpoints for status, inventory, backorders, deliveries. Call and get values."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# Engine reference set by main when starting run-service
_engine: Any = None


def set_engine(engine: Any) -> None:
    global _engine
    _engine = engine


def get_engine() -> Any:
    if _engine is None:
        raise HTTPException(status_code=503, detail="Simulation not running")
    return _engine


app = FastAPI(title="Supply Chain Simulator API", description="Read-only live state. Call and get values.")


def create_app(engine: Any | None = None) -> FastAPI:
    if engine is not None:
        set_engine(engine)
    return app


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


@app.get("/status")
def get_status() -> dict:
    """Simulation status: current_time, tick_count, running."""
    e = get_engine()
    return {
        "current_time": _iso_utc(e.current_time),
        "tick_count": e.tick_count,
        "running": getattr(e, "running", True),
    }


@app.get("/inventory")
def get_inventory() -> dict:
    """All current inventory (parts + products)."""
    return get_engine().inventory


@app.get("/inventory/parts")
def get_inventory_parts() -> dict:
    """Only parts (keys in parts_by_id)."""
    e = get_engine()
    parts_by_id = getattr(e, "parts_by_id", {})
    return {k: e.inventory.get(k, {}) for k in parts_by_id if k in e.inventory}


@app.get("/inventory/products")
def get_inventory_products() -> dict:
    """Only finished products (keys in product_ids)."""
    e = get_engine()
    product_ids = getattr(e, "product_ids", [])
    return {k: e.inventory.get(k, {}) for k in product_ids if k in e.inventory}


@app.get("/inventory/{item_id}")
def get_inventory_item(item_id: str) -> dict:
    """Single part or product level."""
    e = get_engine()
    if item_id not in e.inventory:
        raise HTTPException(status_code=404, detail="Not found")
    return e.inventory[item_id]


@app.get("/backorders")
def get_backorders() -> list[dict]:
    """Pending backorders."""
    e = get_engine()
    out = []
    for bo in getattr(e, "_pending_backorders", []):
        out.append({
            "order_id": bo.order_id,
            "customer_id": bo.customer_id,
            "product_id": bo.product_id,
            "qty_remaining": bo.qty_remaining,
            "original_qty": bo.original_qty,
            "created_at": _iso_utc(bo.created_at),
        })
    return out


@app.get("/deliveries")
def get_deliveries() -> list[dict]:
    """Pending deliveries (loads in transit)."""
    e = get_engine()
    out = []
    for pd in getattr(e, "_pending_deliveries", []):
        out.append({
            "load_id": pd.load_id,
            "order_id": pd.order_id,
            "customer_id": pd.customer_id,
            "route_id": pd.route_id,
            "product_id": pd.product_id,
            "qty": pd.qty,
            "weight_lbs": pd.weight_lbs,
            "pieces": pd.pieces,
            "scheduled_pickup": _iso_utc(pd.scheduled_pickup),
            "scheduled_delivery": _iso_utc(pd.scheduled_delivery),
            "origin_facility_id": pd.origin_facility_id,
            "destination_facility_id": pd.destination_facility_id,
        })
    return out
