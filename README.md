# Supply Chain Simulator

A realistic supply chain simulation for practicing data engineering skills (SQL, Pandas, Excel, PowerBI).

## Overview

This simulator generates realistic supply chain events including:
- Customer demand with business-hour patterns, bulk orders, and **seasonality**
- Production scheduling with duration modeling
- Procurement with supplier reliability affecting lead times
- Inventory management with automatic reorder points
- Quality issues and partial shipments
- **Split shipments and backorders** (SQL join complexity)
- **Cost variation** (commodity drift + supplier pricing)
- **Seasonal patterns** (CNY, holidays, end-of-quarter)
- **Data corruption** (1% of events corrupted for error handling practice)

**Your job as data engineer:** Parse the raw JSONL event log (including corrupted records!), design your own schema, join events together, and build dashboards.

## Project Layout

- `scripts/` - Generator scripts and simulation engine (`world_engine.py`)
- `data/` - Generated JSON datasets and runtime state
- `main.py` - Single entry-point CLI
- `config.json` - Configuration defaults

## Config File

`config.json` sits at the repo root and supplies defaults for the CLI.
CLI flags always override config values.

### Basic Config Fields

- `generate.seed`: RNG seed for all generators
- `simulate.ticks`: number of hourly ticks to run
- `simulate.seed`: RNG seed for the simulator
- `simulate.start_time`: ISO-8601 start time (e.g. `2026-02-02T08:00:00Z`) or `null`
- `all.*`: same fields as `simulate`, used when running `main.py all`

### Engine Config (Simulation Parameters)

The `simulate.engine` and `all.engine` sections control simulation behavior:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `demand_probability_base` | 0.05 | Chance of order per tick (off-hours) |
| `demand_probability_business_hours` | 0.12 | Chance of order per tick (8am-6pm) |
| `business_hours_start` | 8 | Business hours start (24h) |
| `business_hours_end` | 18 | Business hours end (24h) |
| `bulk_order_probability` | 0.08 | Chance of bulk order |
| `bulk_order_qty_min/max` | 10/20 | Bulk order quantity range |
| `normal_order_qty_min/max` | 1/5 | Normal order quantity range |
| `production_duration_hours_min/max` | 8/24 | Production time range |
| `base_lead_time_hours_min/max` | 24/168 | Supplier lead time range |
| `partial_shipment_probability` | 0.15 | Chance of partial delivery |
| `quality_reject_rate_min/max` | 0.01/0.05 | Quality rejection rate range |
| `data_corruption_enabled` | true | Enable random data corruption |
| `data_corruption_probability` | 0.01 | Chance of corrupting each event (1%) |
| `cost_drift_enabled` | true | Enable commodity price drift |
| `cost_drift_daily_pct` | 0.005 | Daily price drift ±0.5% |
| `cost_drift_max_pct` | 0.20 | Max cumulative drift ±20% |
| `seasonality_enabled` | true | Enable demand/supplier seasonality |
| `demand_seasonality_strength` | 1.0 | Demand seasonality intensity (0-1) |
| `supplier_seasonality_strength` | 1.0 | Supplier seasonality intensity (0-1) |

## Commands

### 1) Generate all data

```bash
python main.py generate
```

Override config:
```bash
python main.py generate --seed 123
```

### 2) Run the simulator

```bash
python main.py simulate
```

Override config:
```bash
python main.py simulate --ticks 48 --seed 7 --start-time 2026-02-02T08:00:00Z
```

### 3) Generate everything and simulate

```bash
python main.py all
```

Override config:
```bash
python main.py all --ticks 72 --seed 99
```

## Outputs

### Master Data (JSON files)

- `suppliers.json` - Supplier master with reliability scores
- `parts.json` - Part catalog with costs and valid suppliers
- `bom.json` - Bill of materials for DRONE-X1
- `customers.json` - Customer master with contract tiers
- `inventory.json` - Current inventory state (updated by simulation)
- `production_schedule.json` - Active jobs (updated by simulation)

### Event Log (JSONL)

All simulation events are appended to `data/daily_events_log.jsonl`.

Each event has the structure:
```json
{
  "timestamp": "2026-02-02T08:00:00Z",
  "event_type": "EventTypeName",
  "payload": { ... }
}
```

### Event Types

| Event | Description | Key Payload Fields |
|-------|-------------|-------------------|
| `SalesOrderCreated` | Customer places order | `order_id`, `customer_id`, `product_id`, `qty` |
| `ShipmentCreated` | Order shipped in full | `order_id`, `product_id`, `qty`, `remaining_stock` |
| `PartialShipmentCreated` | Partial fulfillment, rest backordered | `order_id`, `qty_shipped`, `qty_backordered` |
| `BackorderCreated` | Order cannot be fulfilled | `order_id`, `qty_backordered`, `reason` |
| `BackorderFulfilled` | Backorder shipped (full or partial) | `order_id`, `qty_shipped`, `qty_still_pending` |
| `ProductionJobCreated` | New production job | `job_id`, `product_id`, `production_duration_hours` |
| `ProductionStarted` | Job begins (parts consumed) | `job_id`, `product_id`, `expected_completion` |
| `ProductionCompleted` | Job finished, product added | `job_id`, `product_id`, `new_qty_on_hand` |
| `PurchaseOrderCreated` | Parts ordered from supplier | `purchase_order_id`, `part_id`, `qty`, `unit_cost`, `total_cost`, `eta` |
| `PurchaseOrderReceived` | Parts received into inventory | `purchase_order_id`, `qty_received`, `new_qty_on_hand` |
| `ReorderTriggered` | Auto-reorder at reorder point | `part_id`, `qty_on_hand`, `reorder_point`, `order_qty` |
| `PartialShipment` | Supplier delivers less than ordered | `purchase_order_id`, `ordered_qty`, `received_qty` |
| `QualityRejection` | Parts rejected at inspection | `purchase_order_id`, `qty_rejected`, `supplier_id` |

Note: ~1% of events are intentionally corrupted (invalid JSON, wrong types, etc.) for error handling practice. Check `corruption_meta_log.jsonl` to verify your pipeline catches them.

### Data Engineering Practice

The event log is intentionally "messy" - you practice:
- Parsing JSONL and loading into your tools
- Joining events (e.g., link `PurchaseOrderCreated` to `PurchaseOrderReceived` via `purchase_order_id`)
- Building dimensional models and fact tables
- Calculating metrics (lead times, on-time delivery, inventory turns)
- Creating dashboards in PowerBI

## Using a Different Config Path

```bash
python main.py --config path/to/config.json simulate
```

## Example: Run a Month of Simulation

```bash
# Generate fresh data and run 720 ticks (30 days)
python main.py all --ticks 720 --seed 42
```

