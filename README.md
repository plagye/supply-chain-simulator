# Supply Chain Simulator

MAJOR UPDATE INCOMING, THIS BRANCH WILL SOON BE DEPRECATED

A realistic supply chain simulation for practicing data engineering skills (SQL, Pandas, Excel, PowerBI).

At first, the simulator was pushing the generated JSON lines straight to the database, but I decided that it's better for the learning experience when the pipeline just generates raw JSON files, and it's the user's mission to create the database and upload data to it. This way, one can even omit the database creation step and just play with the generated data, e.g., using only Pandas.

Keep in mind that the JSON files in data/ directory are there just for example/reference. I suggest deleting them and generating new master data using the simulation's built-in features.

I'm running the simulation on a Ubuntu Server VM hosted on Azure, and I store all the data in a PostgreSQL Azure server, which is also needed for the simulation to store its current state and to restart from the same point. Feel free to design your own workflow.

Quick User Manual: https://jumpshare.com/share/riGw9gb5XHGVsxg5quEG

## The Business Scenario

You're the data engineer at **SkyForge Dynamics**, a mid-sized manufacturer of industrial drones. The company produces the **DRONE-X1**, a commercial-grade quadcopter used for surveying, inspection, and delivery applications.

### The Product: DRONE-X1

The DRONE-X1 is assembled from ~50 different components organized into 7 sub-assemblies:

| Sub-Assembly | Key Components |
|--------------|----------------|
| **Airframe & Structure** | Carbon fiber sheets, aluminum extrusions, landing gear, vibration dampeners |
| **Propulsion System** | Brushless motors (4x), ESCs, propellers (4x) |
| **Power System** | Li-Ion battery cells, battery management ICs, power distribution board |
| **Flight Controller** | 32-bit microcontroller, IMU sensor, barometer, GPS module |
| **Communication** | Radio transceiver, antenna, telemetry module |
| **Payload Bay** | Camera gimbal, mounting brackets, payload connectors |
| **Wiring & Connectors** | Wire harnesses, XT60 connectors, signal cables |

### The Supply Chain

**Suppliers (30 total)** are distributed globally:
- **China (40%)** - Electronics, motors, batteries. *Watch out for Chinese New Year disruptions (Jan-Feb)!*
- **Taiwan (20%)** - Semiconductors, ICs. *Also affected by CNY.*
- **Germany (20%)** - Precision components, sensors. *Slower in August (vacation) and December (holidays).*
- **USA (20%)** - Specialized electronics, GPS modules. *Thanksgiving and Christmas slowdowns.*

Suppliers vary in reliability (70-100%) and pricing. Reliable suppliers charge premium prices but deliver on time. Cheaper suppliers may have longer lead times, partial shipments, or quality issues.

### The Customers

**15 B2B customers** across three regions:
- **North America (40%)** - Primarily commercial/industrial
- **EMEA (35%)** - Mix of commercial and government
- **APAC (25%)** - Growing market

Customers are split into two tiers:
- **Tier 1** - Government/defense contracts with strict SLAs and penalty clauses
- **Tier 2** - Commercial customers with standard terms

Each customer has a **country**, a **structured address** (street, city, state, postal_code, country), and a **delivery_location_code** (see below) that identifies the DC they receive shipments at.

### Geography and location codes

SkyForge's **single plant** is in **Chicago, USA**. All outbound shipments originate there. Facilities (plant and distribution/delivery centers) and routes use a **location code** in the form `COUNTRY_CITY` (e.g. `USA_CHI` for Chicago, `USA_DET` for Detroit, `DEU_FRA` for Frankfurt). **Routes are always described as CODE → CODE** (origin to destination). Each customer has a **delivery_location_code** in the same format (the code of the DC that serves them), so the destination side of every route is explicit on the customer record. Outbound routes in `routes.json` include `origin_location_code` and `destination_location_code`; the engine looks up routes by these codes.

### The Challenges You'll Analyze

- **Demand seasonality**: Q4 surge, summer lull, end-of-quarter rushes, Friday spikes
- **Supplier disruptions**: CNY shuts down Asian suppliers for weeks
- **Cost volatility**: Raw material prices drift ±20% over time
- **Quality issues**: 1-5% of incoming parts fail inspection
- **Backorders**: When demand exceeds supply, orders are split across multiple shipments
- **Black swan events**: Major disruptions (3-year historical data only)

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Generate master data
python main.py generate

# Run simulation for 30 days (720 hours)
python main.py simulate --ticks 720
```

---

## Overview

This simulator generates realistic supply chain events, including:
- Customer demand with business-hour patterns, bulk orders, and **seasonality**
- Production scheduling with duration modeling
- Procurement with supplier reliability affecting lead times
- Inventory management with automatic reorder points
- Quality issues and partial shipments
- **Split shipments and backorders** (SQL join complexity)
- **Cost variation** (commodity drift + supplier pricing)
- **Seasonal patterns** (CNY, holidays, end-of-quarter, day-of-week)
- **Black swan events** (major disruptions for 3-year history)
- **Data corruption** (1% of events corrupted for error handling practice)

**Your job as data engineer:** The simulation produces JSONL event files (single file for historical backfill, date-partitioned for batch/live runs; no loader in this repo). You build the pipeline that parses the JSONL (including corrupted records!), quarantines bad lines, loads valid events into PostgreSQL, designs your schema, joins events, and builds dashboards.

---

## Project Layout

```
supply-chain-simulator/
├── main.py                 # CLI entry point
├── config.json             # Configuration defaults
├── requirements.txt        # Python dependencies
├── .env.example            # Database credentials template
├── scripts/
│   ├── world_engine.py     # Core simulation engine
│   ├── db_manager.py       # PostgreSQL database operations
│   └── generate_*.py       # Data generator scripts
└── data/
    ├── *.json              # Master data and state files
    └── *.jsonl             # Event logs
```

---

## Commands

### 1) Generate Master Data

```bash
python main.py generate [--seed 42]
```

Creates all master data files: suppliers, parts, BOM, **facilities** (with `location_code`), **routes** (inbound and outbound, CODE → CODE), customers (with country, structured address, `delivery_location_code`), inventory, and a minimal production schedule. All data-generator scripts live in `scripts/` and are invoked via `main.py` (e.g. `python main.py generate`).

### 2) Run Simulation (Batch Mode)

```bash
python main.py simulate [--ticks 720] [--seed 42] [--start-time 2026-02-02T08:00:00Z]
```

Runs simulation for a fixed number of ticks (each tick = 1 hour). Events are written to JSON files.

### 3) Generate + Simulate

```bash
python main.py all [--ticks 720] [--seed 42]
```

Combines `generate` and `simulate` in one command.

### 4) Generate Historical Data (NEW)

```bash
python main.py generate-history --years 3 [--seed 42]
```

Generates 1–3 years of historical data in accelerated mode. Events are written to a **single JSONL file** (`data/events/history.jsonl`) for speed—no per-day file rollover. All event types are always emitted (orders, shipments, loads, deliveries, invoices, production, etc.). You can transfer the file to PostgreSQL or use it for analysis.

**Options:**
- `--years` (required): 1, 2, or 3 years of history
- `--seed`: RNG seed for reproducibility
- `--start-time`: Start date (default: N years before now)

**Note:** Black swan events are included only when generating 3 years of history.

**Output:** Historical generation writes one file, `data/events/history.jsonl`. The **run-service** and **simulate** commands use **date-partitioned** JSONL (one file per day under `data/events/`, e.g. `YYYY-MM-DD.jsonl`) for ongoing or short runs. So: one file for bulk historical backfill; per-day files for live or batch simulation.

### 5) Run 24/7 Service (NEW)

```bash
python main.py run-service [--tick-interval 5] [--resume|--fresh] [--seed 42]
```

Runs the simulation as a continuous service. Events are written to date-partitioned JSONL in `data/events/`. Only simulation state (for resume) is saved to PostgreSQL.

**Options:**
- `--tick-interval`: Seconds between ticks (default: 5.0)
- `--resume`: Resume from saved database state (default)
- `--fresh`: Start fresh, ignoring saved state
- `--seed`: RNG seed

**Time behavior:**
| tick-interval | Speed | 1 sim day | 1 sim month |
|---------------|-------|-----------|-------------|
| 5 (default) | Fast | 2 min | ~1 hour |
| 60 | Moderate | 24 min | ~12 hours |
| 3600 | Real-time | 24 hours | 30 days |

---

## Database Setup (PostgreSQL)

**This is intentionally left as a learning exercise.** You need to design your own database schema based on the JSON data and event logs.

**Tips:**
- Look at the JSON files in `data/` for field names and data types
- Parse the date-partitioned files in `data/events/` (e.g. `YYYY-MM-DD.jsonl`) to understand event structures
- Use JSONB for flexible payload storage, or normalize into separate columns
- Add appropriate indexes for timestamp and foreign key columns

### Configure Credentials

Copy `.env.example` to `.env` and fill in your values:

```env
DB_HOST=your-postgres-host.postgres.database.azure.com
DB_PORT=5432
DB_NAME=supply_chain
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSLMODE=require
```

---

## Config File

`config.json` supplies defaults for the CLI. CLI flags always override config values.

### Basic Fields

| Section | Key | Description |
|---------|-----|-------------|
| `generate` | `seed` | RNG seed for generators |
| `simulate` | `ticks` | Number of hourly ticks |
| `simulate` | `seed` | RNG seed for simulation |
| `simulate` | `start_time` | ISO-8601 start time |
| `simulate.engine` | `events_dir` | Optional; directory for date-partitioned event JSONL (default: `data/events`). Can also set `EVENTS_DIR` in `.env`. |
| `run-service` | `tick_interval` | Seconds between ticks |

### Engine Config (Simulation Parameters)

The `simulate.engine` section controls simulation behavior:

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

---

## Seasonality Features

### Monthly Demand Patterns

| Month | Multiplier | Reason |
|-------|------------|--------|
| January | 0.8x | Post-holiday slump |
| February | 0.85x | Recovery |
| March-May | 1.0-1.05x | Normal/Spring |
| June-August | 0.85-0.9x | Summer lull |
| September | 1.1x | Back to business |
| October | 1.2x | Q4 ramp |
| November | 1.4x | Peak season |
| December | 1.3x | Holiday orders |

### Day-of-Week Effects

| Day | Multiplier | Pattern |
|-----|------------|---------|
| Monday | 0.85x | Slow start |
| Tuesday | 0.95x | Ramping up |
| Wednesday | 1.0x | Baseline |
| Thursday | 1.05x | Building momentum |
| Friday | 1.25x | End-of-week rush |
| Saturday | 0.6x | Reduced activity |
| Sunday | 0.4x | Minimal |

### Period-End Spikes

- **Month-end** (last 3 days): +20% demand
- **Quarter-end** (Mar, Jun, Sep, Dec last 5 days): Additional +15%

### Black Swan Events (3-year history only)

Random major disruption placed in year 2 of a 3-year run:

| Event | Duration | Demand | Lead Time | Affected |
|-------|----------|--------|-----------|----------|
| Supply Chain Crisis | 21 days | -30% | 2.5x | China, Taiwan |
| Port Congestion | 30 days | -10% | 2.0x | China, USA |
| Natural Disaster | 14 days | -50% | 3.0x | Taiwan |
| Logistics Disruption | 28 days | -20% | 2.2x | China, Germany, USA |
| Semiconductor Shortage | 25 days | +10% | 3.5x | Taiwan, China |

---

## Output Files

### Master Data (JSON)

| File | Description |
|------|-------------|
| `suppliers.json` | Supplier master with reliability scores |
| `parts.json` | Part catalog with costs and valid suppliers |
| `bom.json` | Bill of materials for DRONE-X1 |
| `customers.json` | Customer master: country, structured address (street, city, state, postal_code, country), delivery_location_code, contract tiers |
| `inventory.json` | Current inventory state (updated by simulation) |
| `production_schedule.json` | Active jobs (updated by simulation). May be empty; the simulation creates jobs on demand when orders or backorders need product. |
| `facilities.json` | Plant (Chicago) and distribution/delivery facilities; each has `facility_id`, `location_code` (e.g. USA_CHI, USA_DET). Used for delivery routes and load dispatch. |
| `routes.json` | Inbound (plant ← supplier country) and outbound (plant → DC) routes. Each route has `origin_location_code` and `destination_location_code` (CODE → CODE), plus distance and transit days. |

### Event Log (JSONL)

Event output depends on the command:

- **Historical generation** (`generate-history`): one file, `data/events/history.jsonl`. No per-day rollover, so generation stays fast.
- **Run-service and simulate**: **date-partitioned** JSONL under `data/events/`—one file per simulation day, named `YYYY-MM-DD.jsonl`. The simulation rolls over to a new file when the simulated day changes. You can override the directory with config key `events_dir` or environment variable `EVENTS_DIR` (default: `data/events`).

Example layout for date-partitioned mode: `data/events/2026-02-02.jsonl`, `data/events/2026-02-03.jsonl`, etc. Each line is one event:

```json
{
  "timestamp": "2026-02-02T08:00:00Z",
  "event_type": "SalesOrderCreated",
  "payload": { "order_id": "...", "customer_id": "...", "qty": 3 }
}
```

Corruption metadata (for verifying your pipeline's quarantine) is written to `data/events/_meta/corruption_meta_log.jsonl`.

### Event Types

| Event | Description | Key Payload Fields |
|-------|-------------|-------------------|
| `SalesOrderCreated` | Customer places order | `order_id`, `customer_id`, `product_id`, `qty` |
| `ShipmentCreated` | Order shipped in full | `order_id`, `product_id`, `qty`, `remaining_stock` |
| `PartialShipmentCreated` | Partial fulfillment | `order_id`, `qty_shipped`, `qty_backordered` |
| `InvoiceCreated` | Invoice issued for shipment | `invoice_id`, `order_id`, `customer_id`, `product_id`, `qty`, `amount`, `currency`, `due_date` |
| `PaymentReceived` | Customer payment received | `invoice_id`, `order_id`, `amount`, `paid_at`, `on_time` |
| `DemandForecastCreated` | Demand forecast snapshot | `snapshot_date`, `product_id`, `forecast_qty`, `horizon_days`, `forecast_date` |
| `MaterialRequirementsCreated` | Material requirements from order (BOM explosion; one event per order) | `order_id`, `product_id`, `source`, `required_by_date`, `requirements` (array of objects with `part_id`, `required_qty`, `required_by_date`) |
| `SOPSnapshotCreated` | S&OP planning snapshot | `plan_date`, `scenario`, `product_id`, `demand_forecast_qty`, `supply_plan_qty`, `inventory_plan_qty` |
| `PromoActive` | Promo / demand shock started | `promo_id`, `start_time`, `end_time`, `demand_multiplier` |
| `CTCMetricsEmitted` | Monthly cash-to-cash metrics snapshot | `period_start`, `period_end`, `avg_days_receivables`, `avg_days_payables`, `avg_days_inventory` |
| `LoadCreated` | Load dispatched for delivery | `load_id`, `order_id`, `customer_id`, `route_id`, `product_id`, `qty`, `weight_lbs`, `pieces`, `scheduled_pickup`, `scheduled_delivery`, `distance_miles` |
| `DeliveryEvent` | Pickup or delivery at facility | `event_id`, `load_id`, `event_type` (Pickup/Delivery), `facility_id`, `scheduled_datetime`, `actual_datetime`, `detention_minutes`, `on_time_flag` |
| `BackorderCreated` | Order cannot be fulfilled | `order_id`, `qty_backordered`, `reason` |
| `BackorderFulfilled` | Backorder shipped | `order_id`, `qty_shipped`, `qty_still_pending` |
| `ProductionJobCreated` | New production job | `job_id`, `product_id`, `production_duration_hours` |
| `ProductionStarted` | Job begins | `job_id`, `expected_completion` |
| `ProductionCompleted` | Job finished | `job_id`, `new_qty_on_hand` |
| `PurchaseOrderCreated` | Parts ordered | `purchase_order_id`, `part_id`, `qty`, `unit_cost`, `eta` |
| `PurchaseOrderReceived` | Parts received | `purchase_order_id`, `qty_received`, `new_qty_on_hand` |
| `ReorderTriggered` | Auto-reorder | `part_id`, `qty_on_hand`, `reorder_point` |
| `PartialShipment` | Supplier delivers less | `purchase_order_id`, `ordered_qty`, `received_qty` |
| `QualityRejection` | Parts rejected | `purchase_order_id`, `qty_rejected`, `supplier_id` |
| `BlackSwanEventStarted` | Major disruption begins | `name`, `affected_countries`, `demand_multiplier` |
| `BlackSwanEventEnded` | Disruption ends | `name`, `duration_days` |

---

## Logging

When running as a service (`run-service`), logs are written to `simulation.log` with automatic rotation (10MB max, 5 backups).

Log levels:
- `INFO`: Normal operation (tick completions, daily summaries)
- `WARNING`: Recoverable issues (database retry, state save failure)
- `ERROR`: Failures that may require attention

---

## Data Engineering Practice

This simulator is designed to give you real-world data engineering challenges:

**Schema Design:**
- Design your own PostgreSQL schema from scratch
- Decide between normalized vs. denormalized structures
- Choose appropriate data types and indexes

**Data Pipeline:**
- Parse the JSONL files in `data/events/` and handle corrupted records (~1% intentionally malformed)
- Load JSON master data into dimension tables
- Stream events from date-partitioned JSONL into your fact tables

**Data Modeling:**
- Join events (link `PurchaseOrderCreated` → `PurchaseOrderReceived` via `purchase_order_id`)
- Build dimensional models (star schema) for analytics
- Handle slowly changing dimensions (supplier reliability changes over time)

**Analytics:**
- Calculate metrics (lead times, on-time delivery, inventory turns)
- Identify seasonality patterns in the data
- Create dashboards in PowerBI, Tableau, or Metabase
- Join orders to loads to delivery events (`LoadCreated`, `DeliveryEvent`) for logistics and on-time delivery analytics; load these into `fact_loads` and `fact_delivery_events` if using a star schema

**Error Handling:**
- Check `data/events/_meta/corruption_meta_log.jsonl` to verify your pipeline catches all corrupted records
- Build robust ETL that doesn't fail on bad data

---

## Example Workflows

### Generate 3 Years of History + Start 24/7 Service

```bash
# 1. Generate master data
python main.py generate --seed 42

# 2. Generate 3 years of historical data (events to data/events/history.jsonl)
python main.py generate-history --years 3 --seed 42

# 3. (Your pipeline) Parse JSONL, quarantine bad lines, load valid events into PostgreSQL

# 4. Start 24/7 service (events to JSONL; state/resume in PostgreSQL)
python main.py run-service --tick-interval 5 --fresh
```

### Run a Month of Batch Simulation

```bash
python main.py all --ticks 720 --seed 42
```

---

## Design and implementation notes

Summary of how event output and performance are set up:

- **Event output:** Historical generation (`generate-history`) writes a **single file**, `data/events/history.jsonl`, with no per-day file rollover, so multi-year runs stay fast. The **simulate** and **run-service** commands write **date-partitioned** JSONL (one file per simulation day, `YYYY-MM-DD.jsonl`) for batch and live use. So: one file for bulk historical backfill; per-day files for ongoing or short runs.

- **Historical cap and black swan:** You can generate 1, 2, or 3 years of history only. Black swan (major disruption) events are included only when generating **3 years**; the event is placed in year 2 of the run.

- **Event I/O and performance:** Events are flushed to disk only when the simulation day rolls over (when switching to the next day’s file) and when the engine saves state on exit—not after every event. That keeps historical generation from being dominated by I/O when many events (orders, loads, deliveries) are emitted.

- **Material requirements (BOM explosion):** Each sales order triggers one **MaterialRequirementsCreated** event (not one per BOM line). The payload includes `order_id`, `product_id`, `source`, `required_by_date`, and a **requirements** array of `{part_id`, `required_qty`, `required_by_date}`. Pipelines can explode this array to one row per part if needed. This avoids the previous design that emitted ~57 events per order and caused huge file sizes and run times.

- **All event types:** Orders, shipments, loads, deliveries, invoices, production, procurement, and the above material-requirements event are always emitted (no “with/without deliveries” toggle). Config can still turn features like `delivery_enabled` or `invoice_enabled` on or off for the engine globally.

---

## Acknowledgements

Cursor is crazy

---
