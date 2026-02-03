-- Supply Chain Simulator Database Schema
-- PostgreSQL 13+
-- 
-- Run this script once to initialize the database:
--   psql -h <host> -U <user> -d <database> -f schema.sql

-- ============================================================================
-- DIMENSION TABLES (Master Data)
-- ============================================================================

-- Suppliers dimension
CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    reliability_score DECIMAL(3,2),
    risk_factor VARCHAR(50),
    price_multiplier DECIMAL(4,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dim_suppliers IS 'Supplier master data - static reference data';

-- Parts dimension
CREATE TABLE IF NOT EXISTS dim_parts (
    part_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    standard_cost DECIMAL(10,2),
    unit_of_measure VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dim_parts IS 'Parts catalog - static reference data';

-- Customers dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id UUID PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    region VARCHAR(50),
    contract_priority VARCHAR(50),
    shipping_address TEXT,
    penalty_clauses JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dim_customers IS 'Customer master data - static reference data';

-- Products dimension (finished goods)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dim_products IS 'Finished products catalog';

-- Insert default product
INSERT INTO dim_products (product_id, name, description) 
VALUES ('DRONE-X1', 'Drone X1', 'Commercial drone assembly')
ON CONFLICT (product_id) DO NOTHING;

-- Bill of Materials (linking products to parts)
CREATE TABLE IF NOT EXISTS dim_bom (
    bom_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL REFERENCES dim_products(product_id),
    sub_assembly VARCHAR(100),
    component_id VARCHAR(50) NOT NULL,
    qty DECIMAL(10,4) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(product_id, sub_assembly, component_id)
);

COMMENT ON TABLE dim_bom IS 'Bill of Materials - product to component mapping';
CREATE INDEX IF NOT EXISTS idx_bom_product ON dim_bom(product_id);
CREATE INDEX IF NOT EXISTS idx_bom_component ON dim_bom(component_id);

-- Part-Supplier relationship (many-to-many)
CREATE TABLE IF NOT EXISTS dim_part_suppliers (
    part_id VARCHAR(50) NOT NULL,
    supplier_id UUID NOT NULL,
    is_preferred BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (part_id, supplier_id)
);

COMMENT ON TABLE dim_part_suppliers IS 'Valid supplier assignments for parts';

-- ============================================================================
-- FACT TABLES (Transactional / Growing)
-- ============================================================================

-- Main events fact table (all simulation events)
CREATE TABLE IF NOT EXISTS fact_events (
    event_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE fact_events IS 'All simulation events - primary fact table';

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON fact_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON fact_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_type_timestamp ON fact_events(event_type, timestamp);

-- Inventory snapshots (periodic state captures)
CREATE TABLE IF NOT EXISTS fact_inventory_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    item_id VARCHAR(50) NOT NULL,
    qty_on_hand DECIMAL(12,2),
    reorder_point INTEGER,
    safety_stock INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE fact_inventory_snapshots IS 'Historical inventory levels for trend analysis';

CREATE INDEX IF NOT EXISTS idx_inventory_timestamp ON fact_inventory_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_inventory_item ON fact_inventory_snapshots(item_id);
CREATE INDEX IF NOT EXISTS idx_inventory_item_timestamp ON fact_inventory_snapshots(item_id, timestamp);

-- Purchase orders fact table (denormalized for analytics)
CREATE TABLE IF NOT EXISTS fact_purchase_orders (
    po_id BIGSERIAL PRIMARY KEY,
    purchase_order_id VARCHAR(100) NOT NULL,
    part_id VARCHAR(50) NOT NULL,
    supplier_id UUID,
    qty DECIMAL(12,2) NOT NULL,
    unit_cost DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    order_date TIMESTAMPTZ NOT NULL,
    expected_delivery TIMESTAMPTZ,
    actual_delivery TIMESTAMPTZ,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE fact_purchase_orders IS 'Purchase order history for supplier analysis';

CREATE INDEX IF NOT EXISTS idx_po_order_date ON fact_purchase_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_po_supplier ON fact_purchase_orders(supplier_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON fact_purchase_orders(status);

-- Sales orders fact table (denormalized for analytics)
CREATE TABLE IF NOT EXISTS fact_sales_orders (
    so_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    customer_id UUID,
    product_id VARCHAR(50) NOT NULL,
    qty INTEGER NOT NULL,
    order_date TIMESTAMPTZ NOT NULL,
    ship_date TIMESTAMPTZ,
    status VARCHAR(50) DEFAULT 'pending',
    is_backorder BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE fact_sales_orders IS 'Sales order history for demand analysis';

CREATE INDEX IF NOT EXISTS idx_so_order_date ON fact_sales_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_so_customer ON fact_sales_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_so_product ON fact_sales_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_so_status ON fact_sales_orders(status);

-- Production jobs fact table
CREATE TABLE IF NOT EXISTS fact_production_jobs (
    job_pk BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    planned_date TIMESTAMPTZ,
    start_date TIMESTAMPTZ,
    completion_date TIMESTAMPTZ,
    due_date TIMESTAMPTZ,
    duration_hours INTEGER,
    assigned_worker_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE fact_production_jobs IS 'Production job history';

CREATE INDEX IF NOT EXISTS idx_jobs_status ON fact_production_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_product ON fact_production_jobs(product_id);
CREATE INDEX IF NOT EXISTS idx_jobs_start_date ON fact_production_jobs(start_date);

-- ============================================================================
-- STATE TABLES (System Management)
-- ============================================================================

-- System state for simulation resume capability
CREATE TABLE IF NOT EXISTS system_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    current_simulation_time TIMESTAMPTZ NOT NULL,
    tick_count BIGINT DEFAULT 0,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'running',
    metadata JSONB,
    CONSTRAINT single_row CHECK (id = 1)
);

COMMENT ON TABLE system_state IS 'Simulation state persistence - single row table';

-- Black swan events log (for historical tracking)
CREATE TABLE IF NOT EXISTS system_black_swan_events (
    event_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    duration_days INTEGER NOT NULL,
    demand_multiplier DECIMAL(4,2),
    lead_time_multiplier DECIMAL(4,2),
    affected_countries TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE system_black_swan_events IS 'Historical record of black swan disruption events';

-- ============================================================================
-- VIEWS (Convenience)
-- ============================================================================

-- Current inventory view (latest snapshot per item)
CREATE OR REPLACE VIEW v_current_inventory AS
SELECT DISTINCT ON (item_id)
    item_id,
    qty_on_hand,
    reorder_point,
    safety_stock,
    timestamp as last_updated
FROM fact_inventory_snapshots
ORDER BY item_id, timestamp DESC;

COMMENT ON VIEW v_current_inventory IS 'Latest inventory level per item';

-- Event counts by type (for monitoring)
CREATE OR REPLACE VIEW v_event_summary AS
SELECT 
    event_type,
    COUNT(*) as event_count,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event
FROM fact_events
GROUP BY event_type
ORDER BY event_count DESC;

COMMENT ON VIEW v_event_summary IS 'Summary statistics by event type';

-- Daily event counts (for trend analysis)
CREATE OR REPLACE VIEW v_daily_events AS
SELECT 
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count
FROM fact_events
GROUP BY DATE(timestamp), event_type
ORDER BY event_date DESC, event_count DESC;

COMMENT ON VIEW v_daily_events IS 'Daily event counts by type';

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update triggers to dimension tables
DROP TRIGGER IF EXISTS update_dim_suppliers_updated_at ON dim_suppliers;
CREATE TRIGGER update_dim_suppliers_updated_at
    BEFORE UPDATE ON dim_suppliers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dim_parts_updated_at ON dim_parts;
CREATE TRIGGER update_dim_parts_updated_at
    BEFORE UPDATE ON dim_parts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dim_customers_updated_at ON dim_customers;
CREATE TRIGGER update_dim_customers_updated_at
    BEFORE UPDATE ON dim_customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dim_products_updated_at ON dim_products;
CREATE TRIGGER update_dim_products_updated_at
    BEFORE UPDATE ON dim_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- GRANTS (adjust as needed for your setup)
-- ============================================================================

-- Example: GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app_user;
-- Example: GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;
