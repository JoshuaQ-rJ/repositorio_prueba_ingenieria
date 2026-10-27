-- ==============================================================================
-- Analytical Data Warehouse (DataMart) DDL Script
-- Architecture: Kimball Star Schema (Idempotent)
-- Target RDBMS: PostgreSQL 15+ / 16
-- Database: datamart_dwh
-- File: sql/init/01_init_dwh.sql
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DIMENSION TABLES
-- ------------------------------------------------------------------------------

-- Dimensión de Productos (dim_products)
-- Almacena el catálogo unificado de artículos normalizados.
CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE NOT NULL,
    canonical_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) DEFAULT 'Uncategorized',
    supplier_country VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dimensión de Clientes (dim_customers)
-- Almacena los perfiles de clientes y soporta compras anónimas / invitados (-1).
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_code VARCHAR(50),
    country VARCHAR(100),
    is_anonymous BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Inserción de cliente por defecto para transacciones anónimas o sin customer_id
INSERT INTO dim_customers (customer_id, customer_code, country, is_anonymous)
VALUES (-1, 'ANONYMOUS', 'UNKNOWN', TRUE)
ON CONFLICT (customer_id) DO NOTHING;

-- Dimensión de Tiempo / Calendario (dim_dates)
-- Provee atributos temporales para agregaciones analíticas de negocio.
CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL
);

-- Poblado determinista de dim_dates para el horizonte operativo 2000-2035
INSERT INTO dim_dates (date_key, full_date, year, month, day, quarter, day_of_week)
SELECT
    (EXTRACT(YEAR FROM d)::INT * 10000 + EXTRACT(MONTH FROM d)::INT * 100 + EXTRACT(DAY FROM d)::INT) AS date_key,
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week
FROM generate_series('2000-01-01'::DATE, '2035-12-31'::DATE, '1 day'::INTERVAL) AS s(d)
ON CONFLICT (date_key) DO NOTHING;

-- ------------------------------------------------------------------------------
-- 2. FACT TABLES
-- ------------------------------------------------------------------------------

-- Tabla de Hechos de Ventas (fact_sales)
-- Grano: Una línea de ítem vendido con cantidad estrictamente positiva.
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_products(stock_code),
    customer_id INTEGER NOT NULL REFERENCES dim_customers(customer_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    gross_revenue NUMERIC(14, 4) NOT NULL,
    net_revenue NUMERIC(14, 4) NOT NULL,
    invoice_date_utc TIMESTAMPTZ NOT NULL,
    date_key INTEGER REFERENCES dim_dates(date_key),
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_fact_sales_item UNIQUE (invoice_no, stock_code, invoice_date_utc, quantity)
);

-- Tabla de Hechos de Devoluciones / Cancelaciones (fact_returns)
-- Grano: Una línea de ítem devuelto o cancelado con cantidad estrictamente negativa.
CREATE TABLE IF NOT EXISTS fact_returns (
    return_id BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_products(stock_code),
    customer_id INTEGER NOT NULL REFERENCES dim_customers(customer_id),
    quantity INTEGER NOT NULL CHECK (quantity < 0),
    unit_price NUMERIC(12, 4) NOT NULL,
    return_amount NUMERIC(14, 4) NOT NULL,
    return_date_utc TIMESTAMPTZ NOT NULL,
    date_key INTEGER REFERENCES dim_dates(date_key),
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_fact_returns_item UNIQUE (invoice_no, stock_code, return_date_utc, quantity)
);

-- ------------------------------------------------------------------------------
-- 3. QUARANTINE / AUDIT LOG
-- ------------------------------------------------------------------------------

-- Tabla de Rechazos y Calidad de Datos (rejections_log)
-- Registra eventos o registros crudos que no superaron las reglas de validación.
CREATE TABLE IF NOT EXISTS rejections_log (
    rejection_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(100) NOT NULL,
    raw_record_id VARCHAR(100),
    rejection_reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMPTZ DEFAULT NOW(),
    payload JSONB NOT NULL
);

-- ------------------------------------------------------------------------------
-- 4. PERFORMANCE INDEXES
-- Optimizados para las 7 preguntas analíticas de negocio (filtrado, joins y agregación)
-- ------------------------------------------------------------------------------

-- Índices en fact_sales
CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_date_utc ON fact_sales (invoice_date_utc);
CREATE INDEX IF NOT EXISTS idx_fact_sales_stock_code ON fact_sales (stock_code);
CREATE INDEX IF NOT EXISTS idx_fact_sales_country ON fact_sales (country);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id ON fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_no ON fact_sales (invoice_no);

-- Índices en fact_returns
CREATE INDEX IF NOT EXISTS idx_fact_returns_return_date_utc ON fact_returns (return_date_utc);
CREATE INDEX IF NOT EXISTS idx_fact_returns_stock_code ON fact_returns (stock_code);
CREATE INDEX IF NOT EXISTS idx_fact_returns_customer_id ON fact_returns (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_returns_date_key ON fact_returns (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_returns_country ON fact_returns (country);
CREATE INDEX IF NOT EXISTS idx_fact_returns_invoice_no ON fact_returns (invoice_no);

-- Índices en dimensiones
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products (category);
CREATE INDEX IF NOT EXISTS idx_dim_products_supplier_country ON dim_products (supplier_country);
CREATE INDEX IF NOT EXISTS idx_dim_customers_country ON dim_customers (country);
CREATE INDEX IF NOT EXISTS idx_dim_dates_year_month ON dim_dates (year, month);

-- Índices en rejections_log
CREATE INDEX IF NOT EXISTS idx_rejections_log_rejected_at ON rejections_log (rejected_at);
CREATE INDEX IF NOT EXISTS idx_rejections_log_source_file ON rejections_log (source_file);
CREATE INDEX IF NOT EXISTS idx_rejections_log_reason ON rejections_log (rejection_reason);
CREATE INDEX IF NOT EXISTS idx_rejections_log_payload_gin ON rejections_log USING GIN (payload);
