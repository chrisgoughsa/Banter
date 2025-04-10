-- ========================
-- Silver Layer Tables
-- ========================

-- ========================
-- 1. silver_customers Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_customers (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    client_id VARCHAR(50) NOT NULL,        -- Unique identifier for the customer
    register_time TIMESTAMP NOT NULL,      -- When the customer registered
    register_date DATE NOT NULL,           -- Date portion of register_time
    country VARCHAR(50),                   -- Customer's country
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- Customer status
    metadata JSONB,                        -- Additional customer information
    source_file TEXT NOT NULL,             -- Path to the source data file
    load_time TIMESTAMP NOT NULL,          -- When this record was loaded into the database
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, client_id),
    UNIQUE (affiliate_id, client_id)       -- Additional unique constraint for deduplication
) PARTITION BY RANGE (register_date);

-- Create partitions for the last 12 months
CREATE TABLE silver_customers_prev_year PARTITION OF silver_customers
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '12 months') TO (CURRENT_DATE - INTERVAL '6 months');
CREATE TABLE silver_customers_curr_year PARTITION OF silver_customers
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '6 months') TO (CURRENT_DATE + INTERVAL '6 months');

-- Add indexes for common queries
CREATE INDEX idx_silver_customers_register_date ON silver_customers(register_date);
CREATE INDEX idx_silver_customers_status ON silver_customers(status);
CREATE INDEX idx_silver_customers_country ON silver_customers(country);

-- ========================
-- 2. silver_deposits Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_deposits (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    client_id VARCHAR(50) NOT NULL,        -- Identifier for the client
    order_id VARCHAR(50) NOT NULL,         -- Unique identifier for the deposit order
    deposit_time TIMESTAMP NOT NULL,       -- When the deposit was made
    deposit_date DATE NOT NULL,            -- Date portion of deposit_time
    deposit_coin VARCHAR(10) NOT NULL,     -- Type of coin deposited
    deposit_amount DECIMAL(18,8) NOT NULL, -- Amount deposited
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- Deposit status
    metadata JSONB,                        -- Additional deposit information
    source_file TEXT NOT NULL,             -- Path to the source data file
    load_time TIMESTAMP NOT NULL,          -- When this record was loaded into the database
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    UNIQUE (order_id)                      -- Additional unique constraint for deduplication
) PARTITION BY RANGE (deposit_date);

-- Create partitions for the last 12 months
CREATE TABLE silver_deposits_prev_year PARTITION OF silver_deposits
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '12 months') TO (CURRENT_DATE - INTERVAL '6 months');
CREATE TABLE silver_deposits_curr_year PARTITION OF silver_deposits
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '6 months') TO (CURRENT_DATE + INTERVAL '6 months');

-- Add indexes for common queries
CREATE INDEX idx_silver_deposits_deposit_date ON silver_deposits(deposit_date);
CREATE INDEX idx_silver_deposits_client ON silver_deposits(client_id);
CREATE INDEX idx_silver_deposits_status ON silver_deposits(status);
CREATE INDEX idx_silver_deposits_coin ON silver_deposits(deposit_coin);

-- ========================
-- 3. silver_trades Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_trades (
    id SERIAL,                             -- Auto-incrementing unique identifier
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    client_id VARCHAR(50) NOT NULL,        -- Identifier for the client
    trade_volume DECIMAL(18,8) NOT NULL,   -- Volume of the trade
    trade_time TIMESTAMP NOT NULL,         -- When the trade occurred
    trade_date DATE NOT NULL,              -- Date portion of trade_time
    symbol VARCHAR(20) NOT NULL,           -- Trading symbol
    trade_type VARCHAR(10) NOT NULL,       -- Type of trade (buy/sell)
    status VARCHAR(20) NOT NULL DEFAULT 'completed', -- Trade status
    metadata JSONB,                        -- Additional trade information
    source_file TEXT NOT NULL,             -- Path to the source data file
    load_time TIMESTAMP NOT NULL,          -- When this record was loaded into the database
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (affiliate_id, client_id, trade_time, trade_volume)  -- Natural key for deduplication
) PARTITION BY RANGE (trade_date);

-- Create partitions for the last 12 months
CREATE TABLE silver_trades_prev_year PARTITION OF silver_trades
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '12 months') TO (CURRENT_DATE - INTERVAL '6 months');
CREATE TABLE silver_trades_curr_year PARTITION OF silver_trades
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '6 months') TO (CURRENT_DATE + INTERVAL '6 months');

-- Add indexes for common queries
CREATE INDEX idx_silver_trades_trade_date ON silver_trades(trade_date);
CREATE INDEX idx_silver_trades_client ON silver_trades(client_id);
CREATE INDEX idx_silver_trades_symbol ON silver_trades(symbol);
CREATE INDEX idx_silver_trades_type ON silver_trades(trade_type);
CREATE INDEX idx_silver_trades_status ON silver_trades(status);

-- ========================
-- 4. silver_assets Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_assets (
    id SERIAL,                             -- Auto-incrementing unique identifier
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    client_id VARCHAR(50) NOT NULL,        -- Identifier for the client
    balance DECIMAL(18,8) NOT NULL,        -- Current balance
    update_time TIMESTAMP NOT NULL,        -- When the balance was last updated
    update_date DATE NOT NULL,             -- Date portion of update_time
    symbol VARCHAR(20) NOT NULL,           -- Asset symbol
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- Asset status
    remark TEXT,                           -- Additional remarks or notes
    metadata JSONB,                        -- Additional asset information
    source_file TEXT NOT NULL,             -- Path to the source data file
    load_time TIMESTAMP NOT NULL,          -- When this record was loaded into the database
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (affiliate_id, client_id, update_time)  -- Natural key for deduplication
) PARTITION BY RANGE (update_date);

-- Create partitions for the last 12 months
CREATE TABLE silver_assets_prev_year PARTITION OF silver_assets
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '12 months') TO (CURRENT_DATE - INTERVAL '6 months');
CREATE TABLE silver_assets_curr_year PARTITION OF silver_assets
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '6 months') TO (CURRENT_DATE + INTERVAL '6 months');

-- Add indexes for common queries
CREATE INDEX idx_silver_assets_update_date ON silver_assets(update_date);
CREATE INDEX idx_silver_assets_client ON silver_assets(client_id);
CREATE INDEX idx_silver_assets_symbol ON silver_assets(symbol);
CREATE INDEX idx_silver_assets_status ON silver_assets(status);

-- ========================
-- 5. Data Quality Metrics Table
-- ========================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    table_name VARCHAR(50) NOT NULL,       -- Name of the table being measured
    metric_date DATE NOT NULL,             -- Date of the measurement
    total_records BIGINT NOT NULL,         -- Total number of records
    valid_records BIGINT NOT NULL,         -- Number of valid records
    invalid_records BIGINT NOT NULL,       -- Number of invalid records
    completeness_score DECIMAL(5,2),       -- Completeness score (0-100)
    accuracy_score DECIMAL(5,2),           -- Accuracy score (0-100)
    consistency_score DECIMAL(5,2),        -- Consistency score (0-100)
    timeliness_score DECIMAL(5,2),         -- Timeliness score (0-100)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, metric_date)
);

-- Add index for metric lookups
CREATE INDEX idx_data_quality_metric_date ON data_quality_metrics(metric_date);

-- ========================
-- Insert Functions with Deduplication and Data Quality Checks
-- ========================

-- Function to insert customers with deduplication and data quality checks
CREATE OR REPLACE FUNCTION insert_silver_customers(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_register_time TIMESTAMP,
    p_country VARCHAR,
    p_status VARCHAR,
    p_metadata JSONB,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    -- Data quality checks
    IF p_affiliate_id IS NULL OR p_client_id IS NULL OR p_register_time IS NULL THEN
        RAISE EXCEPTION 'Required fields cannot be NULL';
    END IF;

    INSERT INTO silver_customers (
        affiliate_id,
        client_id,
        register_time,
        register_date,
        country,
        status,
        metadata,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_register_time,
        p_register_time::DATE,
        p_country,
        COALESCE(p_status, 'active'),
        p_metadata,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id) DO UPDATE
    SET
        register_time = EXCLUDED.register_time,
        register_date = EXCLUDED.register_date,
        country = EXCLUDED.country,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        source_file = EXCLUDED.source_file,
        load_time = EXCLUDED.load_time,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert deposits with deduplication and data quality checks
CREATE OR REPLACE FUNCTION insert_silver_deposits(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_order_id VARCHAR,
    p_deposit_time TIMESTAMP,
    p_deposit_coin VARCHAR,
    p_deposit_amount DECIMAL,
    p_status VARCHAR,
    p_metadata JSONB,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    -- Data quality checks
    IF p_affiliate_id IS NULL OR p_client_id IS NULL OR p_order_id IS NULL OR 
       p_deposit_time IS NULL OR p_deposit_coin IS NULL OR p_deposit_amount IS NULL THEN
        RAISE EXCEPTION 'Required fields cannot be NULL';
    END IF;

    IF p_deposit_amount <= 0 THEN
        RAISE EXCEPTION 'Deposit amount must be positive';
    END IF;

    INSERT INTO silver_deposits (
        affiliate_id,
        client_id,
        order_id,
        deposit_time,
        deposit_date,
        deposit_coin,
        deposit_amount,
        status,
        metadata,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_order_id,
        p_deposit_time,
        p_deposit_time::DATE,
        p_deposit_coin,
        p_deposit_amount,
        COALESCE(p_status, 'pending'),
        p_metadata,
        p_source_file,
        p_load_time
    ) ON CONFLICT (order_id) DO UPDATE
    SET
        deposit_time = EXCLUDED.deposit_time,
        deposit_date = EXCLUDED.deposit_date,
        deposit_coin = EXCLUDED.deposit_coin,
        deposit_amount = EXCLUDED.deposit_amount,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        source_file = EXCLUDED.source_file,
        load_time = EXCLUDED.load_time,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert trades with deduplication and data quality checks
CREATE OR REPLACE FUNCTION insert_silver_trades(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_trade_volume DECIMAL,
    p_trade_time TIMESTAMP,
    p_symbol VARCHAR,
    p_trade_type VARCHAR,
    p_status VARCHAR,
    p_metadata JSONB,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    -- Data quality checks
    IF p_affiliate_id IS NULL OR p_client_id IS NULL OR p_trade_volume IS NULL OR 
       p_trade_time IS NULL OR p_symbol IS NULL OR p_trade_type IS NULL THEN
        RAISE EXCEPTION 'Required fields cannot be NULL';
    END IF;

    IF p_trade_volume <= 0 THEN
        RAISE EXCEPTION 'Trade volume must be positive';
    END IF;

    IF p_trade_type NOT IN ('buy', 'sell') THEN
        RAISE EXCEPTION 'Trade type must be either buy or sell';
    END IF;

    INSERT INTO silver_trades (
        affiliate_id,
        client_id,
        trade_volume,
        trade_time,
        trade_date,
        symbol,
        trade_type,
        status,
        metadata,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_trade_volume,
        p_trade_time,
        p_trade_time::DATE,
        p_symbol,
        p_trade_type,
        COALESCE(p_status, 'completed'),
        p_metadata,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id, trade_time, trade_volume) DO UPDATE
    SET
        symbol = EXCLUDED.symbol,
        trade_type = EXCLUDED.trade_type,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        source_file = EXCLUDED.source_file,
        load_time = EXCLUDED.load_time,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert assets with deduplication and data quality checks
CREATE OR REPLACE FUNCTION insert_silver_assets(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_balance DECIMAL,
    p_update_time TIMESTAMP,
    p_symbol VARCHAR,
    p_status VARCHAR,
    p_remark TEXT,
    p_metadata JSONB,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    -- Data quality checks
    IF p_affiliate_id IS NULL OR p_client_id IS NULL OR p_balance IS NULL OR 
       p_update_time IS NULL OR p_symbol IS NULL THEN
        RAISE EXCEPTION 'Required fields cannot be NULL';
    END IF;

    IF p_balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot be negative';
    END IF;

    INSERT INTO silver_assets (
        affiliate_id,
        client_id,
        balance,
        update_time,
        update_date,
        symbol,
        status,
        remark,
        metadata,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_balance,
        p_update_time,
        p_update_time::DATE,
        p_symbol,
        COALESCE(p_status, 'active'),
        p_remark,
        p_metadata,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id, update_time) DO UPDATE
    SET
        balance = EXCLUDED.balance,
        symbol = EXCLUDED.symbol,
        status = EXCLUDED.status,
        remark = EXCLUDED.remark,
        metadata = EXCLUDED.metadata,
        source_file = EXCLUDED.source_file,
        load_time = EXCLUDED.load_time,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql; 