-- ========================
-- Bronze Layer Tables
-- ========================

-- ========================
-- 1. bronze_assets Table
-- ========================
CREATE TABLE bronze_assets (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    asset_id VARCHAR(50) NOT NULL,         -- Unique identifier for the asset
    symbol VARCHAR(50) NOT NULL,           -- Trading symbol
    name VARCHAR(100) NOT NULL,            -- Asset name
    data JSONB NOT NULL,                   -- Raw JSON data containing additional asset information
    load_timestamp TIMESTAMP NOT NULL,     -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP NOT NULL,  -- When the API call that fetched this data was made
    source_file VARCHAR(255) NOT NULL,     -- Path to the source JSON file
    load_status VARCHAR(20) NOT NULL,      -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    retry_count INTEGER DEFAULT 0,         -- Number of retry attempts
    last_retry_timestamp TIMESTAMP,        -- Timestamp of last retry attempt
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, asset_id, load_timestamp)
) PARTITION BY RANGE (load_timestamp);

-- Create partitions for the last 3 months
CREATE TABLE bronze_assets_prev_month PARTITION OF bronze_assets
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '3 months') TO (CURRENT_DATE - INTERVAL '2 months');
CREATE TABLE bronze_assets_curr_month PARTITION OF bronze_assets
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '2 months') TO (CURRENT_DATE - INTERVAL '1 month');
CREATE TABLE bronze_assets_next_month PARTITION OF bronze_assets
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '1 month') TO (CURRENT_DATE + INTERVAL '1 month');

-- Add indexes for common queries
CREATE INDEX idx_bronze_assets_load_status ON bronze_assets(load_status);
CREATE INDEX idx_bronze_assets_api_run ON bronze_assets(api_run_timestamp);

-- ========================
-- 2. bronze_customers Table
-- ========================
CREATE TABLE bronze_customers (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    customer_id VARCHAR(50) NOT NULL,      -- Unique identifier for the customer
    data JSONB NOT NULL,                   -- Raw JSON data containing customer information
    load_timestamp TIMESTAMP NOT NULL,     -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP NOT NULL,  -- When the API call that fetched this data was made
    source_file VARCHAR(255) NOT NULL,     -- Path to the source JSON file
    load_status VARCHAR(20) NOT NULL,      -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    retry_count INTEGER DEFAULT 0,         -- Number of retry attempts
    last_retry_timestamp TIMESTAMP,        -- Timestamp of last retry attempt
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, customer_id, load_timestamp)
) PARTITION BY RANGE (load_timestamp);

-- Create partitions for the last 3 months
CREATE TABLE bronze_customers_prev_month PARTITION OF bronze_customers
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '3 months') TO (CURRENT_DATE - INTERVAL '2 months');
CREATE TABLE bronze_customers_curr_month PARTITION OF bronze_customers
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '2 months') TO (CURRENT_DATE - INTERVAL '1 month');
CREATE TABLE bronze_customers_next_month PARTITION OF bronze_customers
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '1 month') TO (CURRENT_DATE + INTERVAL '1 month');

-- Add indexes for common queries
CREATE INDEX idx_bronze_customers_load_status ON bronze_customers(load_status);
CREATE INDEX idx_bronze_customers_api_run ON bronze_customers(api_run_timestamp);

-- ========================
-- 3. bronze_deposits Table
-- ========================
CREATE TABLE bronze_deposits (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    deposit_id VARCHAR(50) NOT NULL,       -- Unique identifier for the deposit
    data JSONB NOT NULL,                   -- Raw JSON data containing deposit information
    load_timestamp TIMESTAMP NOT NULL,     -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP NOT NULL,  -- When the API call that fetched this data was made
    source_file VARCHAR(255) NOT NULL,     -- Path to the source JSON file
    load_status VARCHAR(20) NOT NULL,      -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    retry_count INTEGER DEFAULT 0,         -- Number of retry attempts
    last_retry_timestamp TIMESTAMP,        -- Timestamp of last retry attempt
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, deposit_id, load_timestamp)
) PARTITION BY RANGE (load_timestamp);

-- Create partitions for the last 3 months
CREATE TABLE bronze_deposits_prev_month PARTITION OF bronze_deposits
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '3 months') TO (CURRENT_DATE - INTERVAL '2 months');
CREATE TABLE bronze_deposits_curr_month PARTITION OF bronze_deposits
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '2 months') TO (CURRENT_DATE - INTERVAL '1 month');
CREATE TABLE bronze_deposits_next_month PARTITION OF bronze_deposits
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '1 month') TO (CURRENT_DATE + INTERVAL '1 month');

-- Add indexes for common queries
CREATE INDEX idx_bronze_deposits_load_status ON bronze_deposits(load_status);
CREATE INDEX idx_bronze_deposits_api_run ON bronze_deposits(api_run_timestamp);

-- ========================
-- 4. bronze_trades Table
-- ========================
CREATE TABLE bronze_trades (
    affiliate_id VARCHAR(50) NOT NULL,     -- Identifier for the affiliate
    trade_id VARCHAR(50) NOT NULL,         -- Unique identifier for the trade
    data JSONB NOT NULL,                   -- Raw JSON data containing trade information
    load_timestamp TIMESTAMP NOT NULL,     -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP NOT NULL,  -- When the API call that fetched this data was made
    source_file VARCHAR(255) NOT NULL,     -- Path to the source JSON file
    load_status VARCHAR(20) NOT NULL,      -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    retry_count INTEGER DEFAULT 0,         -- Number of retry attempts
    last_retry_timestamp TIMESTAMP,        -- Timestamp of last retry attempt
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, trade_id, load_timestamp)
) PARTITION BY RANGE (load_timestamp);

-- Create partitions for the last 3 months
CREATE TABLE bronze_trades_prev_month PARTITION OF bronze_trades
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '3 months') TO (CURRENT_DATE - INTERVAL '2 months');
CREATE TABLE bronze_trades_curr_month PARTITION OF bronze_trades
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '2 months') TO (CURRENT_DATE - INTERVAL '1 month');
CREATE TABLE bronze_trades_next_month PARTITION OF bronze_trades
    FOR VALUES FROM (CURRENT_DATE - INTERVAL '1 month') TO (CURRENT_DATE + INTERVAL '1 month');

-- Add indexes for common queries
CREATE INDEX idx_bronze_trades_load_status ON bronze_trades(load_status);
CREATE INDEX idx_bronze_trades_api_run ON bronze_trades(api_run_timestamp);

-- ========================
-- 5. ETL Status Table
-- ========================
CREATE TABLE etl_status (
    table_name VARCHAR(50) PRIMARY KEY,    -- Name of the table being processed
    last_successful_load TIMESTAMP,        -- Timestamp of last successful load
    last_attempted_load TIMESTAMP,         -- Timestamp of last attempted load
    load_status VARCHAR(20) NOT NULL,      -- Current status of the load
    error_message TEXT,                    -- Any error message if load_status is ERROR
    records_processed INTEGER,             -- Number of records processed in last load
    processing_time INTERVAL,              -- Time taken for last load
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add index for status lookups
CREATE INDEX idx_etl_status_load_status ON etl_status(load_status); 