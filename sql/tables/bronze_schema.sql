-- ========================
-- Bronze Layer Tables
-- ========================

-- ========================
-- 1. bronze_assets Table
-- ========================
CREATE TABLE bronze_assets (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    asset_id VARCHAR(50),                  -- Unique identifier for the asset
    symbol VARCHAR(50),                    -- Trading symbol
    name VARCHAR(100),                     -- Asset name
    data JSONB,                            -- Raw JSON data containing additional asset information
    load_timestamp TIMESTAMP,              -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP,           -- When the API call that fetched this data was made
    source_file VARCHAR(255),              -- Path to the source JSON file
    load_status VARCHAR(20),               -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    PRIMARY KEY (affiliate_id, asset_id)
);

-- ========================
-- 2. bronze_customers Table
-- ========================
CREATE TABLE bronze_customers (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    customer_id VARCHAR(50),               -- Unique identifier for the customer
    data JSONB,                            -- Raw JSON data containing customer information
    load_timestamp TIMESTAMP,              -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP,           -- When the API call that fetched this data was made
    source_file VARCHAR(255),              -- Path to the source JSON file
    load_status VARCHAR(20),               -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    PRIMARY KEY (affiliate_id, customer_id)
);

-- ========================
-- 3. bronze_deposits Table
-- ========================
CREATE TABLE bronze_deposits (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    deposit_id VARCHAR(50),                -- Unique identifier for the deposit
    data JSONB,                            -- Raw JSON data containing deposit information
    load_timestamp TIMESTAMP,              -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP,           -- When the API call that fetched this data was made
    source_file VARCHAR(255),              -- Path to the source JSON file
    load_status VARCHAR(20),               -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    PRIMARY KEY (affiliate_id, deposit_id)
);

-- ========================
-- 4. bronze_trades Table
-- ========================
CREATE TABLE bronze_trades (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    trade_id VARCHAR(50),                  -- Unique identifier for the trade
    data JSONB,                            -- Raw JSON data containing trade information
    load_timestamp TIMESTAMP,              -- When this record was loaded into the database
    api_run_timestamp TIMESTAMP,           -- When the API call that fetched this data was made
    source_file VARCHAR(255),              -- Path to the source JSON file
    load_status VARCHAR(20),               -- Status of the load (SUCCESS, ERROR, PARTIAL)
    error_message TEXT,                    -- Any error message if load_status is ERROR
    PRIMARY KEY (affiliate_id, trade_id)
); 