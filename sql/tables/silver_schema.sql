-- ========================
-- Silver Layer Tables
-- ========================

-- ========================
-- 1. silver_customers Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_customers (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    client_id VARCHAR(50),                 -- Unique identifier for the customer
    register_time TIMESTAMP,               -- When the customer registered
    register_date DATE,                    -- Date portion of register_time
    source_file TEXT,                      -- Path to the source data file
    load_time TIMESTAMP,                   -- When this record was loaded into the database
    PRIMARY KEY (affiliate_id, client_id),
    UNIQUE (affiliate_id, client_id)       -- Additional unique constraint for deduplication
);

-- ========================
-- 2. silver_deposits Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_deposits (
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    client_id VARCHAR(50),                 -- Identifier for the client
    order_id VARCHAR(50),                  -- Unique identifier for the deposit order
    deposit_time TIMESTAMP,                -- When the deposit was made
    deposit_date DATE,                     -- Date portion of deposit_time
    deposit_coin VARCHAR(10),              -- Type of coin deposited
    deposit_amount DECIMAL(18,8),          -- Amount deposited
    source_file TEXT,                      -- Path to the source data file
    load_time TIMESTAMP,                   -- When this record was loaded into the database
    PRIMARY KEY (order_id),
    UNIQUE (order_id)                      -- Additional unique constraint for deduplication
);

-- ========================
-- 3. silver_trades Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_trades (
    id SERIAL,                             -- Auto-incrementing unique identifier
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    client_id VARCHAR(50),                 -- Identifier for the client
    trade_volume DECIMAL(18,8),            -- Volume of the trade
    trade_time TIMESTAMP,                  -- When the trade occurred
    trade_date DATE,                       -- Date portion of trade_time
    source_file TEXT,                      -- Path to the source data file
    load_time TIMESTAMP,                   -- When this record was loaded into the database
    PRIMARY KEY (id),
    UNIQUE (affiliate_id, client_id, trade_time, trade_volume)  -- Natural key for deduplication
);

-- ========================
-- 4. silver_assets Table
-- ========================
CREATE TABLE IF NOT EXISTS silver_assets (
    id SERIAL,                             -- Auto-incrementing unique identifier
    affiliate_id VARCHAR(50),              -- Identifier for the affiliate
    client_id VARCHAR(50),                 -- Identifier for the client
    balance DECIMAL(18,8),                 -- Current balance
    update_time TIMESTAMP,                 -- When the balance was last updated
    update_date DATE,                      -- Date portion of update_time
    remark TEXT,                           -- Additional remarks or notes
    source_file TEXT,                      -- Path to the source data file
    load_time TIMESTAMP,                   -- When this record was loaded into the database
    PRIMARY KEY (id),
    UNIQUE (affiliate_id, client_id, update_time)  -- Natural key for deduplication
);

-- ========================
-- Insert Functions with Deduplication
-- ========================

-- Function to insert customers with deduplication
CREATE OR REPLACE FUNCTION insert_silver_customers(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_register_time TIMESTAMP,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    INSERT INTO silver_customers (
        affiliate_id,
        client_id,
        register_time,
        register_date,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_register_time,
        p_register_time::DATE,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Function to insert deposits with deduplication
CREATE OR REPLACE FUNCTION insert_silver_deposits(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_order_id VARCHAR,
    p_deposit_time TIMESTAMP,
    p_deposit_coin VARCHAR,
    p_deposit_amount DECIMAL,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    INSERT INTO silver_deposits (
        affiliate_id,
        client_id,
        order_id,
        deposit_time,
        deposit_date,
        deposit_coin,
        deposit_amount,
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
        p_source_file,
        p_load_time
    ) ON CONFLICT (order_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Function to insert trades with deduplication
CREATE OR REPLACE FUNCTION insert_silver_trades(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_trade_volume DECIMAL,
    p_trade_time TIMESTAMP,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    INSERT INTO silver_trades (
        affiliate_id,
        client_id,
        trade_volume,
        trade_time,
        trade_date,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_trade_volume,
        p_trade_time,
        p_trade_time::DATE,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id, trade_time, trade_volume) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Function to insert assets with deduplication
CREATE OR REPLACE FUNCTION insert_silver_assets(
    p_affiliate_id VARCHAR,
    p_client_id VARCHAR,
    p_balance DECIMAL,
    p_update_time TIMESTAMP,
    p_remark TEXT,
    p_source_file TEXT,
    p_load_time TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    INSERT INTO silver_assets (
        affiliate_id,
        client_id,
        balance,
        update_time,
        update_date,
        remark,
        source_file,
        load_time
    ) VALUES (
        p_affiliate_id,
        p_client_id,
        p_balance,
        p_update_time,
        p_update_time::DATE,
        p_remark,
        p_source_file,
        p_load_time
    ) ON CONFLICT (affiliate_id, client_id, update_time) DO NOTHING;
END;
$$ LANGUAGE plpgsql; 