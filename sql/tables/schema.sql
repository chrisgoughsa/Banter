-- ========================
-- Silver Layer Tables
-- ========================

-- ========================
-- 1. AffiliateAccount Table
-- ========================
CREATE TABLE AffiliateAccount (
    affiliate_id STRING PRIMARY KEY,      -- Unique ID for the affiliate
    name STRING,                           -- Affiliate name
    email STRING,                          -- Contact email
    join_date TIMESTAMP,                   -- Date the affiliate joined
    metadata JSON                          -- Optional: extra information
);

-- ========================
-- 2. ClientAccount Table
-- ========================
CREATE TABLE ClientAccount (
    client_id STRING PRIMARY KEY,          -- Unique ID for the client
    affiliate_id STRING,                   -- Foreign Key to AffiliateAccount
    register_time TIMESTAMP,               -- Client registration time
    country STRING,                        -- Optional: client's country
    metadata JSON,                         -- Optional: extra fields
    FOREIGN KEY (affiliate_id) REFERENCES AffiliateAccount(affiliate_id)
);

-- ========================
-- 3. Deposits Table
-- ========================
CREATE TABLE Deposits (
    deposit_id STRING PRIMARY KEY,         -- Unique Deposit Order ID
    client_id STRING,                      -- Foreign Key to ClientAccount
    deposit_time TIMESTAMP,                -- Deposit timestamp
    deposit_coin STRING,                   -- Coin type (e.g., USDT)
    deposit_amount FLOAT64,                -- Amount deposited
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);

-- ========================
-- 4. TradeActivities Table
-- ========================
CREATE TABLE TradeActivities (
    trade_activity_id STRING PRIMARY KEY,  -- Synthetic or real trade ID
    client_id STRING,                      -- Foreign Key to ClientAccount
    trade_time TIMESTAMP,                  -- Trade timestamp
    symbol STRING,                         -- Symbol traded (optional)
    trade_volume FLOAT64,                  -- Volume traded
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);

-- ========================
-- 5. Assets Table
-- ========================
CREATE TABLE Assets (
    asset_id STRING PRIMARY KEY,           -- Synthetic ID or combination
    client_id STRING,                      -- Foreign Key to ClientAccount
    balance FLOAT64,                       -- Account balance
    last_update_time TIMESTAMP,            -- Last update time
    remark STRING,                         -- Remarks (e.g., "too many sub-accounts")
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);