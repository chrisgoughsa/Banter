-- ========================
-- Silver Layer Tables
-- ========================

-- ========================
-- 1. AffiliateAccount Table
-- ========================
CREATE TABLE AffiliateAccount (
    affiliate_id STRING PRIMARY KEY,      -- Unique ID for the affiliate
    name STRING NOT NULL,                 -- Affiliate name
    email STRING NOT NULL,                -- Contact email
    join_date TIMESTAMP NOT NULL,         -- Date the affiliate joined
    status STRING NOT NULL DEFAULT 'active', -- Status of the affiliate
    metadata JSON,                        -- Optional: extra information
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
);

-- Add index for status lookups
CREATE INDEX idx_affiliate_status ON AffiliateAccount(status);

-- ========================
-- 2. ClientAccount Table
-- ========================
CREATE TABLE ClientAccount (
    client_id STRING PRIMARY KEY,          -- Unique ID for the client
    affiliate_id STRING NOT NULL,          -- Foreign Key to AffiliateAccount
    register_time TIMESTAMP NOT NULL,      -- Client registration time
    register_date DATE NOT NULL,           -- Date portion of register_time
    country STRING,                        -- Optional: client's country
    status STRING NOT NULL DEFAULT 'active', -- Status of the client
    metadata JSON,                         -- Optional: extra fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL,
    FOREIGN KEY (affiliate_id) REFERENCES AffiliateAccount(affiliate_id)
);

-- Add indexes for common queries
CREATE INDEX idx_client_affiliate ON ClientAccount(affiliate_id);
CREATE INDEX idx_client_register_date ON ClientAccount(register_date);
CREATE INDEX idx_client_status ON ClientAccount(status);

-- ========================
-- 3. Deposits Table
-- ========================
CREATE TABLE Deposits (
    deposit_id STRING PRIMARY KEY,         -- Unique Deposit Order ID
    client_id STRING NOT NULL,             -- Foreign Key to ClientAccount
    deposit_time TIMESTAMP NOT NULL,       -- Deposit timestamp
    deposit_date DATE NOT NULL,            -- Date portion of deposit_time
    deposit_coin STRING NOT NULL,          -- Coin type (e.g., USDT)
    deposit_amount DECIMAL(18,8) NOT NULL, -- Amount deposited
    status STRING NOT NULL DEFAULT 'pending', -- Status of the deposit
    metadata JSON,                         -- Optional: extra fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL,
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);

-- Add indexes for common queries
CREATE INDEX idx_deposit_client ON Deposits(client_id);
CREATE INDEX idx_deposit_date ON Deposits(deposit_date);
CREATE INDEX idx_deposit_status ON Deposits(status);

-- ========================
-- 4. TradeActivities Table
-- ========================
CREATE TABLE TradeActivities (
    trade_activity_id STRING PRIMARY KEY,  -- Synthetic or real trade ID
    client_id STRING NOT NULL,             -- Foreign Key to ClientAccount
    trade_time TIMESTAMP NOT NULL,         -- Trade timestamp
    trade_date DATE NOT NULL,              -- Date portion of trade_time
    symbol STRING NOT NULL,                -- Symbol traded
    trade_volume DECIMAL(18,8) NOT NULL,   -- Volume traded
    trade_type STRING NOT NULL,            -- Type of trade (buy/sell)
    status STRING NOT NULL DEFAULT 'completed', -- Status of the trade
    metadata JSON,                         -- Optional: extra fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL,
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);

-- Add indexes for common queries
CREATE INDEX idx_trade_client ON TradeActivities(client_id);
CREATE INDEX idx_trade_date ON TradeActivities(trade_date);
CREATE INDEX idx_trade_symbol ON TradeActivities(symbol);
CREATE INDEX idx_trade_status ON TradeActivities(status);

-- ========================
-- 5. Assets Table
-- ========================
CREATE TABLE Assets (
    asset_id STRING PRIMARY KEY,           -- Synthetic ID or combination
    client_id STRING NOT NULL,             -- Foreign Key to ClientAccount
    balance DECIMAL(18,8) NOT NULL,        -- Account balance
    last_update_time TIMESTAMP NOT NULL,   -- Last update time
    last_update_date DATE NOT NULL,        -- Date portion of last_update_time
    symbol STRING NOT NULL,                -- Asset symbol
    status STRING NOT NULL DEFAULT 'active', -- Status of the asset
    remark STRING,                         -- Remarks (e.g., "too many sub-accounts")
    metadata JSON,                         -- Optional: extra fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL,
    FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
);

-- Add indexes for common queries
CREATE INDEX idx_asset_client ON Assets(client_id);
CREATE INDEX idx_asset_update_date ON Assets(last_update_date);
CREATE INDEX idx_asset_symbol ON Assets(symbol);
CREATE INDEX idx_asset_status ON Assets(status);

-- ========================
-- 6. Audit Log Table
-- ========================
CREATE TABLE AuditLog (
    audit_id STRING PRIMARY KEY,
    table_name STRING NOT NULL,
    record_id STRING NOT NULL,
    action STRING NOT NULL,
    old_values JSON,
    new_values JSON,
    changed_by STRING NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add index for audit queries
CREATE INDEX idx_audit_table_record ON AuditLog(table_name, record_id);
CREATE INDEX idx_audit_changed_at ON AuditLog(changed_at);