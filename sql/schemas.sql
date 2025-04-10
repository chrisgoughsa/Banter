-- ========================
-- 1. Bronze Layer Tables
-- ========================

-- bronze_customers table
CREATE TABLE IF NOT EXISTS bronze_customers (
    affiliate_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(50) NOT NULL,
    register_time TIMESTAMP NOT NULL,
    last_login_time TIMESTAMP,
    email VARCHAR(255),
    phone VARCHAR(50),
    country VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    metadata JSONB,
    source_file TEXT NOT NULL,
    load_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id, client_id)
);

-- bronze_deposits table
CREATE TABLE IF NOT EXISTS bronze_deposits (
    order_id VARCHAR(50) NOT NULL,
    affiliate_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(50) NOT NULL,
    deposit_time TIMESTAMP NOT NULL,
    deposit_coin VARCHAR(20) NOT NULL,
    deposit_amount DECIMAL(18,8) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    metadata JSONB,
    source_file TEXT NOT NULL,
    load_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id)
);

-- bronze_trades table
CREATE TABLE IF NOT EXISTS bronze_trades (
    client_id VARCHAR(50) NOT NULL,
    trade_time TIMESTAMP NOT NULL,
    affiliate_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    size DECIMAL(18,8) NOT NULL,
    fee DECIMAL(18,8) NOT NULL,
    trade_volume DECIMAL(18,8) NOT NULL,
    metadata JSONB,
    source_file TEXT NOT NULL,
    load_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, trade_time)
);

-- bronze_assets table
CREATE TABLE IF NOT EXISTS bronze_assets (
    client_id VARCHAR(50) NOT NULL,
    update_time TIMESTAMP NOT NULL,
    affiliate_id VARCHAR(50) NOT NULL,
    asset VARCHAR(20) NOT NULL,
    balance DECIMAL(18,8) NOT NULL,
    available_balance DECIMAL(18,8) NOT NULL,
    frozen_balance DECIMAL(18,8) NOT NULL,
    metadata JSONB,
    source_file TEXT NOT NULL,
    load_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, update_time)
);

-- ========================
-- 2. Silver Layer Tables
-- ========================

-- clientaccount table
CREATE TABLE IF NOT EXISTS clientaccount (
    client_id VARCHAR(50) NOT NULL,
    affiliate_id VARCHAR(50) NOT NULL,
    register_time TIMESTAMP NOT NULL,
    last_login_time TIMESTAMP,
    email VARCHAR(255),
    phone VARCHAR(50),
    country VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id)
);

-- affiliateaccount table
CREATE TABLE IF NOT EXISTS affiliateaccount (
    affiliate_id VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    country VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (affiliate_id)
);

-- deposits table
CREATE TABLE IF NOT EXISTS deposits (
    deposit_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(50) NOT NULL,
    deposit_time TIMESTAMP NOT NULL,
    currency VARCHAR(20) NOT NULL,
    amount DECIMAL(18,8) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (deposit_id),
    FOREIGN KEY (client_id) REFERENCES clientaccount(client_id)
);

-- tradeactivities table
CREATE TABLE IF NOT EXISTS tradeactivities (
    client_id VARCHAR(50) NOT NULL,
    trade_time TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    size DECIMAL(18,8) NOT NULL,
    fee DECIMAL(18,8) NOT NULL,
    trade_volume DECIMAL(18,8) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, trade_time),
    FOREIGN KEY (client_id) REFERENCES clientaccount(client_id)
);

-- ========================
-- 3. Gold Layer Views
-- ========================

-- gold_affiliate_daily_metrics view
CREATE OR REPLACE VIEW gold_affiliate_daily_metrics AS
WITH daily_signups AS (
    SELECT 
        DATE_TRUNC('day', register_time) as date,
        affiliate_id,
        COUNT(*) as new_signups
    FROM clientaccount
    GROUP BY DATE_TRUNC('day', register_time), affiliate_id
),
daily_trades AS (
    SELECT 
        DATE_TRUNC('day', trade_time) as date,
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume
    FROM tradeactivities t
    JOIN clientaccount c ON t.client_id = c.client_id
    GROUP BY DATE_TRUNC('day', trade_time), c.affiliate_id
)
SELECT 
    COALESCE(s.date, t.date) as date,
    COALESCE(s.affiliate_id, t.affiliate_id) as affiliate_id,
    a.name as affiliate_name,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume,
    CURRENT_TIMESTAMP as last_updated,
    'daily_metrics' as view_type
FROM daily_signups s
FULL OUTER JOIN daily_trades t 
    ON s.date = t.date AND s.affiliate_id = t.affiliate_id
LEFT JOIN affiliateaccount a 
    ON COALESCE(s.affiliate_id, t.affiliate_id) = a.affiliate_id
ORDER BY date DESC, affiliate_id;

-- gold_affiliate_monthly_metrics view
CREATE OR REPLACE VIEW gold_affiliate_monthly_metrics AS
WITH monthly_signups AS (
    SELECT 
        DATE_TRUNC('month', register_time) as month,
        affiliate_id,
        COUNT(*) as new_signups
    FROM clientaccount
    GROUP BY DATE_TRUNC('month', register_time), affiliate_id
),
monthly_trades AS (
    SELECT 
        DATE_TRUNC('month', trade_time) as month,
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume,
        COUNT(DISTINCT t.client_id) as active_traders
    FROM tradeactivities t
    JOIN clientaccount c ON t.client_id = c.client_id
    GROUP BY DATE_TRUNC('month', trade_time), c.affiliate_id
),
monthly_deposits AS (
    SELECT 
        DATE_TRUNC('month', deposit_time) as month,
        c.affiliate_id,
        COUNT(*) as total_deposits,
        SUM(amount) as deposit_volume
    FROM deposits d
    JOIN clientaccount c ON d.client_id = c.client_id
    GROUP BY DATE_TRUNC('month', deposit_time), c.affiliate_id
)
SELECT 
    COALESCE(s.month, t.month, d.month) as month,
    COALESCE(s.affiliate_id, t.affiliate_id, d.affiliate_id) as affiliate_id,
    a.name as affiliate_name,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as trading_volume,
    COALESCE(t.active_traders, 0) as active_traders,
    COALESCE(d.total_deposits, 0) as total_deposits,
    COALESCE(d.deposit_volume, 0) as deposit_volume,
    CURRENT_TIMESTAMP as last_updated,
    'monthly_metrics' as view_type
FROM monthly_signups s
FULL OUTER JOIN monthly_trades t 
    ON s.month = t.month AND s.affiliate_id = t.affiliate_id
FULL OUTER JOIN monthly_deposits d
    ON s.month = d.month AND s.affiliate_id = d.affiliate_id
LEFT JOIN affiliateaccount a 
    ON COALESCE(s.affiliate_id, t.affiliate_id, d.affiliate_id) = a.affiliate_id
ORDER BY month DESC, affiliate_id;

-- gold_affiliate_performance view
CREATE OR REPLACE VIEW gold_affiliate_performance AS
WITH affiliate_metrics AS (
    SELECT 
        a.affiliate_id,
        a.name as affiliate_name,
        COUNT(DISTINCT c.client_id) as total_customers,
        COUNT(DISTINCT CASE 
            WHEN c.register_time >= CURRENT_DATE - INTERVAL '30 days' 
            THEN c.client_id 
        END) as new_signups_30d,
        COUNT(DISTINCT CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM tradeactivities t 
                WHERE t.client_id = c.client_id 
                AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
            ) 
            THEN c.client_id 
        END) as active_customers_30d
    FROM affiliateaccount a
    LEFT JOIN clientaccount c ON a.affiliate_id = c.affiliate_id
    GROUP BY a.affiliate_id, a.name
),
trading_metrics AS (
    SELECT 
        c.affiliate_id,
        COUNT(DISTINCT t.client_id) as trades_30d,
        COALESCE(SUM(t.trade_volume), 0) as trading_volume_30d,
        CASE 
            WHEN COUNT(DISTINCT t.client_id) > 0 
            THEN COALESCE(SUM(t.trade_volume), 0) / COUNT(DISTINCT t.client_id)
            ELSE 0 
        END as avg_trade_size
    FROM clientaccount c
    LEFT JOIN tradeactivities t ON c.client_id = t.client_id
    WHERE t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY c.affiliate_id
)
SELECT 
    m.affiliate_id,
    m.affiliate_name,
    m.total_customers,
    m.new_signups_30d,
    m.active_customers_30d,
    COALESCE(t.trades_30d, 0) as trades_30d,
    COALESCE(t.trading_volume_30d, 0) as trading_volume_30d,
    COALESCE(t.avg_trade_size, 0) as avg_trade_size,
    CASE 
        WHEN m.total_customers > 0 
        THEN ROUND(100.0 * m.active_customers_30d / m.total_customers, 2)
        ELSE 0 
    END as monthly_activation_rate,
    CURRENT_TIMESTAMP as last_updated,
    'performance_metrics' as view_type
FROM affiliate_metrics m
LEFT JOIN trading_metrics t ON m.affiliate_id = t.affiliate_id
ORDER BY trading_volume_30d DESC;

-- gold_etl_affiliate_dashboard view
CREATE OR REPLACE VIEW gold_etl_affiliate_dashboard AS
WITH bronze_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(DISTINCT client_id) as bronze_customers,
        CURRENT_TIMESTAMP as bronze_update_time
    FROM bronze_customers
    GROUP BY affiliate_id
),
silver_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(DISTINCT client_id) as silver_customers,
        CURRENT_TIMESTAMP as silver_update_time
    FROM clientaccount
    GROUP BY affiliate_id
),
gold_metrics AS (
    SELECT 
        affiliate_id,
        CURRENT_TIMESTAMP as gold_update_time
    FROM gold_affiliate_daily_metrics
    GROUP BY affiliate_id
),
etl_status AS (
    SELECT 
        COALESCE(b.affiliate_id, s.affiliate_id, g.affiliate_id) as affiliate_id,
        a.name as affiliate_name,
        b.bronze_customers,
        b.bronze_update_time,
        s.silver_customers,
        s.silver_update_time,
        g.gold_update_time,
        CASE 
            WHEN b.bronze_customers > 0 AND s.silver_customers > 0 AND g.gold_update_time IS NOT NULL 
            THEN 'Complete'
            WHEN b.bronze_customers > 0 AND s.silver_customers > 0 
            THEN 'Silver Complete'
            WHEN b.bronze_customers > 0 
            THEN 'Bronze Complete'
            ELSE 'Not Started'
        END as etl_status,
        CASE 
            WHEN b.bronze_customers > 0 AND s.silver_customers > 0 AND g.gold_update_time IS NOT NULL 
            THEN 100
            WHEN b.bronze_customers > 0 AND s.silver_customers > 0 
            THEN 66
            WHEN b.bronze_customers > 0 
            THEN 33
            ELSE 0
        END as completion_percentage
    FROM bronze_metrics b
    FULL OUTER JOIN silver_metrics s ON b.affiliate_id = s.affiliate_id
    FULL OUTER JOIN gold_metrics g ON b.affiliate_id = g.affiliate_id
    LEFT JOIN affiliateaccount a ON COALESCE(b.affiliate_id, s.affiliate_id, g.affiliate_id) = a.affiliate_id
)
SELECT 
    affiliate_id,
    affiliate_name,
    bronze_customers as total_records,
    CASE 
        WHEN etl_status = 'Complete' THEN bronze_customers
        ELSE 0
    END as success_count,
    CASE 
        WHEN etl_status = 'Not Started' THEN bronze_customers
        ELSE 0
    END as error_count,
    CASE 
        WHEN etl_status IN ('Bronze Complete', 'Silver Complete') THEN bronze_customers
        ELSE 0
    END as partial_count,
    completion_percentage as success_rate,
    bronze_update_time,
    silver_update_time,
    gold_update_time,
    etl_status,
    CURRENT_TIMESTAMP as last_updated
FROM etl_status
ORDER BY affiliate_id; 