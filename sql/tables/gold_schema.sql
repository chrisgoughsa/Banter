-- ========================
-- Gold Layer Views
-- ========================

-- ========================
-- 1. Daily Affiliate Metrics View
-- ========================
CREATE OR REPLACE VIEW gold_affiliate_daily_metrics AS
WITH daily_signups AS (
    SELECT 
        DATE_TRUNC('day', register_time) as date,
        affiliate_id,
        COUNT(*) as new_signups
    FROM ClientAccount
    GROUP BY DATE_TRUNC('day', register_time), affiliate_id
),
daily_trades AS (
    SELECT 
        DATE_TRUNC('day', trade_time) as date,
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    GROUP BY DATE_TRUNC('day', trade_time), c.affiliate_id
)
SELECT 
    COALESCE(s.date, t.date) as date,
    COALESCE(s.affiliate_id, t.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume
FROM daily_signups s
FULL OUTER JOIN daily_trades t ON s.date = t.date AND s.affiliate_id = t.affiliate_id;

-- ========================
-- 2. Weekly Affiliate Metrics View
-- ========================
CREATE OR REPLACE VIEW gold_affiliate_weekly_metrics AS
WITH weekly_signups AS (
    SELECT 
        DATE_TRUNC('week', register_time) as week_start,
        affiliate_id,
        COUNT(*) as new_signups
    FROM ClientAccount
    GROUP BY DATE_TRUNC('week', register_time), affiliate_id
),
weekly_trades AS (
    SELECT 
        DATE_TRUNC('week', trade_time) as week_start,
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    GROUP BY DATE_TRUNC('week', trade_time), c.affiliate_id
)
SELECT 
    COALESCE(s.week_start, t.week_start) as week_start,
    COALESCE(s.affiliate_id, t.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume
FROM weekly_signups s
FULL OUTER JOIN weekly_trades t ON s.week_start = t.week_start AND s.affiliate_id = t.affiliate_id;

-- ========================
-- 3. Monthly Affiliate Metrics View
-- ========================
CREATE OR REPLACE VIEW gold_affiliate_monthly_metrics AS
WITH monthly_signups AS (
    SELECT 
        DATE_TRUNC('month', register_time) as month_start,
        affiliate_id,
        COUNT(*) as new_signups
    FROM ClientAccount
    GROUP BY DATE_TRUNC('month', register_time), affiliate_id
),
monthly_trades AS (
    SELECT 
        DATE_TRUNC('month', trade_time) as month_start,
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    GROUP BY DATE_TRUNC('month', trade_time), c.affiliate_id
)
SELECT 
    COALESCE(s.month_start, t.month_start) as month_start,
    COALESCE(s.affiliate_id, t.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume
FROM monthly_signups s
FULL OUTER JOIN monthly_trades t ON s.month_start = t.month_start AND s.affiliate_id = t.affiliate_id;

-- ========================
-- 4. Affiliate Customer Funnel View
-- ========================
CREATE OR REPLACE VIEW gold_affiliate_customer_funnel AS
WITH customer_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(*) as total_customers,
        COUNT(CASE WHEN register_time >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as new_customers_30d,
        COUNT(CASE WHEN EXISTS (
            SELECT 1 FROM TradeActivities t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
        ) THEN 1 END) as active_customers_30d
    FROM ClientAccount c
    GROUP BY affiliate_id
),
trading_metrics AS (
    SELECT 
        c.affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    WHERE t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY c.affiliate_id
)
SELECT 
    c.affiliate_id,
    c.total_customers,
    c.new_customers_30d,
    c.active_customers_30d,
    COALESCE(t.total_trades, 0) as trades_30d,
    COALESCE(t.total_volume, 0) as volume_30d
FROM customer_metrics c
LEFT JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id;

-- ========================
-- 5. Affiliate Performance Trend View
-- ========================
CREATE OR REPLACE VIEW gold_affiliate_performance_trend AS
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', trade_time) as date,
        c.affiliate_id,
        COUNT(*) as daily_trades,
        SUM(trade_volume) as daily_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', trade_time), c.affiliate_id
),
weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', trade_time) as week,
        c.affiliate_id,
        COUNT(*) as weekly_trades,
        SUM(trade_volume) as weekly_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('week', trade_time), c.affiliate_id
),
monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', trade_time) as month,
        c.affiliate_id,
        COUNT(*) as monthly_trades,
        SUM(trade_volume) as monthly_volume
    FROM TradeActivities t
    JOIN ClientAccount c ON t.client_id = c.client_id
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('month', trade_time), c.affiliate_id
)
SELECT 
    d.date,
    d.affiliate_id,
    d.daily_trades,
    d.daily_volume,
    w.weekly_trades,
    w.weekly_volume,
    m.monthly_trades,
    m.monthly_volume
FROM daily_metrics d
LEFT JOIN weekly_metrics w ON d.affiliate_id = w.affiliate_id 
    AND DATE_TRUNC('week', d.date) = w.week
LEFT JOIN monthly_metrics m ON d.affiliate_id = m.affiliate_id 
    AND DATE_TRUNC('month', d.date) = m.month
ORDER BY d.date DESC, d.affiliate_id;

-- ========================
-- 6. ETL and Affiliate Performance Dashboard View
-- ========================
CREATE OR REPLACE VIEW gold_etl_affiliate_dashboard AS
WITH etl_status AS (
    -- Get latest ETL run status for each table
    SELECT 
        CASE 
            WHEN table_name = 'bronze_assets' THEN 'Assets'
            WHEN table_name = 'bronze_customers' THEN 'Customers'
            WHEN table_name = 'bronze_deposits' THEN 'Deposits'
            WHEN table_name = 'bronze_trades' THEN 'Trades'
        END as data_source,
        MAX(load_timestamp) as last_load_time,
        MAX(api_run_timestamp) as last_api_run,
        COUNT(*) as total_records,
        COUNT(CASE WHEN load_status = 'SUCCESS' THEN 1 END) as success_count,
        COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END) as error_count,
        COUNT(CASE WHEN load_status = 'PARTIAL' THEN 1 END) as partial_count
    FROM (
        SELECT 'bronze_assets' as table_name, load_timestamp, api_run_timestamp, load_status FROM bronze_assets
        UNION ALL
        SELECT 'bronze_customers' as table_name, load_timestamp, api_run_timestamp, load_status FROM bronze_customers
        UNION ALL
        SELECT 'bronze_deposits' as table_name, load_timestamp, api_run_timestamp, load_status FROM bronze_deposits
        UNION ALL
        SELECT 'bronze_trades' as table_name, load_timestamp, api_run_timestamp, load_status FROM bronze_trades
    ) combined
    GROUP BY table_name
),
affiliate_performance AS (
    -- Get current month's affiliate performance
    SELECT 
        a.affiliate_id,
        a.name as affiliate_name,
        COUNT(DISTINCT c.client_id) as total_customers,
        COUNT(DISTINCT CASE WHEN c.register_time >= DATE_TRUNC('month', CURRENT_DATE) THEN c.client_id END) as new_signups_this_month,
        COUNT(DISTINCT CASE WHEN c.register_time >= CURRENT_DATE - INTERVAL '30 days' THEN c.client_id END) as new_signups_30d,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM TradeActivities t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
        ) THEN c.client_id END) as active_customers_30d,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM TradeActivities t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= DATE_TRUNC('month', CURRENT_DATE)
        ) THEN c.client_id END) as active_customers_this_month,
        COALESCE(SUM(CASE WHEN t.trade_time >= DATE_TRUNC('month', CURRENT_DATE) THEN t.trade_volume ELSE 0 END), 0) as monthly_trading_volume,
        COALESCE(SUM(CASE WHEN t.trade_time >= CURRENT_DATE - INTERVAL '30 days' THEN t.trade_volume ELSE 0 END), 0) as trading_volume_30d,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= DATE_TRUNC('month', CURRENT_DATE) THEN t.trade_activity_id END), 0) as monthly_trades,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= CURRENT_DATE - INTERVAL '30 days' THEN t.trade_activity_id END), 0) as trades_30d
    FROM AffiliateAccount a
    LEFT JOIN ClientAccount c ON a.affiliate_id = c.affiliate_id
    LEFT JOIN TradeActivities t ON c.client_id = t.client_id
    GROUP BY a.affiliate_id, a.name
)
SELECT 
    -- ETL Status
    e.data_source,
    e.last_load_time,
    e.last_api_run,
    e.total_records,
    e.success_count,
    e.error_count,
    e.partial_count,
    CASE 
        WHEN e.error_count > 0 THEN 'ERROR'
        WHEN e.partial_count > 0 THEN 'WARNING'
        ELSE 'SUCCESS'
    END as etl_status,
    
    -- Affiliate Performance
    a.affiliate_id,
    a.affiliate_name,
    a.total_customers,
    a.new_signups_this_month,
    a.new_signups_30d,
    a.active_customers_30d,
    a.active_customers_this_month,
    a.monthly_trading_volume,
    a.trading_volume_30d,
    a.monthly_trades,
    a.trades_30d,
    
    -- Performance Metrics
    CASE 
        WHEN a.total_customers > 0 THEN 
            ROUND(a.active_customers_this_month::FLOAT / a.total_customers * 100, 2)
        ELSE 0 
    END as monthly_activation_rate,
    CASE 
        WHEN a.monthly_trades > 0 THEN 
            ROUND(a.monthly_trading_volume::FLOAT / a.monthly_trades, 2)
        ELSE 0 
    END as avg_trade_size
FROM etl_status e
CROSS JOIN affiliate_performance a
ORDER BY a.monthly_trading_volume DESC, e.data_source; 