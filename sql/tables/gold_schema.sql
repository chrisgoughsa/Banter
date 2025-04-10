-- ========================
-- Gold Layer Views and Materialized Views
-- ========================

-- ========================
-- 1. Daily Affiliate Metrics Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_affiliate_daily_metrics AS
WITH daily_signups AS (
    SELECT 
        DATE_TRUNC('day', register_time) as date,
        affiliate_id,
        COUNT(*) as new_signups,
        COUNT(DISTINCT country) as countries_represented
    FROM silver_customers
    GROUP BY DATE_TRUNC('day', register_time), affiliate_id
),
daily_trades AS (
    SELECT 
        DATE_TRUNC('day', trade_time) as date,
        affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume,
        COUNT(DISTINCT symbol) as unique_symbols_traded,
        COUNT(DISTINCT client_id) as active_traders
    FROM silver_trades
    GROUP BY DATE_TRUNC('day', trade_time), affiliate_id
),
daily_deposits AS (
    SELECT 
        DATE_TRUNC('day', deposit_time) as date,
        affiliate_id,
        COUNT(*) as total_deposits,
        SUM(deposit_amount) as total_deposit_amount,
        COUNT(DISTINCT deposit_coin) as unique_coins_deposited,
        COUNT(DISTINCT client_id) as depositing_clients
    FROM silver_deposits
    GROUP BY DATE_TRUNC('day', deposit_time), affiliate_id
)
SELECT 
    COALESCE(s.date, t.date, d.date) as date,
    COALESCE(s.affiliate_id, t.affiliate_id, d.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(s.countries_represented, 0) as countries_represented,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume,
    COALESCE(t.unique_symbols_traded, 0) as unique_symbols_traded,
    COALESCE(t.active_traders, 0) as active_traders,
    COALESCE(d.total_deposits, 0) as total_deposits,
    COALESCE(d.total_deposit_amount, 0) as total_deposit_amount,
    COALESCE(d.unique_coins_deposited, 0) as unique_coins_deposited,
    COALESCE(d.depositing_clients, 0) as depositing_clients
FROM daily_signups s
FULL OUTER JOIN daily_trades t ON s.date = t.date AND s.affiliate_id = t.affiliate_id
FULL OUTER JOIN daily_deposits d ON s.date = d.date AND s.affiliate_id = d.affiliate_id;

-- Add index for faster queries
CREATE INDEX idx_gold_affiliate_daily_metrics_date ON gold_affiliate_daily_metrics(date);
CREATE INDEX idx_gold_affiliate_daily_metrics_affiliate ON gold_affiliate_daily_metrics(affiliate_id);

-- ========================
-- 2. Weekly Affiliate Metrics Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_affiliate_weekly_metrics AS
WITH weekly_signups AS (
    SELECT 
        DATE_TRUNC('week', register_time) as week_start,
        affiliate_id,
        COUNT(*) as new_signups,
        COUNT(DISTINCT country) as countries_represented
    FROM silver_customers
    GROUP BY DATE_TRUNC('week', register_time), affiliate_id
),
weekly_trades AS (
    SELECT 
        DATE_TRUNC('week', trade_time) as week_start,
        affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume,
        COUNT(DISTINCT symbol) as unique_symbols_traded,
        COUNT(DISTINCT client_id) as active_traders
    FROM silver_trades
    GROUP BY DATE_TRUNC('week', trade_time), affiliate_id
),
weekly_deposits AS (
    SELECT 
        DATE_TRUNC('week', deposit_time) as week_start,
        affiliate_id,
        COUNT(*) as total_deposits,
        SUM(deposit_amount) as total_deposit_amount,
        COUNT(DISTINCT deposit_coin) as unique_coins_deposited,
        COUNT(DISTINCT client_id) as depositing_clients
    FROM silver_deposits
    GROUP BY DATE_TRUNC('week', deposit_time), affiliate_id
)
SELECT 
    COALESCE(s.week_start, t.week_start, d.week_start) as week_start,
    COALESCE(s.affiliate_id, t.affiliate_id, d.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(s.countries_represented, 0) as countries_represented,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume,
    COALESCE(t.unique_symbols_traded, 0) as unique_symbols_traded,
    COALESCE(t.active_traders, 0) as active_traders,
    COALESCE(d.total_deposits, 0) as total_deposits,
    COALESCE(d.total_deposit_amount, 0) as total_deposit_amount,
    COALESCE(d.unique_coins_deposited, 0) as unique_coins_deposited,
    COALESCE(d.depositing_clients, 0) as depositing_clients
FROM weekly_signups s
FULL OUTER JOIN weekly_trades t ON s.week_start = t.week_start AND s.affiliate_id = t.affiliate_id
FULL OUTER JOIN weekly_deposits d ON s.week_start = d.week_start AND s.affiliate_id = d.affiliate_id;

-- Add index for faster queries
CREATE INDEX idx_gold_affiliate_weekly_metrics_week ON gold_affiliate_weekly_metrics(week_start);
CREATE INDEX idx_gold_affiliate_weekly_metrics_affiliate ON gold_affiliate_weekly_metrics(affiliate_id);

-- ========================
-- 3. Monthly Affiliate Metrics Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_affiliate_monthly_metrics AS
WITH monthly_signups AS (
    SELECT 
        DATE_TRUNC('month', register_time) as month_start,
        affiliate_id,
        COUNT(*) as new_signups,
        COUNT(DISTINCT country) as countries_represented
    FROM silver_customers
    GROUP BY DATE_TRUNC('month', register_time), affiliate_id
),
monthly_trades AS (
    SELECT 
        DATE_TRUNC('month', trade_time) as month_start,
        affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume,
        COUNT(DISTINCT symbol) as unique_symbols_traded,
        COUNT(DISTINCT client_id) as active_traders
    FROM silver_trades
    GROUP BY DATE_TRUNC('month', trade_time), affiliate_id
),
monthly_deposits AS (
    SELECT 
        DATE_TRUNC('month', deposit_time) as month_start,
        affiliate_id,
        COUNT(*) as total_deposits,
        SUM(deposit_amount) as total_deposit_amount,
        COUNT(DISTINCT deposit_coin) as unique_coins_deposited,
        COUNT(DISTINCT client_id) as depositing_clients
    FROM silver_deposits
    GROUP BY DATE_TRUNC('month', deposit_time), affiliate_id
)
SELECT 
    COALESCE(s.month_start, t.month_start, d.month_start) as month_start,
    COALESCE(s.affiliate_id, t.affiliate_id, d.affiliate_id) as affiliate_id,
    COALESCE(s.new_signups, 0) as new_signups,
    COALESCE(s.countries_represented, 0) as countries_represented,
    COALESCE(t.total_trades, 0) as total_trades,
    COALESCE(t.total_volume, 0) as total_volume,
    COALESCE(t.unique_symbols_traded, 0) as unique_symbols_traded,
    COALESCE(t.active_traders, 0) as active_traders,
    COALESCE(d.total_deposits, 0) as total_deposits,
    COALESCE(d.total_deposit_amount, 0) as total_deposit_amount,
    COALESCE(d.unique_coins_deposited, 0) as unique_coins_deposited,
    COALESCE(d.depositing_clients, 0) as depositing_clients
FROM monthly_signups s
FULL OUTER JOIN monthly_trades t ON s.month_start = t.month_start AND s.affiliate_id = t.affiliate_id
FULL OUTER JOIN monthly_deposits d ON s.month_start = d.month_start AND s.affiliate_id = d.affiliate_id;

-- Add index for faster queries
CREATE INDEX idx_gold_affiliate_monthly_metrics_month ON gold_affiliate_monthly_metrics(month_start);
CREATE INDEX idx_gold_affiliate_monthly_metrics_affiliate ON gold_affiliate_monthly_metrics(affiliate_id);

-- ========================
-- 4. Affiliate Customer Funnel Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_affiliate_customer_funnel AS
WITH customer_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(*) as total_customers,
        COUNT(DISTINCT country) as countries_represented,
        COUNT(CASE WHEN register_time >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as new_customers_30d,
        COUNT(CASE WHEN EXISTS (
            SELECT 1 FROM silver_trades t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
        ) THEN 1 END) as active_customers_30d,
        COUNT(CASE WHEN EXISTS (
            SELECT 1 FROM silver_deposits d 
            WHERE d.client_id = c.client_id 
            AND d.deposit_time >= CURRENT_DATE - INTERVAL '30 days'
        ) THEN 1 END) as depositing_customers_30d
    FROM silver_customers c
    GROUP BY affiliate_id
),
trading_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(*) as total_trades,
        SUM(trade_volume) as total_volume,
        COUNT(DISTINCT symbol) as unique_symbols_traded,
        COUNT(DISTINCT client_id) as active_traders
    FROM silver_trades
    WHERE trade_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY affiliate_id
),
deposit_metrics AS (
    SELECT 
        affiliate_id,
        COUNT(*) as total_deposits,
        SUM(deposit_amount) as total_deposit_amount,
        COUNT(DISTINCT deposit_coin) as unique_coins_deposited,
        COUNT(DISTINCT client_id) as depositing_clients
    FROM silver_deposits
    WHERE deposit_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY affiliate_id
)
SELECT 
    c.affiliate_id,
    c.total_customers,
    c.countries_represented,
    c.new_customers_30d,
    c.active_customers_30d,
    c.depositing_customers_30d,
    COALESCE(t.total_trades, 0) as trades_30d,
    COALESCE(t.total_volume, 0) as volume_30d,
    COALESCE(t.unique_symbols_traded, 0) as unique_symbols_traded_30d,
    COALESCE(t.active_traders, 0) as active_traders_30d,
    COALESCE(d.total_deposits, 0) as deposits_30d,
    COALESCE(d.total_deposit_amount, 0) as deposit_amount_30d,
    COALESCE(d.unique_coins_deposited, 0) as unique_coins_deposited_30d,
    COALESCE(d.depositing_clients, 0) as depositing_clients_30d
FROM customer_metrics c
LEFT JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
LEFT JOIN deposit_metrics d ON c.affiliate_id = d.affiliate_id;

-- Add index for faster queries
CREATE INDEX idx_gold_affiliate_customer_funnel_affiliate ON gold_affiliate_customer_funnel(affiliate_id);

-- ========================
-- 5. Affiliate Performance Trend Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_affiliate_performance_trend AS
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', trade_time) as date,
        affiliate_id,
        COUNT(*) as daily_trades,
        SUM(trade_volume) as daily_volume,
        COUNT(DISTINCT symbol) as daily_symbols,
        COUNT(DISTINCT client_id) as daily_traders
    FROM silver_trades
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', trade_time), affiliate_id
),
weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', trade_time) as week,
        affiliate_id,
        COUNT(*) as weekly_trades,
        SUM(trade_volume) as weekly_volume,
        COUNT(DISTINCT symbol) as weekly_symbols,
        COUNT(DISTINCT client_id) as weekly_traders
    FROM silver_trades
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('week', trade_time), affiliate_id
),
monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', trade_time) as month,
        affiliate_id,
        COUNT(*) as monthly_trades,
        SUM(trade_volume) as monthly_volume,
        COUNT(DISTINCT symbol) as monthly_symbols,
        COUNT(DISTINCT client_id) as monthly_traders
    FROM silver_trades
    WHERE trade_time >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('month', trade_time), affiliate_id
)
SELECT 
    d.date,
    d.affiliate_id,
    d.daily_trades,
    d.daily_volume,
    d.daily_symbols,
    d.daily_traders,
    w.weekly_trades,
    w.weekly_volume,
    w.weekly_symbols,
    w.weekly_traders,
    m.monthly_trades,
    m.monthly_volume,
    m.monthly_symbols,
    m.monthly_traders
FROM daily_metrics d
LEFT JOIN weekly_metrics w ON d.affiliate_id = w.affiliate_id 
    AND DATE_TRUNC('week', d.date) = w.week
LEFT JOIN monthly_metrics m ON d.affiliate_id = m.affiliate_id 
    AND DATE_TRUNC('month', d.date) = m.month
ORDER BY d.date DESC, d.affiliate_id;

-- Add index for faster queries
CREATE INDEX idx_gold_affiliate_performance_trend_date ON gold_affiliate_performance_trend(date);
CREATE INDEX idx_gold_affiliate_performance_trend_affiliate ON gold_affiliate_performance_trend(affiliate_id);

-- ========================
-- 6. ETL and Affiliate Performance Dashboard Materialized View
-- ========================
CREATE MATERIALIZED VIEW gold_etl_affiliate_dashboard AS
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
        COUNT(DISTINCT c.country) as countries_represented,
        COUNT(DISTINCT CASE WHEN c.register_time >= DATE_TRUNC('month', CURRENT_DATE) THEN c.client_id END) as new_signups_this_month,
        COUNT(DISTINCT CASE WHEN c.register_time >= CURRENT_DATE - INTERVAL '30 days' THEN c.client_id END) as new_signups_30d,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM silver_trades t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
        ) THEN c.client_id END) as active_customers_30d,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM silver_trades t 
            WHERE t.client_id = c.client_id 
            AND t.trade_time >= DATE_TRUNC('month', CURRENT_DATE)
        ) THEN c.client_id END) as active_customers_this_month,
        COALESCE(SUM(CASE WHEN t.trade_time >= DATE_TRUNC('month', CURRENT_DATE) THEN t.trade_volume ELSE 0 END), 0) as monthly_trading_volume,
        COALESCE(SUM(CASE WHEN t.trade_time >= CURRENT_DATE - INTERVAL '30 days' THEN t.trade_volume ELSE 0 END), 0) as trading_volume_30d,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= DATE_TRUNC('month', CURRENT_DATE) THEN t.symbol END), 0) as monthly_symbols_traded,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= CURRENT_DATE - INTERVAL '30 days' THEN t.symbol END), 0) as symbols_traded_30d,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= DATE_TRUNC('month', CURRENT_DATE) THEN t.trade_activity_id END), 0) as monthly_trades,
        COALESCE(COUNT(DISTINCT CASE WHEN t.trade_time >= CURRENT_DATE - INTERVAL '30 days' THEN t.trade_activity_id END), 0) as trades_30d
    FROM silver_customers c
    LEFT JOIN silver_trades t ON c.client_id = t.client_id
    GROUP BY c.affiliate_id
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
    -- Affiliate Performance
    p.affiliate_id,
    p.affiliate_name,
    p.total_customers,
    p.countries_represented,
    p.new_signups_this_month,
    p.new_signups_30d,
    p.active_customers_30d,
    p.active_customers_this_month,
    p.monthly_trading_volume,
    p.trading_volume_30d,
    p.monthly_symbols_traded,
    p.symbols_traded_30d,
    p.monthly_trades,
    p.trades_30d
FROM etl_status e
CROSS JOIN affiliate_performance p;

-- Add index for faster queries
CREATE INDEX idx_gold_etl_affiliate_dashboard_affiliate ON gold_etl_affiliate_dashboard(affiliate_id);
CREATE INDEX idx_gold_etl_affiliate_dashboard_data_source ON gold_etl_affiliate_dashboard(data_source);

-- ========================
-- Refresh Functions for Materialized Views
-- ========================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_affiliate_daily_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_affiliate_weekly_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_affiliate_monthly_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_affiliate_customer_funnel;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_affiliate_performance_trend;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_etl_affiliate_dashboard;
END;
$$ LANGUAGE plpgsql; 