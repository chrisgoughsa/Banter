import psycopg2
from datetime import datetime, timedelta
from typing import Dict, Any

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crypto_data_platform',
    'user': 'postgres',
    'password': 'postgres'
}

def drop_existing_objects(conn):
    """Drop any existing tables or views with gold_ prefix"""
    with conn.cursor() as cur:
        # Get list of existing tables and views
        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'gold_%'
        """)
        
        # Drop each object
        for obj in cur.fetchall():
            name, obj_type = obj
            if obj_type == 'BASE TABLE':
                cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
            elif obj_type == 'VIEW':
                cur.execute(f"DROP VIEW IF EXISTS {name} CASCADE")
        
    conn.commit()

def create_gold_views(conn):
    """Create gold layer views focused on affiliate performance metrics"""
    # First drop any existing objects
    drop_existing_objects(conn)
    
    with conn.cursor() as cur:
        # Create daily affiliate performance view
        cur.execute("""
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
            FULL OUTER JOIN daily_trades t ON s.date = t.date AND s.affiliate_id = t.affiliate_id
        """)
        
        # Create weekly affiliate performance view
        cur.execute("""
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
            FULL OUTER JOIN weekly_trades t ON s.week_start = t.week_start AND s.affiliate_id = t.affiliate_id
        """)
        
        # Create monthly affiliate performance view
        cur.execute("""
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
            FULL OUTER JOIN monthly_trades t ON s.month_start = t.month_start AND s.affiliate_id = t.affiliate_id
        """)
        
        # Create affiliate customer acquisition funnel view
        cur.execute("""
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
            LEFT JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
        """)
        
        # Create affiliate performance trend view
        cur.execute("""
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
            ORDER BY d.date DESC, d.affiliate_id
        """)
        
    conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Create gold views
        create_gold_views(conn)
        
        print("Successfully created gold layer views")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 