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

def create_gold_tables(conn):
    """Create gold layer tables focused on affiliate performance metrics"""
    with conn.cursor() as cur:
        # Create daily affiliate performance table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold_affiliate_daily_metrics (
                date DATE,
                affiliate_id VARCHAR(50),
                new_signups INTEGER,
                active_customers INTEGER,
                total_trading_volume DECIMAL(20,8),
                total_trades INTEGER,
                average_trade_size DECIMAL(20,8),
                PRIMARY KEY (date, affiliate_id)
            )
        """)
        
        # Create weekly affiliate performance table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold_affiliate_weekly_metrics (
                week_start DATE,
                affiliate_id VARCHAR(50),
                new_signups INTEGER,
                active_customers INTEGER,
                total_trading_volume DECIMAL(20,8),
                total_trades INTEGER,
                average_trade_size DECIMAL(20,8),
                PRIMARY KEY (week_start, affiliate_id)
            )
        """)
        
        # Create monthly affiliate performance table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold_affiliate_monthly_metrics (
                month_start DATE,
                affiliate_id VARCHAR(50),
                new_signups INTEGER,
                active_customers INTEGER,
                total_trading_volume DECIMAL(20,8),
                total_trades INTEGER,
                average_trade_size DECIMAL(20,8),
                PRIMARY KEY (month_start, affiliate_id)
            )
        """)
        
        # Create affiliate customer acquisition funnel view
        cur.execute("""
            CREATE OR REPLACE VIEW gold_affiliate_customer_funnel AS
            WITH customer_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(*) as total_customers,
                    COUNT(CASE WHEN registration_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as new_customers_30d,
                    COUNT(CASE WHEN last_activity_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as active_customers_30d,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_customers
                FROM silver_customers
                GROUP BY affiliate_id
            ),
            trading_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(*) as total_trades,
                    SUM(total_value) as total_volume,
                    AVG(total_value) as avg_trade_size
                FROM silver_trades
                WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY affiliate_id
            )
            SELECT 
                c.affiliate_id,
                c.total_customers,
                c.new_customers_30d,
                c.active_customers_30d,
                c.active_customers,
                COALESCE(t.total_trades, 0) as trades_30d,
                COALESCE(t.total_volume, 0) as volume_30d,
                COALESCE(t.avg_trade_size, 0) as avg_trade_size
            FROM customer_metrics c
            LEFT JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
        """)
        
        # Create affiliate performance trend view
        cur.execute("""
            CREATE OR REPLACE VIEW gold_affiliate_performance_trend AS
            WITH daily_metrics AS (
                SELECT 
                    DATE_TRUNC('day', trade_date) as date,
                    affiliate_id,
                    COUNT(*) as daily_trades,
                    SUM(total_value) as daily_volume
                FROM silver_trades
                WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY DATE_TRUNC('day', trade_date), affiliate_id
            ),
            weekly_metrics AS (
                SELECT 
                    DATE_TRUNC('week', trade_date) as week,
                    affiliate_id,
                    COUNT(*) as weekly_trades,
                    SUM(total_value) as weekly_volume
                FROM silver_trades
                WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY DATE_TRUNC('week', trade_date), affiliate_id
            ),
            monthly_metrics AS (
                SELECT 
                    DATE_TRUNC('month', trade_date) as month,
                    affiliate_id,
                    COUNT(*) as monthly_trades,
                    SUM(total_value) as monthly_volume
                FROM silver_trades
                WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY DATE_TRUNC('month', trade_date), affiliate_id
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

def calculate_daily_metrics(conn, date: datetime):
    """Calculate and store daily metrics for all affiliates"""
    with conn.cursor() as cur:
        # Get new signups and active customers
        cur.execute("""
            INSERT INTO gold_affiliate_daily_metrics (
                date, affiliate_id, new_signups, active_customers,
                total_trading_volume, total_trades, average_trade_size
            )
            WITH customer_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(CASE WHEN registration_date = %s THEN 1 END) as new_signups,
                    COUNT(CASE WHEN last_activity_date = %s THEN 1 END) as active_customers
                FROM silver_customers
                GROUP BY affiliate_id
            ),
            trading_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(*) as total_trades,
                    SUM(total_value) as total_volume,
                    AVG(total_value) as avg_trade_size
                FROM silver_trades
                WHERE trade_date = %s
                GROUP BY affiliate_id
            )
            SELECT 
                %s as date,
                COALESCE(c.affiliate_id, t.affiliate_id) as affiliate_id,
                COALESCE(c.new_signups, 0) as new_signups,
                COALESCE(c.active_customers, 0) as active_customers,
                COALESCE(t.total_volume, 0) as total_volume,
                COALESCE(t.total_trades, 0) as total_trades,
                COALESCE(t.avg_trade_size, 0) as avg_trade_size
            FROM customer_metrics c
            FULL OUTER JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
            ON CONFLICT (date, affiliate_id) DO UPDATE
            SET new_signups = EXCLUDED.new_signups,
                active_customers = EXCLUDED.active_customers,
                total_trading_volume = EXCLUDED.total_trading_volume,
                total_trades = EXCLUDED.total_trades,
                average_trade_size = EXCLUDED.average_trade_size
        """, (date, date, date, date))
        
    conn.commit()

def calculate_weekly_metrics(conn, week_start: datetime):
    """Calculate and store weekly metrics for all affiliates"""
    with conn.cursor() as cur:
        week_end = week_start + timedelta(days=6)
        
        cur.execute("""
            INSERT INTO gold_affiliate_weekly_metrics (
                week_start, affiliate_id, new_signups, active_customers,
                total_trading_volume, total_trades, average_trade_size
            )
            WITH customer_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(CASE WHEN registration_date BETWEEN %s AND %s THEN 1 END) as new_signups,
                    COUNT(CASE WHEN last_activity_date BETWEEN %s AND %s THEN 1 END) as active_customers
                FROM silver_customers
                GROUP BY affiliate_id
            ),
            trading_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(*) as total_trades,
                    SUM(total_value) as total_volume,
                    AVG(total_value) as avg_trade_size
                FROM silver_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY affiliate_id
            )
            SELECT 
                %s as week_start,
                COALESCE(c.affiliate_id, t.affiliate_id) as affiliate_id,
                COALESCE(c.new_signups, 0) as new_signups,
                COALESCE(c.active_customers, 0) as active_customers,
                COALESCE(t.total_volume, 0) as total_volume,
                COALESCE(t.total_trades, 0) as total_trades,
                COALESCE(t.avg_trade_size, 0) as avg_trade_size
            FROM customer_metrics c
            FULL OUTER JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
            ON CONFLICT (week_start, affiliate_id) DO UPDATE
            SET new_signups = EXCLUDED.new_signups,
                active_customers = EXCLUDED.active_customers,
                total_trading_volume = EXCLUDED.total_trading_volume,
                total_trades = EXCLUDED.total_trades,
                average_trade_size = EXCLUDED.average_trade_size
        """, (week_start, week_end, week_start, week_end, week_start, week_end, week_start))
        
    conn.commit()

def calculate_monthly_metrics(conn, month_start: datetime):
    """Calculate and store monthly metrics for all affiliates"""
    with conn.cursor() as cur:
        # Calculate last day of month
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        month_end = next_month - timedelta(days=1)
        
        cur.execute("""
            INSERT INTO gold_affiliate_monthly_metrics (
                month_start, affiliate_id, new_signups, active_customers,
                total_trading_volume, total_trades, average_trade_size
            )
            WITH customer_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(CASE WHEN registration_date BETWEEN %s AND %s THEN 1 END) as new_signups,
                    COUNT(CASE WHEN last_activity_date BETWEEN %s AND %s THEN 1 END) as active_customers
                FROM silver_customers
                GROUP BY affiliate_id
            ),
            trading_metrics AS (
                SELECT 
                    affiliate_id,
                    COUNT(*) as total_trades,
                    SUM(total_value) as total_volume,
                    AVG(total_value) as avg_trade_size
                FROM silver_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY affiliate_id
            )
            SELECT 
                %s as month_start,
                COALESCE(c.affiliate_id, t.affiliate_id) as affiliate_id,
                COALESCE(c.new_signups, 0) as new_signups,
                COALESCE(c.active_customers, 0) as active_customers,
                COALESCE(t.total_volume, 0) as total_volume,
                COALESCE(t.total_trades, 0) as total_trades,
                COALESCE(t.avg_trade_size, 0) as avg_trade_size
            FROM customer_metrics c
            FULL OUTER JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
            ON CONFLICT (month_start, affiliate_id) DO UPDATE
            SET new_signups = EXCLUDED.new_signups,
                active_customers = EXCLUDED.active_customers,
                total_trading_volume = EXCLUDED.total_trading_volume,
                total_trades = EXCLUDED.total_trades,
                average_trade_size = EXCLUDED.average_trade_size
        """, (month_start, month_end, month_start, month_end, month_start, month_end, month_start))
        
    conn.commit()

def transform_silver_to_gold(conn):
    """Transform silver data to gold layer"""
    # Create gold tables and views
    create_gold_tables(conn)
    
    # Calculate metrics for the last 90 days
    today = datetime.now().date()
    for i in range(90):
        date = today - timedelta(days=i)
        calculate_daily_metrics(conn, date)
        
        # Calculate weekly metrics on Sundays
        if date.weekday() == 6:  # Sunday
            week_start = date - timedelta(days=6)
            calculate_weekly_metrics(conn, week_start)
            
        # Calculate monthly metrics on the first day of each month
        if date.day == 1:
            calculate_monthly_metrics(conn, date)

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Transform silver data to gold
        transform_silver_to_gold(conn)
        
        print("Successfully transformed silver data to gold layer")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 