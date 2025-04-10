import psycopg2
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'dbname': 'crypto_data_platform',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

def get_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def create_gold_views(conn):
    """Create gold layer views with metadata"""
    cur = conn.cursor()
    try:
        # Drop existing views
        views_to_drop = [
            'gold_affiliate_daily_metrics',
            'gold_affiliate_weekly_metrics',
            'gold_affiliate_monthly_metrics',
            'gold_affiliate_customer_funnel',
            'gold_affiliate_performance_trend',
            'gold_etl_affiliate_dashboard'
        ]
        
        for view in views_to_drop:
            cur.execute(f"DROP VIEW IF EXISTS {view}")
            logger.info(f"Dropped view {view}")

        # Create daily metrics view with metadata
        cur.execute("""
            CREATE VIEW gold_affiliate_daily_metrics AS
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
                COALESCE(t.total_volume, 0) as total_volume,
                CURRENT_TIMESTAMP as last_updated,
                'daily_metrics' as view_type
            FROM daily_signups s
            FULL OUTER JOIN daily_trades t 
                ON s.date = t.date AND s.affiliate_id = t.affiliate_id
        """)
        logger.info("Created gold_affiliate_daily_metrics view")

        # Create weekly metrics view with metadata
        cur.execute("""
            CREATE VIEW gold_affiliate_weekly_metrics AS
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
                COALESCE(t.total_volume, 0) as total_volume,
                CURRENT_TIMESTAMP as last_updated,
                'weekly_metrics' as view_type
            FROM weekly_signups s
            FULL OUTER JOIN weekly_trades t 
                ON s.week_start = t.week_start AND s.affiliate_id = t.affiliate_id
        """)
        logger.info("Created gold_affiliate_weekly_metrics view")

        # Create monthly metrics view with metadata
        cur.execute("""
            CREATE VIEW gold_affiliate_monthly_metrics AS
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
                COALESCE(t.total_volume, 0) as total_volume,
                CURRENT_TIMESTAMP as last_updated,
                'monthly_metrics' as view_type
            FROM monthly_signups s
            FULL OUTER JOIN monthly_trades t 
                ON s.month_start = t.month_start AND s.affiliate_id = t.affiliate_id
        """)
        logger.info("Created gold_affiliate_monthly_metrics view")

        # Create customer funnel view with metadata
        cur.execute("""
            CREATE VIEW gold_affiliate_customer_funnel AS
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
                COALESCE(t.total_volume, 0) as volume_30d,
                CURRENT_TIMESTAMP as last_updated,
                'customer_funnel' as view_type
            FROM customer_metrics c
            LEFT JOIN trading_metrics t ON c.affiliate_id = t.affiliate_id
        """)
        logger.info("Created gold_affiliate_customer_funnel view")

        # Create performance trend view with metadata
        cur.execute("""
            CREATE VIEW gold_affiliate_performance_trend AS
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
                m.monthly_volume,
                CURRENT_TIMESTAMP as last_updated,
                'performance_trend' as view_type
            FROM daily_metrics d
            LEFT JOIN weekly_metrics w ON d.affiliate_id = w.affiliate_id 
                AND DATE_TRUNC('week', d.date) = w.week
            LEFT JOIN monthly_metrics m ON d.affiliate_id = m.affiliate_id 
                AND DATE_TRUNC('month', d.date) = m.month
            ORDER BY d.date DESC, d.affiliate_id
        """)
        logger.info("Created gold_affiliate_performance_trend view")

        # Create ETL and affiliate performance dashboard view with metadata
        cur.execute("""
            CREATE VIEW gold_etl_affiliate_dashboard AS
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
                            FROM TradeActivities t 
                            WHERE t.client_id = c.client_id 
                            AND t.trade_time >= CURRENT_DATE - INTERVAL '30 days'
                        ) 
                        THEN c.client_id 
                    END) as active_customers_30d
                FROM AffiliateAccount a
                LEFT JOIN ClientAccount c ON a.affiliate_id = c.affiliate_id
                GROUP BY a.affiliate_id, a.name
            ),
            trading_metrics AS (
                SELECT 
                    c.affiliate_id,
                    COUNT(DISTINCT t.trade_activity_id) as trades_30d,
                    COALESCE(SUM(t.trade_volume), 0) as trading_volume_30d,
                    CASE 
                        WHEN COUNT(DISTINCT t.trade_activity_id) > 0 
                        THEN COALESCE(SUM(t.trade_volume), 0) / COUNT(DISTINCT t.trade_activity_id)
                        ELSE 0 
                    END as avg_trade_size
                FROM ClientAccount c
                LEFT JOIN TradeActivities t ON c.client_id = t.client_id
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
                'etl_dashboard' as view_type
            FROM affiliate_metrics m
            LEFT JOIN trading_metrics t ON m.affiliate_id = t.affiliate_id
            ORDER BY trading_volume_30d DESC
        """)
        logger.info("Created gold_etl_affiliate_dashboard view")

        conn.commit()
        logger.info("Successfully created all gold views with metadata")
        
    except Exception as e:
        logger.error(f"Error creating gold views: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def main():
    """Main function to create gold layer views"""
    logger.info("Starting silver to gold transformation")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Create gold views with metadata
        create_gold_views(conn)
        
        logger.info("Completed silver to gold transformation")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 