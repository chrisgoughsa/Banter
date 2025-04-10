"""
Gold layer view definitions for analytics.
"""
import logging
from psycopg2.extensions import connection

from src.utils.db import execute_query, DatabaseError

logger = logging.getLogger(__name__)

class GoldViewManager:
    """Manager for gold layer analytical views."""
    
    def __init__(self, conn: connection):
        self.conn = conn

    def create_or_replace_view(self, view_name: str, query: str) -> None:
        """Create or replace a view."""
        try:
            # Drop view if exists
            execute_query(self.conn, f"DROP VIEW IF EXISTS {view_name} CASCADE")
            
            # Create new view
            execute_query(self.conn, f"CREATE VIEW {view_name} AS {query}")
            logger.info(f"Successfully created view {view_name}")
            
        except DatabaseError as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            raise

    def create_daily_metrics_view(self):
        """Create daily affiliate metrics view."""
        try:
            query = """
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
                ORDER BY date DESC, affiliate_id
            """
            self.create_or_replace_view('gold_affiliate_daily_metrics', query)
        except Exception as e:
            logger.error(f"Failed to create daily metrics view: {e}")
            raise

    def create_monthly_metrics_view(self):
        """Create monthly affiliate metrics view."""
        try:
            query = """
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
                        SUM(deposit_amount) as deposit_volume
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
                ORDER BY month DESC, affiliate_id
            """
            self.create_or_replace_view('gold_affiliate_monthly_metrics', query)
        except Exception as e:
            logger.error(f"Failed to create monthly metrics view: {e}")
            raise

    def create_affiliate_performance_view(self):
        """Create affiliate performance metrics view."""
        try:
            query = """
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
                        COUNT(DISTINCT t.id) as trades_30d,
                        COALESCE(SUM(t.trade_volume), 0) as trading_volume_30d,
                        CASE 
                            WHEN COUNT(DISTINCT t.id) > 0 
                            THEN COALESCE(SUM(t.trade_volume), 0) / COUNT(DISTINCT t.id)
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
                ORDER BY trading_volume_30d DESC
            """
            self.create_or_replace_view('gold_affiliate_performance', query)
        except Exception as e:
            logger.error(f"Failed to create affiliate performance view: {e}")
            raise

    def create_etl_dashboard_view(self):
        """Create ETL dashboard metrics view."""
        try:
            query = """
                WITH etl_metrics AS (
                    SELECT 
                        a.affiliate_id,
                        a.name as affiliate_name,
                        a.last_updated as affiliate_update_time,
                        COUNT(DISTINCT c.client_id) as total_records,
                        COUNT(DISTINCT c.client_id) as success_count,
                        0 as error_count,
                        0 as partial_count
                    FROM affiliateaccount a
                    LEFT JOIN clientaccount c ON a.affiliate_id = c.affiliate_id
                    GROUP BY a.affiliate_id, a.name, a.last_updated
                )
                SELECT 
                    affiliate_id,
                    affiliate_name,
                    affiliate_update_time,
                    total_records,
                    success_count,
                    error_count,
                    partial_count,
                    CASE 
                        WHEN total_records > 0 
                        THEN ROUND(100.0 * success_count / total_records, 2)
                        ELSE 0 
                    END as success_rate,
                    CURRENT_TIMESTAMP as last_updated,
                    'etl_dashboard' as view_type
                FROM etl_metrics
                ORDER BY affiliate_id
            """
            self.create_or_replace_view('gold_etl_affiliate_dashboard', query)
        except Exception as e:
            logger.error(f"Failed to create ETL dashboard view: {e}")
            raise

    def create_all_views(self):
        """Create all gold layer views."""
        try:
            logger.info("Creating gold layer views")
            
            # Create views in order of dependencies
            self.create_daily_metrics_view()
            self.create_monthly_metrics_view()
            self.create_affiliate_performance_view()
            self.create_etl_dashboard_view()
            
            logger.info("Successfully created all gold layer views")
            
        except Exception as e:
            logger.error(f"Failed to create gold layer views: {e}")
            raise 