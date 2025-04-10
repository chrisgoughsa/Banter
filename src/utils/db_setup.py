"""
Database and table creation scripts.
"""
import logging
from typing import List, Dict, Any
from psycopg2.extensions import connection
from src.utils.db import execute_query, DatabaseError

logger = logging.getLogger(__name__)

def create_database(conn: connection) -> None:
    """Create the database if it doesn't exist."""
    try:
        # Create database if it doesn't exist
        execute_query(conn, """
            DO $$ 
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'crypto_data_platform') THEN
                    CREATE DATABASE crypto_data_platform;
                END IF;
            END $$;
        """)
        logger.info("Database created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create database: {e}")
        raise

def create_affiliate_table(conn: connection) -> None:
    """Create the silver affiliate table."""
    try:
        execute_query(conn, """
            DROP TABLE IF EXISTS affiliateaccount CASCADE;
            CREATE TABLE affiliateaccount (
                affiliate_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255),
                join_date DATE,
                last_updated TIMESTAMP
            )
        """)
        logger.info("Affiliate table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create affiliate table: {e}")
        raise

def create_customer_table(conn: connection) -> None:
    """Create the silver customer table."""
    try:
        execute_query(conn, """
            DROP TABLE IF EXISTS clientaccount CASCADE;
            CREATE TABLE clientaccount (
                client_id VARCHAR(50) PRIMARY KEY,
                affiliate_id VARCHAR(50),
                register_time TIMESTAMP,
                last_updated TIMESTAMP,
                FOREIGN KEY (affiliate_id) REFERENCES affiliateaccount(affiliate_id)
            )
        """)
        logger.info("Customer table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create customer table: {e}")
        raise

def create_deposit_table(conn: connection) -> None:
    """Create the silver deposit table."""
    try:
        execute_query(conn, """
            DROP TABLE IF EXISTS deposits CASCADE;
            CREATE TABLE deposits (
                order_id VARCHAR(50) PRIMARY KEY,
                client_id VARCHAR(50),
                deposit_time TIMESTAMP,
                deposit_coin VARCHAR(10),
                deposit_amount DECIMAL(18,8),
                last_updated TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clientaccount(client_id)
            )
        """)
        logger.info("Deposit table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create deposit table: {e}")
        raise

def create_trade_table(conn: connection) -> None:
    """Create the silver trade table."""
    try:
        execute_query(conn, """
            DROP TABLE IF EXISTS tradeactivities CASCADE;
            CREATE TABLE tradeactivities (
                client_id VARCHAR(50),
                trade_time TIMESTAMP,
                trade_volume DECIMAL(18,8),
                last_updated TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clientaccount(client_id),
                PRIMARY KEY (client_id, trade_time)
            )
        """)
        logger.info("Trade table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create trade table: {e}")
        raise

def create_silver_tables(conn: connection) -> None:
    """Create all silver layer tables in the correct order."""
    create_affiliate_table(conn)
    create_customer_table(conn)
    create_deposit_table(conn)
    create_trade_table(conn)

def create_customers_table(conn: connection) -> None:
    """Create the customers table if it doesn't exist."""
    try:
        execute_query(conn, """
            CREATE TABLE IF NOT EXISTS bronze_customers (
                affiliate_id VARCHAR(255) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                register_time TIMESTAMP NOT NULL,
                source_file VARCHAR(255),
                load_time TIMESTAMP,
                load_status VARCHAR(20),
                PRIMARY KEY (affiliate_id, client_id)
            )
        """)
        logger.info("Customers table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create customers table: {e}")
        raise

def create_deposits_table(conn: connection) -> None:
    """Create the deposits table if it doesn't exist."""
    try:
        execute_query(conn, """
            CREATE TABLE IF NOT EXISTS bronze_deposits (
                order_id VARCHAR(255) PRIMARY KEY,
                affiliate_id VARCHAR(255) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                deposit_time TIMESTAMP NOT NULL,
                deposit_coin VARCHAR(50) NOT NULL,
                deposit_amount DECIMAL(20,8) NOT NULL,
                source_file VARCHAR(255),
                load_time TIMESTAMP,
                load_status VARCHAR(20)
            )
        """)
        logger.info("Deposits table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create deposits table: {e}")
        raise

def create_trades_table(conn: connection) -> None:
    """Create the trades table if it doesn't exist."""
    try:
        execute_query(conn, """
            CREATE TABLE IF NOT EXISTS bronze_trades (
                id SERIAL PRIMARY KEY,
                affiliate_id VARCHAR(255) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                trade_volume DECIMAL(20,8) NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                source_file VARCHAR(255),
                load_time TIMESTAMP,
                load_status VARCHAR(20)
            )
        """)
        logger.info("Trades table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create trades table: {e}")
        raise

def create_assets_table(conn: connection) -> None:
    """Create the assets table if it doesn't exist."""
    try:
        execute_query(conn, """
            CREATE TABLE IF NOT EXISTS bronze_assets (
                id SERIAL PRIMARY KEY,
                affiliate_id VARCHAR(255) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                balance DECIMAL(20,8) NOT NULL,
                update_time TIMESTAMP NOT NULL,
                remark VARCHAR(255),
                source_file VARCHAR(255),
                load_time TIMESTAMP,
                load_status VARCHAR(20)
            )
        """)
        logger.info("Assets table created or already exists")
    except DatabaseError as e:
        logger.error(f"Failed to create assets table: {e}")
        raise

def create_daily_metrics_view(conn: connection) -> None:
    """Create daily affiliate metrics view."""
    try:
        execute_query(conn, """
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
            ORDER BY date DESC, affiliate_id
        """)
        logger.info("Daily metrics view created or updated")
    except DatabaseError as e:
        logger.error(f"Failed to create daily metrics view: {e}")
        raise

def create_monthly_metrics_view(conn: connection) -> None:
    """Create monthly affiliate metrics view."""
    try:
        execute_query(conn, """
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
        """)
        logger.info("Monthly metrics view created or updated")
    except DatabaseError as e:
        logger.error(f"Failed to create monthly metrics view: {e}")
        raise

def create_affiliate_performance_view(conn: connection) -> None:
    """Create affiliate performance metrics view."""
    try:
        execute_query(conn, """
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
                        COUNT(t.client_id) as trades_30d,
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
                ORDER BY trading_volume_30d DESC
        """)
        logger.info("Affiliate performance view created or updated")
    except DatabaseError as e:
        logger.error(f"Failed to create affiliate performance view: {e}")
        raise

def create_etl_dashboard_view(conn: connection) -> None:
    """Create ETL dashboard metrics view."""
    try:
        execute_query(conn, """
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
            ORDER BY affiliate_id
        """)
        logger.info("ETL dashboard view created or updated")
    except DatabaseError as e:
        logger.error(f"Failed to create ETL dashboard view: {e}")
        raise

def create_gold_views(conn: connection) -> None:
    """Create all gold layer views."""
    try:
        logger.info("Creating gold layer views")
        
        # Create views in order of dependencies
        create_daily_metrics_view(conn)
        create_monthly_metrics_view(conn)
        create_affiliate_performance_view(conn)
        create_etl_dashboard_view(conn)
        
        logger.info("Successfully created all gold layer views")
        
    except Exception as e:
        logger.error(f"Failed to create gold layer views: {e}")
        raise

def create_all_tables(conn: connection) -> None:
    """Create all tables in the correct order."""
    # Create bronze tables
    create_customers_table(conn)
    create_deposits_table(conn)
    create_trades_table(conn)
    create_assets_table(conn)
    
    # Create silver tables
    create_silver_tables(conn)
    
    # Create gold views
    create_gold_views(conn) 