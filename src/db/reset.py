"""
Database reset functionality.
"""
import logging
from psycopg2.extensions import connection
from .connection import get_db_connection, DatabaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def reset_database() -> None:
    """
    Reset the database by dropping all tables and recreating them.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Drop all tables and views
            cur.execute("""
                DROP VIEW IF EXISTS gold_etl_affiliate_dashboard CASCADE;
                DROP VIEW IF EXISTS gold_affiliate_performance CASCADE;
                DROP VIEW IF EXISTS gold_affiliate_monthly_metrics CASCADE;
                DROP VIEW IF EXISTS gold_affiliate_daily_metrics CASCADE;
                
                DROP TABLE IF EXISTS tradeactivities CASCADE;
                DROP TABLE IF EXISTS deposits CASCADE;
                DROP TABLE IF EXISTS clientaccount CASCADE;
                DROP TABLE IF EXISTS affiliateaccount CASCADE;
                
                DROP TABLE IF EXISTS bronze_assets CASCADE;
                DROP TABLE IF EXISTS bronze_trades CASCADE;
                DROP TABLE IF EXISTS bronze_deposits CASCADE;
                DROP TABLE IF EXISTS bronze_customers CASCADE;
            """)
            
            conn.commit()
            logger.info("All tables and views dropped successfully")
            
            # Recreate the schema
            from .setup import setup_database
            setup_database(conn)
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database reset failed: {e}")
        raise DatabaseError(f"Failed to reset database: {e}")
    finally:
        if conn:
            conn.close() 