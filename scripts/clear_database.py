import psycopg2
import logging

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

def clear_database():
    """Clear all tables from the database"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Drop tables in the correct order (respecting foreign key constraints)
            tables = [
                'Assets',
                'Deposits',
                'TradeActivities',
                'ClientAccount',
                'AffiliateAccount',
                'bronze_assets',
                'bronze_deposits',
                'bronze_trades',
                'bronze_customers',
                'bronze_affiliates'
            ]
            
            for table in tables:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"Dropped table {table}")
                except Exception as e:
                    logger.error(f"Error dropping table {table}: {e}")
                    continue
            
            conn.commit()
            logger.info("Successfully cleared all tables from the database")
            
    except Exception as e:
        logger.error(f"Error in clear_database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    clear_database() 