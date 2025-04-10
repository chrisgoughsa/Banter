#!/usr/bin/env python3
"""
Script to clear all tables and views from the database.
"""
import logging
import sys

from src.utils.db import get_db_connection, execute_query, DatabaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clear_database(conn) -> None:
    """Clear all tables and views from the database."""
    try:
        # Drop views first
        views = [
            'gold_affiliate_daily_metrics',
            'gold_affiliate_monthly_metrics',
            'gold_affiliate_performance',
            'gold_etl_affiliate_dashboard'
        ]
        
        for view in views:
            try:
                execute_query(conn, f"DROP VIEW IF EXISTS {view} CASCADE")
                logger.info(f"Dropped view {view}")
            except DatabaseError as e:
                logger.error(f"Error dropping view {view}: {e}")
                continue

        # Drop tables in the correct order (respecting foreign key constraints)
        tables = [
            'etl_data_quality_log',  # New table for data quality metrics
            'TradeActivities',
            'Deposits',
            'ClientAccount',
            'AffiliateAccount',
            'bronze_assets',
            'bronze_deposits',
            'bronze_trades',
            'bronze_customers'
        ]
        
        for table in tables:
            try:
                execute_query(conn, f"DROP TABLE IF EXISTS {table} CASCADE")
                logger.info(f"Dropped table {table}")
            except DatabaseError as e:
                logger.error(f"Error dropping table {table}: {e}")
                continue
            
        logger.info("Successfully cleared all tables and views from the database")
            
    except Exception as e:
        logger.error(f"Error in clear_database: {e}")
        raise

def main():
    """Main cleanup process."""
    logger.info("Starting database cleanup")
    
    try:
        # Get database connection
        with get_db_connection() as conn:
            # Clear database
            clear_database(conn)
            
        logger.info("Successfully completed database cleanup")
        
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 