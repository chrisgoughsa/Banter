#!/usr/bin/env python3
"""
Main script for running the silver layer ETL process.
"""
import logging
import sys

from src.utils.db import get_db_connection, DatabaseError
from src.etl.silver.transformers import (
    AffiliateTransformer,
    CustomerTransformer,
    DepositTransformer,
    TradeTransformer
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def transform_silver_layer(conn) -> None:
    """Transform data from bronze to silver layer."""
    try:
        # Transform affiliate data first (required for foreign keys)
        logger.info("Transforming affiliate data")
        affiliate_transformer = AffiliateTransformer(conn)
        affiliate_transformer.create_affiliate_table()
        affiliate_transformer.transform_affiliates()

        # Transform customer data
        logger.info("Transforming customer data")
        customer_transformer = CustomerTransformer(conn)
        customer_transformer.create_customer_table()
        customer_transformer.transform_customers()

        # Transform deposit data
        logger.info("Transforming deposit data")
        deposit_transformer = DepositTransformer(conn)
        deposit_transformer.create_deposit_table()
        deposit_transformer.transform_deposits()

        # Transform trade data
        logger.info("Transforming trade data")
        trade_transformer = TradeTransformer(conn)
        trade_transformer.create_trade_table()
        trade_transformer.transform_trades()

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def main():
    """Main ETL process."""
    logger.info("Starting silver layer ETL process")
    
    conn = None
    try:
        # Get database connection
        logger.info("Attempting to connect to database...")
        conn = get_db_connection()
        logger.info("Successfully connected to database")
        
        conn.autocommit = False  # Ensure we're using transactions
        
        # Transform data to silver layer
        transform_silver_layer(conn)
        
        # Commit all changes
        conn.commit()
        logger.info("Successfully completed silver layer ETL process")
        
    except DatabaseError as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {str(e)}")
        logger.exception("Full traceback:")  # Add full traceback
        sys.exit(1)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error: {str(e)}")
        logger.exception("Full traceback:")  # Add full traceback
        sys.exit(1)
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")

if __name__ == "__main__":
    main() 