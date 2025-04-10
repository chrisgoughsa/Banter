"""
Data transformers for the silver layer.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime

import pandas as pd
from psycopg2.extensions import connection

from src.utils.db import execute_query, execute_batch, DatabaseError
from src.utils.data_quality import validate_data_quality, log_data_quality_metrics
from src.config.settings import ETL_CONFIG

logger = logging.getLogger(__name__)

class SilverTransformer:
    """Base class for silver layer transformations."""
    
    def __init__(self, conn: connection):
        self.conn = conn
        self.conn.autocommit = False  # Ensure we're using transactions
        self.batch_size = ETL_CONFIG['batch_size']

    def create_table(self, query: str) -> None:
        """Create a table using the provided query."""
        try:
            execute_query(self.conn, query)
            self.conn.commit()  # Explicitly commit after table creation
            logger.info(f"Successfully created/updated table structure")
        except DatabaseError as e:
            self.conn.rollback()  # Rollback on error
            logger.error(f"Failed to create/update table: {e}")
            raise

    def transform_and_load(self, query: str, target_table: str) -> None:
        """Transform data using SQL and load into target table."""
        try:
            # Execute transformation query
            logger.info(f"Executing transformation query for {target_table}")
            logger.debug(f"Query: {query}")  # Log the actual query for debugging
            
            # First verify the source table exists
            source_table = query.split("FROM")[1].split()[0].strip()
            check_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{source_table}')"
            result = execute_query(self.conn, check_query)
            
            # Handle both dictionary and tuple results
            if isinstance(result[0], dict):
                table_exists = result[0].get('exists', False)
            else:
                table_exists = result[0][0] if result else False
            
            if not table_exists:
                raise DatabaseError(f"Source table {source_table} does not exist")
            
            # Execute the transformation
            execute_query(self.conn, query)
            self.conn.commit()  # Commit the transformation
            
            # Get count after transformation
            count_query = f"SELECT COUNT(*) FROM {target_table}"
            count_result = execute_query(self.conn, count_query)
            
            # Handle both dictionary and tuple results for count
            if isinstance(count_result[0], dict):
                count_after = count_result[0].get('count', 0)
            else:
                count_after = count_result[0][0] if count_result else 0
            
            logger.info(f"Added {count_after} records to {target_table}")
            
            # Get sample of transformed data for quality checks
            sample_query = f"""
                SELECT * FROM {target_table} 
                ORDER BY last_updated DESC 
                LIMIT 1000
            """
            result = execute_query(self.conn, sample_query)
            
            if not result:
                logger.warning(f"No data found in {target_table} after transformation")
                return

            # Convert to DataFrame for data quality checks
            df = pd.DataFrame(result)
            
            # Perform data quality checks
            quality_metrics = validate_data_quality(df, target_table)
            log_data_quality_metrics(self.conn, quality_metrics)
            
            if not quality_metrics['validation_passed']:
                logger.warning(f"Data quality issues found in {target_table}:")
                for issue in quality_metrics['issues']:
                    logger.warning(f"  - {issue}")

            logger.info(f"Successfully transformed data for {target_table}")
            
        except DatabaseError as e:
            self.conn.rollback()  # Rollback on error
            logger.error(f"Database error in transform_and_load for {target_table}: {str(e)}")
            raise
        except Exception as e:
            self.conn.rollback()  # Rollback on error
            logger.error(f"Unexpected error in transform_and_load for {target_table}: {str(e)}")
            logger.exception("Full traceback:")  # Add full traceback
            raise

    def drop_dependent_objects(self):
        """Drop all dependent objects before recreating tables."""
        try:
            # First drop all views
            views = [
                'gold_etl_affiliate_dashboard',
                'gold_affiliate_daily_metrics',
                'gold_affiliate_weekly_metrics',
                'gold_affiliate_monthly_metrics',
                'gold_affiliate_customer_funnel',
                'gold_affiliate_performance_trend'
            ]
            
            for view in views:
                try:
                    execute_query(self.conn, f"DROP VIEW IF EXISTS {view} CASCADE")
                    logger.info(f"Dropped view {view}")
                except DatabaseError as e:
                    logger.warning(f"Failed to drop view {view}: {e}")
            
            # Then drop tables in correct order (respecting foreign key constraints)
            tables = [
                'tradeactivities',  # Has FK to clientaccount
                'deposits',         # Has FK to clientaccount
                'clientaccount',    # Has FK to affiliateaccount
                'affiliateaccount'  # No dependencies
            ]
            
            for table in tables:
                try:
                    execute_query(self.conn, f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"Dropped table {table}")
                except DatabaseError as e:
                    logger.warning(f"Failed to drop table {table}: {e}")
            
            logger.info("Successfully dropped all dependent objects")
            
        except Exception as e:
            logger.error(f"Error in drop_dependent_objects: {e}")
            raise

class AffiliateTransformer(SilverTransformer):
    """Transform affiliate data to silver layer."""
    
    def create_affiliate_table(self):
        """Create the silver affiliate table."""
        query = """
            DROP TABLE IF EXISTS affiliateaccount CASCADE;
            CREATE TABLE affiliateaccount (
                affiliate_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255),
                join_date DATE,
                last_updated TIMESTAMP
            )
        """
        self.create_table(query)

    def transform_affiliates(self):
        """Transform affiliate data from bronze to silver."""
        try:
            query = """
                INSERT INTO affiliateaccount (
                    affiliate_id,
                    name,
                    email,
                    join_date,
                    last_updated
                )
                SELECT DISTINCT
                    affiliate_id,
                    'Affiliate ' || affiliate_id as name,
                    'affiliate' || affiliate_id || '@example.com' as email,
                    DATE(MIN(register_time)) as join_date,
                    CURRENT_TIMESTAMP as last_updated
                FROM bronze_customers
                WHERE affiliate_id IS NOT NULL
                GROUP BY affiliate_id
                ON CONFLICT (affiliate_id) DO UPDATE
                SET last_updated = EXCLUDED.last_updated
            """
            self.transform_and_load(query, 'affiliateaccount')
        except Exception as e:
            logger.error(f"Failed to transform affiliate data: {str(e)}")
            raise

class CustomerTransformer(SilverTransformer):
    """Transform customer data to silver layer."""
    
    def create_customer_table(self):
        """Create the silver customer table."""
        query = """
            DROP TABLE IF EXISTS clientaccount CASCADE;
            CREATE TABLE clientaccount (
                client_id VARCHAR(50) PRIMARY KEY,
                affiliate_id VARCHAR(50),
                register_time TIMESTAMP,
                last_updated TIMESTAMP,
                FOREIGN KEY (affiliate_id) REFERENCES affiliateaccount(affiliate_id)
            )
        """
        self.create_table(query)

    def transform_customers(self):
        """Transform customer data from bronze to silver."""
        try:
            query = """
                INSERT INTO clientaccount (
                    client_id,
                    affiliate_id,
                    register_time,
                    last_updated
                )
                SELECT DISTINCT ON (client_id)
                    client_id,
                    affiliate_id,
                    register_time,
                    CURRENT_TIMESTAMP as last_updated
                FROM bronze_customers
                WHERE client_id IS NOT NULL AND affiliate_id IS NOT NULL
                ORDER BY client_id, register_time DESC
                ON CONFLICT (client_id) DO UPDATE
                SET last_updated = EXCLUDED.last_updated
            """
            self.transform_and_load(query, 'clientaccount')
        except Exception as e:
            logger.error(f"Failed to transform customer data: {str(e)}")
            raise

class DepositTransformer(SilverTransformer):
    """Transform deposit data to silver layer."""
    
    def create_deposit_table(self):
        """Create the silver deposit table."""
        query = """
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
        """
        self.create_table(query)

    def transform_deposits(self):
        """Transform deposit data from bronze to silver."""
        try:
            query = """
                INSERT INTO deposits (
                    order_id,
                    client_id,
                    deposit_time,
                    deposit_coin,
                    deposit_amount,
                    last_updated
                )
                SELECT DISTINCT ON (order_id)
                    order_id,
                    client_id,
                    deposit_time,
                    deposit_coin,
                    deposit_amount,
                    CURRENT_TIMESTAMP as last_updated
                FROM bronze_deposits
                WHERE order_id IS NOT NULL AND client_id IS NOT NULL
                ORDER BY order_id, deposit_time DESC
                ON CONFLICT (order_id) DO UPDATE
                SET last_updated = EXCLUDED.last_updated
            """
            self.transform_and_load(query, 'deposits')
        except Exception as e:
            logger.error(f"Failed to transform deposit data: {str(e)}")
            raise

class TradeTransformer(SilverTransformer):
    """Transform trade data to silver layer."""
    
    def create_trade_table(self):
        """Create the silver trade table."""
        query = """
            DROP TABLE IF EXISTS tradeactivities CASCADE;
            CREATE TABLE tradeactivities (
                id SERIAL PRIMARY KEY,
                client_id VARCHAR(50),
                trade_time TIMESTAMP,
                trade_volume DECIMAL(18,8),
                last_updated TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clientaccount(client_id)
            )
        """
        self.create_table(query)

    def transform_trades(self):
        """Transform trade data from bronze to silver."""
        try:
            query = """
                INSERT INTO tradeactivities (
                    client_id,
                    trade_time,
                    trade_volume,
                    last_updated
                )
                SELECT
                    client_id,
                    trade_time,
                    trade_volume,
                    CURRENT_TIMESTAMP as last_updated
                FROM bronze_trades
                WHERE client_id IS NOT NULL
                ORDER BY trade_time DESC
            """
            self.transform_and_load(query, 'tradeactivities')
        except Exception as e:
            logger.error(f"Failed to transform trade data: {str(e)}")
            raise 