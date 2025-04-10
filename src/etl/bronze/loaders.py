"""
Data loaders for the bronze layer.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime

import pandas as pd
from psycopg2.extensions import connection

from src.config.settings import ETL_CONFIG
from src.utils.db import execute_batch, execute_query, DatabaseError
from src.utils.data_quality import validate_data_quality, log_data_quality_metrics, create_data_quality_table
from src.utils.db_setup import create_all_tables

logger = logging.getLogger(__name__)

class BronzeLoader:
    """Base class for bronze layer data loading."""
    
    def __init__(self, conn: connection):
        self.conn = conn
        create_data_quality_table(conn)
        create_all_tables(conn)
        
    def load_data(self, data: List[Dict[str, Any]], table_name: str, columns: List[str]) -> None:
        """Load data into a table."""
        if not data:
            logger.warning(f"No data to load into {table_name}")
            return

        # Convert to DataFrame for data quality checks
        df = pd.DataFrame(data)
        
        # Perform data quality checks
        quality_metrics = validate_data_quality(df, table_name)
        log_data_quality_metrics(self.conn, quality_metrics)
        
        if not quality_metrics['validation_passed']:
            logger.warning(f"Data quality issues found in {table_name}:")
            for issue in quality_metrics['issues']:
                logger.warning(f"  - {issue}")

        # Prepare SQL with NULL for non-raw fields
        raw_columns = ['affiliate_id', 'client_id', 'register_time', 'deposit_time', 
                      'trade_time', 'update_time', 'order_id', 'deposit_coin', 
                      'deposit_amount', 'trade_volume', 'balance', 'remark']
        
        # Create a list of values for each record, using NULL for non-raw fields
        current_time = datetime.now()
        rows = []
        for record in data:
            row = []
            for col in columns:
                if col == 'load_time':
                    row.append(current_time)
                elif col in raw_columns and col in record:
                    row.append(record[col])
                else:
                    row.append(None)
            rows.append(row)
        
        # Prepare SQL
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """
        
        try:
            # Load data in batches
            batch_size = ETL_CONFIG['batch_size']
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                execute_batch(self.conn, insert_sql, batch)
                logger.info(f"Loaded batch of {len(batch)} records into {table_name}")
                
        except DatabaseError as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise

class CustomerLoader(BronzeLoader):
    """Load customer data into bronze layer."""
    
    def load_customers(self, customers: List[Dict[str, Any]]):
        """Load customer data into bronze_customers table."""
        columns = ['affiliate_id', 'client_id', 'register_time', 'source_file', 'load_time', 'load_status']
        self.load_data(customers, 'bronze_customers', columns)

class DepositLoader(BronzeLoader):
    """Load deposit data into bronze layer."""
    
    def load_deposits(self, deposits: List[Dict[str, Any]]):
        """Load deposit data into bronze_deposits table."""
        columns = ['order_id', 'affiliate_id', 'client_id', 'deposit_time', 
                  'deposit_coin', 'deposit_amount', 'source_file', 'load_time', 'load_status']
        self.load_data(deposits, 'bronze_deposits', columns)

class TradeLoader(BronzeLoader):
    """Load trade data into bronze layer."""
    
    def load_trades(self, trades: List[Dict[str, Any]]):
        """Load trade data into bronze_trades table."""
        columns = ['affiliate_id', 'client_id', 'trade_volume', 'trade_time', 
                  'source_file', 'load_time', 'load_status']
        self.load_data(trades, 'bronze_trades', columns)

class AssetLoader(BronzeLoader):
    """Load asset data into bronze layer."""
    
    def load_assets(self, assets: List[Dict[str, Any]]):
        """Load asset data into bronze_assets table."""
        columns = ['affiliate_id', 'client_id', 'balance', 'update_time', 
                  'remark', 'source_file', 'load_time', 'load_status']
        self.load_data(assets, 'bronze_assets', columns) 