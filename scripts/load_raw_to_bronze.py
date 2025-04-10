import os
import json
import glob
import psycopg2
from datetime import datetime
from typing import Dict, List, Optional
import logging
from pathlib import Path

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

def process_json_file(file_path: str) -> Dict:
    """Process a JSON file and return its contents"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return None

def extract_affiliate_id(file_path: str) -> str:
    """Extract affiliate ID from file path"""
    parts = Path(file_path).parts
    for part in parts:
        if part.startswith('affiliate'):
            return part
    return None

def create_bronze_tables(conn):
    """Create bronze layer tables"""
    cur = conn.cursor()
    
    # Create bronze_affiliates table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_affiliates (
            affiliate_id VARCHAR(50) PRIMARY KEY,
            data JSONB,
            load_timestamp TIMESTAMP,
            api_run_timestamp TIMESTAMP,
            source_file VARCHAR(255),
            load_status VARCHAR(20),
            error_message TEXT
        )
    """)
    
    # Create bronze_assets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_assets (
            affiliate_id VARCHAR(50),
            asset_id VARCHAR(50),
            symbol VARCHAR(50),
            name VARCHAR(100),
            data JSONB,
            load_timestamp TIMESTAMP,
            api_run_timestamp TIMESTAMP,
            source_file VARCHAR(255),
            load_status VARCHAR(20),
            error_message TEXT,
            PRIMARY KEY (affiliate_id, asset_id)
        )
    """)
    
    # Create bronze_customers table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_customers (
            affiliate_id VARCHAR(50),
            customer_id VARCHAR(50),
            data JSONB,
            load_timestamp TIMESTAMP,
            api_run_timestamp TIMESTAMP,
            source_file VARCHAR(255),
            load_status VARCHAR(20),
            error_message TEXT,
            PRIMARY KEY (affiliate_id, customer_id)
        )
    """)
    
    # Create bronze_deposits table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_deposits (
            affiliate_id VARCHAR(50),
            deposit_id VARCHAR(50),
            data JSONB,
            load_timestamp TIMESTAMP,
            api_run_timestamp TIMESTAMP,
            source_file VARCHAR(255),
            load_status VARCHAR(20),
            error_message TEXT,
            PRIMARY KEY (affiliate_id, deposit_id)
        )
    """)
    
    # Create bronze_trades table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_trades (
            affiliate_id VARCHAR(50),
            trade_id VARCHAR(50),
            data JSONB,
            load_timestamp TIMESTAMP,
            api_run_timestamp TIMESTAMP,
            source_file VARCHAR(255),
            load_status VARCHAR(20),
            error_message TEXT,
            PRIMARY KEY (affiliate_id, trade_id)
        )
    """)
    
    conn.commit()
    logger.info("Created bronze layer tables")

def process_directory(cur, base_dir: str, table_name: str, affiliate_id: str):
    """Process all JSON files in a directory recursively"""
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                try:
                    process_json_file_to_db(cur, table_name, file_path, affiliate_id)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")

def process_json_file_to_db(cur, table_name: str, file_path: str, affiliate_id: str):
    """Process a single JSON file and insert into the appropriate table"""
    try:
        data = process_json_file(file_path)
        if not data:
            return

        # Get file stats for metadata
        file_stats = os.stat(file_path)
        current_time = datetime.now()
        
        # Process the data array from the response
        if 'data' not in data or not isinstance(data['data'], list):
            logger.warning(f"No data array found in {file_path}")
            return
            
        for record in data['data']:
            # Extract record ID based on table type and ensure it's a string
            record_id = None
            if table_name == 'bronze_assets':
                record_id = str(record.get('uid'))
            elif table_name == 'bronze_customers':
                record_id = str(record.get('uid'))
            elif table_name == 'bronze_deposits':
                record_id = str(record.get('orderId'))
            elif table_name == 'bronze_trades':
                record_id = str(record.get('uid'))

            if not record_id:
                logger.warning(f"No record ID found in record from {file_path}")
                continue

            # Insert into appropriate table
            if table_name == 'bronze_assets':
                cur.execute("""
                    INSERT INTO bronze_assets 
                    (affiliate_id, asset_id, data, load_timestamp, api_run_timestamp, source_file, load_status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS')
                    ON CONFLICT (affiliate_id, asset_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        load_timestamp = EXCLUDED.load_timestamp,
                        api_run_timestamp = EXCLUDED.api_run_timestamp,
                        source_file = EXCLUDED.source_file,
                        load_status = EXCLUDED.load_status
                """, (affiliate_id, record_id, json.dumps(record), current_time, 
                      datetime.fromtimestamp(file_stats.st_mtime), str(file_path)))
                      
            elif table_name == 'bronze_customers':
                cur.execute("""
                    INSERT INTO bronze_customers 
                    (affiliate_id, customer_id, data, load_timestamp, api_run_timestamp, source_file, load_status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS')
                    ON CONFLICT (affiliate_id, customer_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        load_timestamp = EXCLUDED.load_timestamp,
                        api_run_timestamp = EXCLUDED.api_run_timestamp,
                        source_file = EXCLUDED.source_file,
                        load_status = EXCLUDED.load_status
                """, (affiliate_id, record_id, json.dumps(record), current_time, 
                      datetime.fromtimestamp(file_stats.st_mtime), str(file_path)))
                
            elif table_name == 'bronze_deposits':
                cur.execute("""
                    INSERT INTO bronze_deposits 
                    (affiliate_id, deposit_id, data, load_timestamp, api_run_timestamp, source_file, load_status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS')
                    ON CONFLICT (affiliate_id, deposit_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        load_timestamp = EXCLUDED.load_timestamp,
                        api_run_timestamp = EXCLUDED.api_run_timestamp,
                        source_file = EXCLUDED.source_file,
                        load_status = EXCLUDED.load_status
                """, (affiliate_id, record_id, json.dumps(record), current_time, 
                      datetime.fromtimestamp(file_stats.st_mtime), str(file_path)))
                
            elif table_name == 'bronze_trades':
                cur.execute("""
                    INSERT INTO bronze_trades 
                    (affiliate_id, trade_id, data, load_timestamp, api_run_timestamp, source_file, load_status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS')
                    ON CONFLICT (affiliate_id, trade_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        load_timestamp = EXCLUDED.load_timestamp,
                        api_run_timestamp = EXCLUDED.api_run_timestamp,
                        source_file = EXCLUDED.source_file,
                        load_status = EXCLUDED.load_status
                """, (affiliate_id, record_id, json.dumps(record), current_time, 
                      datetime.fromtimestamp(file_stats.st_mtime), str(file_path)))
            
        logger.info(f"Successfully processed {file_path}")
            
    except Exception as e:
        # Update the record with error status
        cur.execute(f"""
            UPDATE {table_name}
            SET load_status = 'ERROR',
                error_message = %s
            WHERE affiliate_id = %s AND {'asset_id' if table_name == 'bronze_assets' else 'customer_id' if table_name == 'bronze_customers' else 'deposit_id' if table_name == 'bronze_deposits' else 'trade_id'} = %s
        """, (str(e), affiliate_id, record_id))
        logger.error(f"Error processing {file_path}: {e}")
        raise

def main():
    """Main function to load raw data into bronze layer"""
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        
        # Create tables
        create_bronze_tables(conn)
        
        # Process each affiliate directory
        bronze_dir = os.path.join('data', 'bronze')
        with conn.cursor() as cur:
            for affiliate_dir in os.listdir(bronze_dir):
                if not affiliate_dir.startswith('affiliate'):
                    continue
                    
                affiliate_id = affiliate_dir
                logger.info(f"Processing affiliate {affiliate_id}")
                
                # Process affiliate data
                affiliate_data = {
                    "id": affiliate_id,
                    "name": f"Affiliate {affiliate_id}",
                    "email": f"{affiliate_id}@example.com",
                    "join_date": datetime.now().isoformat()
                }
                
                cur.execute("""
                    INSERT INTO bronze_affiliates 
                    (affiliate_id, data, load_timestamp, api_run_timestamp, source_file, load_status)
                    VALUES (%s, %s, %s, %s, %s, 'SUCCESS')
                    ON CONFLICT (affiliate_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        load_timestamp = EXCLUDED.load_timestamp,
                        api_run_timestamp = EXCLUDED.api_run_timestamp,
                        source_file = EXCLUDED.source_file,
                        load_status = EXCLUDED.load_status
                """, (affiliate_id, json.dumps(affiliate_data), datetime.now(), datetime.now(), str(os.path.join(bronze_dir, affiliate_dir))))
                
                # Process other data types in the correct order
                affiliate_path = os.path.join(bronze_dir, affiliate_dir)
                
                # 1. Process customers first
                customers_path = os.path.join(affiliate_path, 'customer_list')
                if os.path.exists(customers_path):
                    logger.info(f"Processing customers for {affiliate_id}")
                    process_directory(cur, customers_path, 'bronze_customers', affiliate_id)
                
                # 2. Process assets
                assets_path = os.path.join(affiliate_path, 'assets')
                if os.path.exists(assets_path):
                    logger.info(f"Processing assets for {affiliate_id}")
                    process_directory(cur, assets_path, 'bronze_assets', affiliate_id)
                
                # 3. Process deposits
                deposits_path = os.path.join(affiliate_path, 'deposits')
                if os.path.exists(deposits_path):
                    logger.info(f"Processing deposits for {affiliate_id}")
                    process_directory(cur, deposits_path, 'bronze_deposits', affiliate_id)
                
                # 4. Process trades
                trades_path = os.path.join(affiliate_path, 'trade_activities')
                if os.path.exists(trades_path):
                    logger.info(f"Processing trades for {affiliate_id}")
                    process_directory(cur, trades_path, 'bronze_trades', affiliate_id)
                
            conn.commit()
            logger.info("Successfully loaded all bronze data")
            
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 