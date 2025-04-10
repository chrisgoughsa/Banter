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

def load_assets(conn, bronze_dir: str):
    """Load asset data into bronze_assets table"""
    cur = conn.cursor()
    asset_pattern = os.path.join(bronze_dir, "affiliate*/assets/*.json")
    
    for file_path in glob.glob(asset_pattern):
        try:
            affiliate_id = extract_affiliate_id(file_path)
            data = process_json_file(file_path)
            if not data:
                continue

            # Get file stats for metadata
            file_stats = os.stat(file_path)
            current_time = datetime.now()

            # Insert into bronze_assets
            cur.execute("""
                INSERT INTO bronze_assets (
                    affiliate_id, asset_id, symbol, name, data,
                    load_timestamp, api_run_timestamp, source_file, load_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (affiliate_id, asset_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    load_timestamp = EXCLUDED.load_timestamp,
                    api_run_timestamp = EXCLUDED.api_run_timestamp,
                    load_status = EXCLUDED.load_status
            """, (
                affiliate_id,
                data.get('asset_id'),
                data.get('symbol'),
                data.get('name'),
                json.dumps(data),
                current_time,
                datetime.fromtimestamp(file_stats.st_mtime),
                file_path,
                'SUCCESS'
            ))
            
            logger.info(f"Loaded asset data from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading asset file {file_path}: {e}")
            if cur:
                cur.execute("""
                    INSERT INTO bronze_assets (
                        affiliate_id, asset_id, load_timestamp, load_status, error_message, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    affiliate_id,
                    data.get('asset_id') if data else None,
                    datetime.now(),
                    'ERROR',
                    str(e),
                    file_path
                ))
    
    conn.commit()

def load_customers(conn, bronze_dir: str):
    """Load customer data into bronze_customers table"""
    cur = conn.cursor()
    customer_pattern = os.path.join(bronze_dir, "affiliate*/customer_list/*.json")
    
    for file_path in glob.glob(customer_pattern):
        try:
            affiliate_id = extract_affiliate_id(file_path)
            data = process_json_file(file_path)
            if not data:
                continue

            # Get file stats for metadata
            file_stats = os.stat(file_path)
            current_time = datetime.now()

            # Insert into bronze_customers
            cur.execute("""
                INSERT INTO bronze_customers (
                    affiliate_id, customer_id, data,
                    load_timestamp, api_run_timestamp, source_file, load_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (affiliate_id, customer_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    load_timestamp = EXCLUDED.load_timestamp,
                    api_run_timestamp = EXCLUDED.api_run_timestamp,
                    load_status = EXCLUDED.load_status
            """, (
                affiliate_id,
                data.get('customer_id'),
                json.dumps(data),
                current_time,
                datetime.fromtimestamp(file_stats.st_mtime),
                file_path,
                'SUCCESS'
            ))
            
            logger.info(f"Loaded customer data from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading customer file {file_path}: {e}")
            if cur:
                cur.execute("""
                    INSERT INTO bronze_customers (
                        affiliate_id, customer_id, load_timestamp, load_status, error_message, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    affiliate_id,
                    data.get('customer_id') if data else None,
                    datetime.now(),
                    'ERROR',
                    str(e),
                    file_path
                ))
    
    conn.commit()

def load_deposits(conn, bronze_dir: str):
    """Load deposit data into bronze_deposits table"""
    cur = conn.cursor()
    deposit_pattern = os.path.join(bronze_dir, "affiliate*/deposits/*.json")
    
    for file_path in glob.glob(deposit_pattern):
        try:
            affiliate_id = extract_affiliate_id(file_path)
            data = process_json_file(file_path)
            if not data:
                continue

            # Get file stats for metadata
            file_stats = os.stat(file_path)
            current_time = datetime.now()

            # Insert into bronze_deposits
            cur.execute("""
                INSERT INTO bronze_deposits (
                    affiliate_id, deposit_id, data,
                    load_timestamp, api_run_timestamp, source_file, load_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (affiliate_id, deposit_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    load_timestamp = EXCLUDED.load_timestamp,
                    api_run_timestamp = EXCLUDED.api_run_timestamp,
                    load_status = EXCLUDED.load_status
            """, (
                affiliate_id,
                data.get('deposit_id'),
                json.dumps(data),
                current_time,
                datetime.fromtimestamp(file_stats.st_mtime),
                file_path,
                'SUCCESS'
            ))
            
            logger.info(f"Loaded deposit data from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading deposit file {file_path}: {e}")
            if cur:
                cur.execute("""
                    INSERT INTO bronze_deposits (
                        affiliate_id, deposit_id, load_timestamp, load_status, error_message, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    affiliate_id,
                    data.get('deposit_id') if data else None,
                    datetime.now(),
                    'ERROR',
                    str(e),
                    file_path
                ))
    
    conn.commit()

def load_trades(conn, bronze_dir: str):
    """Load trade data into bronze_trades table"""
    cur = conn.cursor()
    trade_pattern = os.path.join(bronze_dir, "affiliate*/trade_activities/*.json")
    
    for file_path in glob.glob(trade_pattern):
        try:
            affiliate_id = extract_affiliate_id(file_path)
            data = process_json_file(file_path)
            if not data:
                continue

            # Get file stats for metadata
            file_stats = os.stat(file_path)
            current_time = datetime.now()

            # Insert into bronze_trades
            cur.execute("""
                INSERT INTO bronze_trades (
                    affiliate_id, trade_id, data,
                    load_timestamp, api_run_timestamp, source_file, load_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (affiliate_id, trade_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    load_timestamp = EXCLUDED.load_timestamp,
                    api_run_timestamp = EXCLUDED.api_run_timestamp,
                    load_status = EXCLUDED.load_status
            """, (
                affiliate_id,
                data.get('trade_id'),
                json.dumps(data),
                current_time,
                datetime.fromtimestamp(file_stats.st_mtime),
                file_path,
                'SUCCESS'
            ))
            
            logger.info(f"Loaded trade data from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading trade file {file_path}: {e}")
            if cur:
                cur.execute("""
                    INSERT INTO bronze_trades (
                        affiliate_id, trade_id, load_timestamp, load_status, error_message, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    affiliate_id,
                    data.get('trade_id') if data else None,
                    datetime.now(),
                    'ERROR',
                    str(e),
                    file_path
                ))
    
    conn.commit()

def main():
    """Main function to load all data into bronze tables"""
    logger.info("Starting bronze layer data load")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Create bronze tables
        create_bronze_tables(conn)
        
        # Get bronze data directory
        bronze_dir = os.path.join(os.getcwd(), 'data', 'bronze')
        if not os.path.exists(bronze_dir):
            raise Exception(f"Bronze data directory not found: {bronze_dir}")
        
        # Load data into bronze tables
        load_assets(conn, bronze_dir)
        load_customers(conn, bronze_dir)
        load_deposits(conn, bronze_dir)
        load_trades(conn, bronze_dir)
        
        logger.info("Completed bronze layer data load")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 