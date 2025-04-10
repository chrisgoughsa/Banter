import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
from typing import Dict, Any
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

def create_silver_tables(conn):
    """Create silver layer tables matching the existing schema"""
    with conn.cursor() as cur:
        # Create AffiliateAccount table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS AffiliateAccount (
                affiliate_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                join_date TIMESTAMP,
                metadata JSONB
            )
        """)
        
        # Create ClientAccount table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ClientAccount (
                client_id VARCHAR(50) PRIMARY KEY,
                affiliate_id VARCHAR(50),
                register_time TIMESTAMP,
                country VARCHAR(50),
                metadata JSONB,
                FOREIGN KEY (affiliate_id) REFERENCES AffiliateAccount(affiliate_id)
            )
        """)
        
        # Create Deposits table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Deposits (
                deposit_id VARCHAR(50) PRIMARY KEY,
                client_id VARCHAR(50),
                deposit_time TIMESTAMP,
                deposit_coin VARCHAR(20),
                deposit_amount DECIMAL,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
        # Create TradeActivities table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TradeActivities (
                trade_activity_id VARCHAR(50) PRIMARY KEY,
                client_id VARCHAR(50),
                trade_time TIMESTAMP,
                symbol VARCHAR(20),
                trade_volume DECIMAL,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
        # Create Assets table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Assets (
                asset_id VARCHAR(50) PRIMARY KEY,
                client_id VARCHAR(50),
                balance DECIMAL,
                last_update_time TIMESTAMP,
                remark TEXT,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
    conn.commit()

def process_affiliates(conn):
    """Process affiliate data from bronze tables"""
    with conn.cursor() as cur:
        # Get all affiliates from bronze_affiliates
        cur.execute("SELECT affiliate_id, data FROM bronze_affiliates")
        for row in cur.fetchall():
            affiliate_id, data = row
            cleaned_data = clean_affiliate_data(data)
            
            try:
                cur.execute("""
                    INSERT INTO AffiliateAccount (
                        affiliate_id, name, email, join_date, metadata
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (affiliate_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        join_date = EXCLUDED.join_date,
                        metadata = EXCLUDED.metadata
                """, (
                    cleaned_data['affiliate_id'],
                    cleaned_data['name'],
                    cleaned_data['email'],
                    cleaned_data['join_date'],
                    cleaned_data['metadata']
                ))
            except Exception as e:
                logger.error(f"Error processing affiliate {affiliate_id}: {e}")
                conn.rollback()
                continue
        
    conn.commit()
    logger.info("Completed processing affiliates")

def standardize_date(date_str: str) -> datetime:
    """Standardize various date formats to datetime"""
    if not date_str:
        return None
        
    try:
        # First try to handle millisecond timestamps
        if str(date_str).isdigit():
            timestamp = int(date_str)
            # Convert milliseconds to seconds if needed
            if timestamp > 1000000000000:  # If timestamp is in milliseconds
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp)
            
        # Then try various string formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        logger.warning(f"Could not parse date string: {date_str}")
        return None
    except Exception as e:
        logger.warning(f"Error parsing date {date_str}: {e}")
        return None

def clean_affiliate_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize affiliate data"""
    cleaned = {
        'affiliate_id': data.get('id'),
        'name': data.get('name', '').strip(),
        'email': data.get('email', '').lower().strip(),
        'join_date': standardize_date(data.get('join_date')),
        'metadata': json.dumps(data)
    }
    
    return cleaned

def clean_client_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize client data"""
    cleaned = {
        'client_id': data.get('uid'),
        'affiliate_id': data.get('affiliate_id'),
        'register_time': standardize_date(data.get('registerTime')),
        'country': data.get('country', ''),
        'metadata': json.dumps({
            'source': 'bronze_customers',
            'original_data': data
        })
    }
    return cleaned

def clean_deposit_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize deposit data"""
    cleaned = {
        'deposit_id': data.get('orderId'),
        'client_id': data.get('uid'),
        'deposit_time': standardize_date(data.get('depositTime')),
        'deposit_coin': data.get('depositCoin', ''),
        'deposit_amount': float(data.get('depositAmount', 0))
    }
    return cleaned

def clean_trade_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize trade data"""
    # Get and validate timestamp
    timestamp = data.get('time', '')
    trade_time = standardize_date(timestamp)
    if not trade_time:
        logger.warning(f"Invalid trade time: {timestamp}, data: {data}")
        return None  # Skip invalid trades
    
    # Generate a unique trade_activity_id
    trade_activity_id = f"{data.get('uid')}_{timestamp}"
    
    try:
        # Handle the misspelled 'volumn' field and convert to float
        trade_volume = float(data.get('volumn', 0))
        if trade_volume < 0:
            logger.warning(f"Negative trade volume found: {trade_volume}, setting to 0")
            trade_volume = 0
    except (ValueError, TypeError):
        logger.warning(f"Invalid trade volume value: {data.get('volumn')}, setting to 0")
        trade_volume = 0.0
    
    cleaned = {
        'trade_activity_id': trade_activity_id,
        'client_id': data.get('uid'),
        'trade_time': trade_time,
        'symbol': data.get('symbol', 'UNKNOWN'),
        'trade_volume': trade_volume
    }
    
    # Log successful cleaning
    logger.debug(f"Cleaned trade data: {cleaned}")
    return cleaned

def clean_asset_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize asset data"""
    cleaned = {
        'asset_id': data.get('uid'),  # Use uid as asset_id
        'client_id': data.get('uid'),  # Use uid as client_id
        'balance': float(data.get('balance', 0)),
        'last_update_time': standardize_date(data.get('uTime')),
        'remark': data.get('remark', ''),
        'metadata': json.dumps({
            'source': 'bronze_assets',
            'original_data': data
        })
    }
    return cleaned

def process_customers(conn, bronze_dir):
    """Process all customer data files"""
    with conn.cursor() as cur:
        # Get all affiliate directories
        for affiliate_dir in os.listdir(bronze_dir):
            if not affiliate_dir.startswith('affiliate'):
                continue
                
            affiliate_id = affiliate_dir
            customer_dir = os.path.join(bronze_dir, affiliate_dir, 'customer_list')
            
            if not os.path.exists(customer_dir):
                logger.warning(f"Customer directory not found for {affiliate_id}")
                continue

            logger.info(f"Processing customers for affiliate {affiliate_id}")
            
            # First create the affiliate if it doesn't exist
            cur.execute("""
                INSERT INTO AffiliateAccount (affiliate_id, name)
                VALUES (%s, %s)
                ON CONFLICT (affiliate_id) DO NOTHING
            """, (affiliate_id, f"Affiliate {affiliate_id}"))
            
            # Process all year directories
            for year in os.listdir(customer_dir):
                year_dir = os.path.join(customer_dir, year)
                if not os.path.isdir(year_dir):
                    continue
                    
                # Process all month directories
                for month in os.listdir(year_dir):
                    month_dir = os.path.join(year_dir, month)
                    if not os.path.isdir(month_dir):
                        continue
                        
                    # Process all day directories
                    for day in os.listdir(month_dir):
                        day_dir = os.path.join(month_dir, day)
                        if not os.path.isdir(day_dir):
                            continue
                            
                        # Process all page files
                        for page_file in os.listdir(day_dir):
                            if not page_file.endswith('.json'):
                                continue
                                
                            page_path = os.path.join(day_dir, page_file)
                            try:
                                with open(page_path, 'r') as f:
                                    data = json.load(f)
                                    
                                if data.get('code') != '00000':
                                    logger.warning(f"Skipping {page_path}: API error {data.get('code')}")
                                    continue
                                    
                                customers_processed = 0
                                for customer in data.get('data', []):
                                    customer['affiliate_id'] = affiliate_id
                                    cleaned_data = clean_client_data(customer)
                                    
                                    try:
                                        cur.execute("""
                                            INSERT INTO ClientAccount (
                                                client_id, affiliate_id, register_time, country, metadata
                                            ) VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT (client_id) DO UPDATE
                                            SET affiliate_id = EXCLUDED.affiliate_id,
                                                register_time = EXCLUDED.register_time,
                                                country = EXCLUDED.country,
                                                metadata = EXCLUDED.metadata
                                        """, (
                                            cleaned_data['client_id'],
                                            cleaned_data['affiliate_id'],
                                            cleaned_data['register_time'],
                                            cleaned_data['country'],
                                            cleaned_data['metadata']
                                        ))
                                        customers_processed += 1
                                    except Exception as e:
                                        logger.error(f"Error processing client {cleaned_data['client_id']}: {e}")
                                        conn.rollback()
                                        continue
                                
                                logger.info(f"Processed {customers_processed} customers from {page_path}")
                                        
                            except Exception as e:
                                logger.error(f"Error processing file {page_path}: {e}")
                                continue
                                
            # Verify customers were added for this affiliate
            cur.execute("SELECT COUNT(*) FROM ClientAccount WHERE affiliate_id = %s", (affiliate_id,))
            count = cur.fetchone()[0]
            logger.info(f"Total customers for affiliate {affiliate_id}: {count}")
                                
        conn.commit()
        logger.info("Completed processing all customer data")

def process_assets(conn):
    """Process asset data from bronze tables"""
    with conn.cursor() as cur:
        # Get all assets from bronze_assets
        cur.execute("""
            SELECT a.affiliate_id, a.asset_id, a.data 
            FROM bronze_assets a
            JOIN bronze_customers c ON a.affiliate_id = c.affiliate_id AND a.asset_id = c.customer_id
        """)
        
        for row in cur.fetchall():
            affiliate_id, asset_id, data = row
            cleaned_data = clean_asset_data(data)
            
            try:
                cur.execute("""
                    INSERT INTO Assets (
                        asset_id, client_id, balance, last_update_time, remark
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (asset_id) DO UPDATE
                    SET balance = EXCLUDED.balance,
                        last_update_time = EXCLUDED.last_update_time,
                        remark = EXCLUDED.remark
                """, (
                    cleaned_data['asset_id'],
                    cleaned_data['client_id'],
                    cleaned_data['balance'],
                    cleaned_data['last_update_time'],
                    cleaned_data['remark']
                ))
            except Exception as e:
                logger.error(f"Error processing asset {asset_id} for affiliate {affiliate_id}: {e}")
                conn.rollback()
                continue
        
    conn.commit()
    logger.info("Completed processing assets")

def process_trades(conn, bronze_dir):
    """Process all trade data files"""
    with conn.cursor() as cur:
        trades_processed = 0
        trades_skipped = 0
        # Get all affiliate directories
        for affiliate_dir in os.listdir(bronze_dir):
            if not affiliate_dir.startswith('affiliate'):
                continue
                
            affiliate_id = affiliate_dir
            trades_dir = os.path.join(bronze_dir, affiliate_dir, 'trade_activities')
            
            if not os.path.exists(trades_dir):
                logger.warning(f"Trade activities directory not found for {affiliate_id}")
                continue
                
            logger.info(f"Processing trades for affiliate {affiliate_id}")
            
            # Process all year directories
            for year in os.listdir(trades_dir):
                year_dir = os.path.join(trades_dir, year)
                if not os.path.isdir(year_dir):
                    continue
                    
                # Process all month directories
                for month in os.listdir(year_dir):
                    month_dir = os.path.join(year_dir, month)
                    if not os.path.isdir(month_dir):
                        continue
                        
                    # Process all day directories
                    for day in os.listdir(month_dir):
                        day_dir = os.path.join(month_dir, day)
                        if not os.path.isdir(day_dir):
                            continue
                            
                        # Process all page files
                        for page_file in os.listdir(day_dir):
                            if not page_file.endswith('.json'):
                                continue
                                
                            page_path = os.path.join(day_dir, page_file)
                            try:
                                with open(page_path, 'r') as f:
                                    data = json.load(f)
                                    
                                if data.get('code') != '00000':
                                    logger.warning(f"Skipping {page_path}: API error {data.get('code')}")
                                    continue
                                    
                                page_trades_processed = 0
                                page_trades_skipped = 0
                                for trade in data.get('data', []):
                                    cleaned_data = clean_trade_data(trade)
                                    if cleaned_data is None:
                                        page_trades_skipped += 1
                                        trades_skipped += 1
                                        continue
                                    
                                    try:
                                        cur.execute("""
                                            INSERT INTO TradeActivities (
                                                trade_activity_id, client_id, trade_time, symbol, trade_volume
                                            ) VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT (trade_activity_id) DO UPDATE
                                            SET client_id = EXCLUDED.client_id,
                                                trade_time = EXCLUDED.trade_time,
                                                symbol = EXCLUDED.symbol,
                                                trade_volume = EXCLUDED.trade_volume
                                        """, (
                                            cleaned_data['trade_activity_id'],
                                            cleaned_data['client_id'],
                                            cleaned_data['trade_time'],
                                            cleaned_data['symbol'],
                                            cleaned_data['trade_volume']
                                        ))
                                        page_trades_processed += 1
                                        trades_processed += 1
                                    except Exception as e:
                                        logger.error(f"Error processing trade {cleaned_data['trade_activity_id']}: {e}")
                                        conn.rollback()
                                        continue
                                
                                logger.info(f"Processed {page_trades_processed} trades, skipped {page_trades_skipped} trades from {page_path}")
                                        
                            except Exception as e:
                                logger.error(f"Error processing file {page_path}: {e}")
                                continue
            
            # Verify trades were added for this affiliate's customers
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(trade_volume), 0)
                FROM TradeActivities t
                JOIN ClientAccount c ON t.client_id = c.client_id
                WHERE c.affiliate_id = %s
            """, (affiliate_id,))
            count, volume = cur.fetchone()
            logger.info(f"Total trades for affiliate {affiliate_id}: {count}, Total volume: {volume}")
                                
        conn.commit()
        logger.info(f"Completed processing all trade data. Total trades processed: {trades_processed}, skipped: {trades_skipped}")

def transform_bronze_to_silver(conn):
    """Transform bronze data to silver layer"""
    try:
        # Create tables first
        create_silver_tables(conn)
        
        # Process affiliates and customers first
        process_affiliates(conn)
        process_customers(conn, "data/bronze")
        
        # Then process trades and other data
        process_trades(conn, "data/bronze")
        process_assets(conn)
        
        logger.info("Completed all transformations")
        
    except Exception as e:
        logger.error(f"Error in transformation process: {e}")
        conn.rollback()
        raise

def main():
    """Main function to transform bronze data into silver tables"""
    logger.info("Starting bronze to silver transformation")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Create tables
        create_silver_tables(conn)
        
        # Transform data
        transform_bronze_to_silver(conn)
        
        logger.info("Completed bronze to silver transformation")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 