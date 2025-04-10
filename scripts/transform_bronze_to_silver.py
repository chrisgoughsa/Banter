import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crypto_data_platform',
    'user': 'postgres',
    'password': 'postgres'
}

def create_silver_tables(conn):
    """Create silver layer tables matching the existing schema"""
    with conn.cursor() as cur:
        # Create AffiliateAccount table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS AffiliateAccount (
                affiliate_id STRING PRIMARY KEY,
                name STRING,
                email STRING,
                join_date TIMESTAMP,
                metadata JSON
            )
        """)
        
        # Create ClientAccount table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ClientAccount (
                client_id STRING PRIMARY KEY,
                affiliate_id STRING,
                register_time TIMESTAMP,
                country STRING,
                metadata JSON,
                FOREIGN KEY (affiliate_id) REFERENCES AffiliateAccount(affiliate_id)
            )
        """)
        
        # Create Deposits table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Deposits (
                deposit_id STRING PRIMARY KEY,
                client_id STRING,
                deposit_time TIMESTAMP,
                deposit_coin STRING,
                deposit_amount FLOAT64,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
        # Create TradeActivities table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TradeActivities (
                trade_activity_id STRING PRIMARY KEY,
                client_id STRING,
                trade_time TIMESTAMP,
                symbol STRING,
                trade_volume FLOAT64,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
        # Create Assets table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Assets (
                asset_id STRING PRIMARY KEY,
                client_id STRING,
                balance FLOAT64,
                last_update_time TIMESTAMP,
                remark STRING,
                FOREIGN KEY (client_id) REFERENCES ClientAccount(client_id)
            )
        """)
        
    conn.commit()

def process_affiliates(conn, bronze_dir):
    """Process affiliate data from directory structure"""
    with conn.cursor() as cur:
        # Get all affiliate directories
        for affiliate_dir in os.listdir(bronze_dir):
            if not affiliate_dir.startswith('affiliate'):
                continue
                
            affiliate_id = affiliate_dir
            # Create a basic affiliate record
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
                affiliate_id,
                f"Affiliate {affiliate_id}",
                f"{affiliate_id}@example.com",
                datetime.now(),
                json.dumps({"source": "directory_structure"})
            ))
        
    conn.commit()

def standardize_date(date_str: str) -> datetime:
    """Standardize various date formats to datetime"""
    if not date_str:
        return None
        
    try:
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
                
        return None
    except:
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
        'client_id': data.get('id'),
        'affiliate_id': data.get('affiliate_id'),
        'register_time': standardize_date(data.get('register_time')),
        'country': data.get('country', '').strip(),
        'metadata': json.dumps(data)
    }
    
    return cleaned

def clean_deposit_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize deposit data"""
    cleaned = {
        'deposit_id': data.get('id'),
        'client_id': data.get('client_id'),
        'deposit_time': standardize_date(data.get('deposit_time')),
        'deposit_coin': data.get('deposit_coin', '').upper().strip(),
        'deposit_amount': float(data.get('deposit_amount', 0))
    }
    
    return cleaned

def clean_trade_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize trade data"""
    cleaned = {
        'trade_activity_id': data.get('id'),
        'client_id': data.get('client_id'),
        'trade_time': standardize_date(data.get('trade_time')),
        'symbol': data.get('symbol', '').upper().strip(),
        'trade_volume': float(data.get('trade_volume', 0))
    }
    
    return cleaned

def clean_asset_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize asset data"""
    cleaned = {
        'asset_id': data.get('id'),
        'client_id': data.get('client_id'),
        'balance': float(data.get('balance', 0)),
        'last_update_time': standardize_date(data.get('last_update_time')),
        'remark': data.get('remark', '').strip()
    }
    
    return cleaned

def transform_bronze_to_silver(conn):
    """Transform bronze data to silver layer"""
    bronze_dir = os.path.join('data', 'bronze')
    
    # First process affiliates
    process_affiliates(conn, bronze_dir)
    
    with conn.cursor() as cur:
        # Process clients
        cur.execute("SELECT affiliate_id, customer_id, data FROM bronze_customers")
        for row in cur.fetchall():
            affiliate_id, customer_id, data_json = row
            data = json.loads(data_json)
            cleaned_data = clean_client_data(data)
            cleaned_data['affiliate_id'] = affiliate_id  # Ensure affiliate_id is set
            
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
        
        # Process deposits
        cur.execute("SELECT affiliate_id, deposit_id, data FROM bronze_deposits")
        for row in cur.fetchall():
            affiliate_id, deposit_id, data_json = row
            data = json.loads(data_json)
            cleaned_data = clean_deposit_data(data)
            
            cur.execute("""
                INSERT INTO Deposits (
                    deposit_id, client_id, deposit_time, deposit_coin, deposit_amount
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (deposit_id) DO UPDATE
                SET client_id = EXCLUDED.client_id,
                    deposit_time = EXCLUDED.deposit_time,
                    deposit_coin = EXCLUDED.deposit_coin,
                    deposit_amount = EXCLUDED.deposit_amount
            """, (
                cleaned_data['deposit_id'],
                cleaned_data['client_id'],
                cleaned_data['deposit_time'],
                cleaned_data['deposit_coin'],
                cleaned_data['deposit_amount']
            ))
        
        # Process trades
        cur.execute("SELECT affiliate_id, trade_id, data FROM bronze_trades")
        for row in cur.fetchall():
            affiliate_id, trade_id, data_json = row
            data = json.loads(data_json)
            cleaned_data = clean_trade_data(data)
            
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
        
        # Process assets
        cur.execute("SELECT affiliate_id, asset_id, data FROM bronze_assets")
        for row in cur.fetchall():
            affiliate_id, asset_id, data_json = row
            data = json.loads(data_json)
            cleaned_data = clean_asset_data(data)
            
            cur.execute("""
                INSERT INTO Assets (
                    asset_id, client_id, balance, last_update_time, remark
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (asset_id) DO UPDATE
                SET client_id = EXCLUDED.client_id,
                    balance = EXCLUDED.balance,
                    last_update_time = EXCLUDED.last_update_time,
                    remark = EXCLUDED.remark
            """, (
                cleaned_data['asset_id'],
                cleaned_data['client_id'],
                cleaned_data['balance'],
                cleaned_data['last_update_time'],
                cleaned_data['remark']
            ))
        
    conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Create tables
        create_silver_tables(conn)
        
        # Transform data
        transform_bronze_to_silver(conn)
        
        print("Successfully transformed bronze data to silver layer")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 