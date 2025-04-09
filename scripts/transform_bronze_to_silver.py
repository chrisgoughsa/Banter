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
    """Create silver layer tables with proper data types and constraints"""
    with conn.cursor() as cur:
        # Create silver customers table with standardized fields
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                affiliate_id VARCHAR(50) NOT NULL,
                email VARCHAR(255),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                country VARCHAR(100),
                registration_date TIMESTAMP,
                last_activity_date TIMESTAMP,
                status VARCHAR(50),
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create silver assets table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_assets (
                asset_id VARCHAR(50) PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                name VARCHAR(100),
                type VARCHAR(50),
                status VARCHAR(50),
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create silver deposits table with standardized fields
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_deposits (
                deposit_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50) REFERENCES silver_customers(customer_id),
                affiliate_id VARCHAR(50) NOT NULL,
                asset_id VARCHAR(50) REFERENCES silver_assets(asset_id),
                amount DECIMAL(20,8),
                status VARCHAR(50),
                deposit_date TIMESTAMP,
                processed_date TIMESTAMP,
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create silver trades table with standardized fields
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_trades (
                trade_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50) REFERENCES silver_customers(customer_id),
                affiliate_id VARCHAR(50) NOT NULL,
                base_asset_id VARCHAR(50) REFERENCES silver_assets(asset_id),
                quote_asset_id VARCHAR(50) REFERENCES silver_assets(asset_id),
                side VARCHAR(10),
                price DECIMAL(20,8),
                quantity DECIMAL(20,8),
                total_value DECIMAL(20,8),
                trade_date TIMESTAMP,
                status VARCHAR(50),
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create aggregated views
        cur.execute("""
            CREATE OR REPLACE VIEW silver_daily_trades AS
            SELECT 
                DATE_TRUNC('day', trade_date) as date,
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id,
                COUNT(*) as trade_count,
                SUM(quantity) as total_quantity,
                SUM(total_value) as total_value,
                AVG(price) as avg_price
            FROM silver_trades
            GROUP BY 
                DATE_TRUNC('day', trade_date),
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id
        """)
        
        cur.execute("""
            CREATE OR REPLACE VIEW silver_weekly_trades AS
            SELECT 
                DATE_TRUNC('week', trade_date) as week,
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id,
                COUNT(*) as trade_count,
                SUM(quantity) as total_quantity,
                SUM(total_value) as total_value,
                AVG(price) as avg_price
            FROM silver_trades
            GROUP BY 
                DATE_TRUNC('week', trade_date),
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id
        """)
        
        cur.execute("""
            CREATE OR REPLACE VIEW silver_monthly_trades AS
            SELECT 
                DATE_TRUNC('month', trade_date) as month,
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id,
                COUNT(*) as trade_count,
                SUM(quantity) as total_quantity,
                SUM(total_value) as total_value,
                AVG(price) as avg_price
            FROM silver_trades
            GROUP BY 
                DATE_TRUNC('month', trade_date),
                customer_id,
                affiliate_id,
                base_asset_id,
                quote_asset_id
        """)
        
    conn.commit()

def standardize_date(date_str: str) -> datetime:
    """Standardize various date formats to datetime"""
    if not date_str:
        return None
        
    try:
        # Try common date formats
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

def clean_customer_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize customer data"""
    cleaned = {
        'customer_id': data.get('id'),
        'affiliate_id': data.get('affiliate_id'),
        'email': data.get('email', '').lower().strip(),
        'first_name': data.get('first_name', '').strip(),
        'last_name': data.get('last_name', '').strip(),
        'country': data.get('country', '').strip(),
        'registration_date': standardize_date(data.get('registration_date')),
        'last_activity_date': standardize_date(data.get('last_activity_date')),
        'status': data.get('status', 'active').lower(),
        'raw_data': json.dumps(data)
    }
    
    # Handle missing values
    for key in ['email', 'first_name', 'last_name', 'country']:
        if not cleaned[key]:
            cleaned[key] = None
            
    return cleaned

def clean_asset_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize asset data"""
    cleaned = {
        'asset_id': data.get('id'),
        'symbol': data.get('symbol', '').upper().strip(),
        'name': data.get('name', '').strip(),
        'type': data.get('type', 'crypto').lower(),
        'status': data.get('status', 'active').lower(),
        'raw_data': json.dumps(data)
    }
    
    # Handle missing values
    for key in ['name', 'type']:
        if not cleaned[key]:
            cleaned[key] = None
            
    return cleaned

def clean_deposit_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize deposit data"""
    cleaned = {
        'deposit_id': data.get('id'),
        'customer_id': data.get('customer_id'),
        'affiliate_id': data.get('affiliate_id'),
        'asset_id': data.get('asset_id'),
        'amount': float(data.get('amount', 0)),
        'status': data.get('status', 'pending').lower(),
        'deposit_date': standardize_date(data.get('deposit_date')),
        'processed_date': standardize_date(data.get('processed_date')),
        'raw_data': json.dumps(data)
    }
    
    # Handle missing values
    for key in ['amount']:
        if cleaned[key] is None:
            cleaned[key] = 0
            
    return cleaned

def clean_trade_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and standardize trade data"""
    cleaned = {
        'trade_id': data.get('id'),
        'customer_id': data.get('customer_id'),
        'affiliate_id': data.get('affiliate_id'),
        'base_asset_id': data.get('base_asset_id'),
        'quote_asset_id': data.get('quote_asset_id'),
        'side': data.get('side', '').lower(),
        'price': float(data.get('price', 0)),
        'quantity': float(data.get('quantity', 0)),
        'total_value': float(data.get('total_value', 0)),
        'trade_date': standardize_date(data.get('trade_date')),
        'status': data.get('status', 'completed').lower(),
        'raw_data': json.dumps(data)
    }
    
    # Handle missing values
    for key in ['price', 'quantity', 'total_value']:
        if cleaned[key] is None:
            cleaned[key] = 0
            
    return cleaned

def transform_bronze_to_silver(conn):
    """Transform bronze data to silver layer"""
    with conn.cursor() as cur:
        # First, process assets to ensure referential integrity
        cur.execute("SELECT data FROM bronze_assets")
        for row in cur.fetchall():
            data = json.loads(row[0])
            cleaned_data = clean_asset_data(data)
            cur.execute("""
                INSERT INTO silver_assets (
                    asset_id, symbol, name, type, status, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (asset_id) DO UPDATE
                SET symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                cleaned_data['asset_id'],
                cleaned_data['symbol'],
                cleaned_data['name'],
                cleaned_data['type'],
                cleaned_data['status'],
                cleaned_data['raw_data']
            ))
        
        # Process customers
        cur.execute("SELECT data FROM bronze_customers")
        for row in cur.fetchall():
            data = json.loads(row[0])
            cleaned_data = clean_customer_data(data)
            cur.execute("""
                INSERT INTO silver_customers (
                    customer_id, affiliate_id, email, first_name, last_name,
                    country, registration_date, last_activity_date, status, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE
                SET email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    country = EXCLUDED.country,
                    registration_date = EXCLUDED.registration_date,
                    last_activity_date = EXCLUDED.last_activity_date,
                    status = EXCLUDED.status,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                cleaned_data['customer_id'],
                cleaned_data['affiliate_id'],
                cleaned_data['email'],
                cleaned_data['first_name'],
                cleaned_data['last_name'],
                cleaned_data['country'],
                cleaned_data['registration_date'],
                cleaned_data['last_activity_date'],
                cleaned_data['status'],
                cleaned_data['raw_data']
            ))
        
        # Process deposits
        cur.execute("SELECT data FROM bronze_deposits")
        for row in cur.fetchall():
            data = json.loads(row[0])
            cleaned_data = clean_deposit_data(data)
            cur.execute("""
                INSERT INTO silver_deposits (
                    deposit_id, customer_id, affiliate_id, asset_id,
                    amount, status, deposit_date, processed_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deposit_id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    affiliate_id = EXCLUDED.affiliate_id,
                    asset_id = EXCLUDED.asset_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    deposit_date = EXCLUDED.deposit_date,
                    processed_date = EXCLUDED.processed_date,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                cleaned_data['deposit_id'],
                cleaned_data['customer_id'],
                cleaned_data['affiliate_id'],
                cleaned_data['asset_id'],
                cleaned_data['amount'],
                cleaned_data['status'],
                cleaned_data['deposit_date'],
                cleaned_data['processed_date'],
                cleaned_data['raw_data']
            ))
        
        # Process trades
        cur.execute("SELECT data FROM bronze_trades")
        for row in cur.fetchall():
            data = json.loads(row[0])
            cleaned_data = clean_trade_data(data)
            cur.execute("""
                INSERT INTO silver_trades (
                    trade_id, customer_id, affiliate_id, base_asset_id,
                    quote_asset_id, side, price, quantity, total_value,
                    trade_date, status, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    affiliate_id = EXCLUDED.affiliate_id,
                    base_asset_id = EXCLUDED.base_asset_id,
                    quote_asset_id = EXCLUDED.quote_asset_id,
                    side = EXCLUDED.side,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    total_value = EXCLUDED.total_value,
                    trade_date = EXCLUDED.trade_date,
                    status = EXCLUDED.status,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                cleaned_data['trade_id'],
                cleaned_data['customer_id'],
                cleaned_data['affiliate_id'],
                cleaned_data['base_asset_id'],
                cleaned_data['quote_asset_id'],
                cleaned_data['side'],
                cleaned_data['price'],
                cleaned_data['quantity'],
                cleaned_data['total_value'],
                cleaned_data['trade_date'],
                cleaned_data['status'],
                cleaned_data['raw_data']
            ))
        
    conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Create silver tables and views
        create_silver_tables(conn)
        
        # Transform bronze data to silver
        transform_bronze_to_silver(conn)
        
        print("Successfully transformed bronze data to silver layer")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 