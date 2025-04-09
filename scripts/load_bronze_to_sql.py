import os
import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

# Database connection parameters from profiles.yml
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crypto_data_platform',
    'user': 'postgres',
    'password': 'postgres'
}

def create_tables(conn):
    """Create necessary tables in the database"""
    with conn.cursor() as cur:
        # Create assets table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_assets (
                affiliate_id VARCHAR(50),
                asset_id VARCHAR(50),
                symbol VARCHAR(50),
                name VARCHAR(100),
                data JSONB,
                PRIMARY KEY (affiliate_id, asset_id)
            )
        """)
        
        # Create customer_list table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_customers (
                affiliate_id VARCHAR(50),
                customer_id VARCHAR(50),
                data JSONB,
                PRIMARY KEY (affiliate_id, customer_id)
            )
        """)
        
        # Create deposits table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_deposits (
                affiliate_id VARCHAR(50),
                deposit_id VARCHAR(50),
                data JSONB,
                PRIMARY KEY (affiliate_id, deposit_id)
            )
        """)
        
        # Create trade_activities table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_trades (
                affiliate_id VARCHAR(50),
                trade_id VARCHAR(50),
                data JSONB,
                PRIMARY KEY (affiliate_id, trade_id)
            )
        """)
        
    conn.commit()

def process_json_files(conn, bronze_dir):
    """Process all JSON files in the bronze directory"""
    with conn.cursor() as cur:
        # Process each affiliate directory
        for affiliate_dir in os.listdir(bronze_dir):
            if not affiliate_dir.startswith('affiliate'):
                continue
                
            affiliate_id = affiliate_dir
            affiliate_path = os.path.join(bronze_dir, affiliate_dir)
            
            # Process assets
            assets_path = os.path.join(affiliate_path, 'assets')
            if os.path.exists(assets_path):
                for file in os.listdir(assets_path):
                    if file.endswith('.json'):
                        with open(os.path.join(assets_path, file), 'r') as f:
                            data = json.load(f)
                            cur.execute("""
                                INSERT INTO bronze_assets (affiliate_id, asset_id, symbol, name, data)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (affiliate_id, asset_id) DO UPDATE
                                SET symbol = EXCLUDED.symbol,
                                    name = EXCLUDED.name,
                                    data = EXCLUDED.data
                            """, (affiliate_id, data.get('id'), data.get('symbol'), data.get('name'), json.dumps(data)))
            
            # Process customer list
            customers_path = os.path.join(affiliate_path, 'customer_list')
            if os.path.exists(customers_path):
                for file in os.listdir(customers_path):
                    if file.endswith('.json'):
                        with open(os.path.join(customers_path, file), 'r') as f:
                            data = json.load(f)
                            cur.execute("""
                                INSERT INTO bronze_customers (affiliate_id, customer_id, data)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (affiliate_id, customer_id) DO UPDATE
                                SET data = EXCLUDED.data
                            """, (affiliate_id, data.get('id'), json.dumps(data)))
            
            # Process deposits
            deposits_path = os.path.join(affiliate_path, 'deposits')
            if os.path.exists(deposits_path):
                for file in os.listdir(deposits_path):
                    if file.endswith('.json'):
                        with open(os.path.join(deposits_path, file), 'r') as f:
                            data = json.load(f)
                            cur.execute("""
                                INSERT INTO bronze_deposits (affiliate_id, deposit_id, data)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (affiliate_id, deposit_id) DO UPDATE
                                SET data = EXCLUDED.data
                            """, (affiliate_id, data.get('id'), json.dumps(data)))
            
            # Process trade activities
            trades_path = os.path.join(affiliate_path, 'trade_activities')
            if os.path.exists(trades_path):
                for file in os.listdir(trades_path):
                    if file.endswith('.json'):
                        with open(os.path.join(trades_path, file), 'r') as f:
                            data = json.load(f)
                            cur.execute("""
                                INSERT INTO bronze_trades (affiliate_id, trade_id, data)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (affiliate_id, trade_id) DO UPDATE
                                SET data = EXCLUDED.data
                            """, (affiliate_id, data.get('id'), json.dumps(data)))
            
    conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Create tables
        create_tables(conn)
        
        # Process JSON files
        bronze_dir = os.path.join('data', 'bronze')
        process_json_files(conn, bronze_dir)
        
        print("Successfully loaded all bronze data into PostgreSQL")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 