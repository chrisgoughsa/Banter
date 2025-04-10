#!/usr/bin/env python3
"""
Main entry point for the Crypto Data Platform.
This script can run the ETL pipeline and/or start the dashboard server.
"""
import argparse
import logging
import sys
import os
import uvicorn
from typing import List, Optional
from dotenv import load_dotenv
from loguru import logger
import click
from datetime import datetime
from pathlib import Path

from src.utils.db import get_db_connection, DatabaseError, create_db_connection
from src.etl.bronze.extractors import (
    CustomerExtractor, DepositExtractor,
    TradeExtractor, AssetExtractor
)
from src.etl.bronze.loaders import (
    CustomerLoader, DepositLoader,
    TradeLoader, AssetLoader
)
from src.etl.silver.transformers import (
    AffiliateTransformer,
    CustomerTransformer,
    DepositTransformer,
    TradeTransformer
)
from src.etl.gold.views import GoldViewManager
from src.config.settings import BRONZE_DIR, ETL_CONFIG
from src.dashboard.app.main import app as dashboard_app
from src.etl.bitget_etl import BitgetETL
from src.models.bitget_models import BitgetConfig
from src.utils.logging_config import setup_logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_affiliate_ids() -> List[str]:
    """Get list of affiliate IDs from the bronze directory."""
    affiliate_dirs = [d for d in BRONZE_DIR.iterdir() if d.is_dir() and d.name.startswith('affiliate')]
    # Extract IDs and sort numerically
    affiliate_ids = [d.name.replace('affiliate', '') for d in affiliate_dirs]
    return sorted(affiliate_ids, key=lambda x: int(x))

def load_bitget_config() -> BitgetConfig:
    """Load and validate Bitget configuration from environment variables."""
    load_dotenv()
    
    config = {
        'base_url': os.getenv('BITGET_BASE_URL'),
        'affiliates': []
    }
    
    # Load affiliate configurations
    affiliate_ids = os.getenv('BITGET_AFFILIATE_IDS', '').split(',')
    for aff_id in affiliate_ids:
        if not aff_id.strip():
            continue
            
        config['affiliates'].append({
            'id': aff_id.strip(),
            'name': os.getenv(f'BITGET_AFFILIATE_{aff_id}_NAME'),
            'api_key': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_KEY'),
            'api_secret': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_SECRET'),
            'api_passphrase': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_PASSPHRASE')
        })
    
    # Validate config using Pydantic model
    return BitgetConfig(**config)

def run_bitget_etl() -> None:
    """Run the Bitget ETL pipeline for all affiliates."""
    try:
        # Load and validate configuration
        config = load_bitget_config()
        
        # Initialize ETL
        etl = BitgetETL(config.dict())
        
        # Run ETL for each affiliate
        for affiliate in config.affiliates:
            affiliate_id = affiliate.id
            logger.info(f"Processing Bitget affiliate: {affiliate_id}")
            
            # Run ETL for affiliate-level data
            etl.run_etl(affiliate_id)
            
            # Optionally, run ETL for specific clients
            # client_ids = os.getenv(f'BITGET_AFFILIATE_{affiliate_id}_CLIENT_IDS', '').split(',')
            # for client_id in client_ids:
            #     if client_id.strip():
            #         etl.run_etl(affiliate_id, client_id.strip())
                
    except Exception as e:
        logger.error(f"Bitget ETL pipeline failed: {str(e)}")
        raise
        
def run_bronze_etl(conn, days_back: int = 7) -> None:
    """Run the bronze layer ETL process."""
    logger.info("Starting bronze layer ETL process")
    
    try:
        # Get list of affiliate IDs to process
        affiliate_ids = get_affiliate_ids()
        if not affiliate_ids:
            logger.error("No affiliate directories found")
            return
        
        logger.info(f"Found {len(affiliate_ids)} affiliates to process")
        
        # Create all tables first
        logger.info("Creating bronze tables")
        CustomerLoader(conn).create_customers_table()
        DepositLoader(conn).create_deposits_table()
        TradeLoader(conn).create_trades_table()
        AssetLoader(conn).create_assets_table()
        
        # Process each affiliate
        for affiliate_id in affiliate_ids:
            logger.info(f"Processing affiliate {affiliate_id}")
            
            # Process customers
            customer_extractor = CustomerExtractor(affiliate_id)
            customer_loader = CustomerLoader(conn)
            customers = list(customer_extractor.extract_customers(days_back))
            customer_loader.load_customers(customers)
            
            # Process deposits
            deposit_extractor = DepositExtractor(affiliate_id)
            deposit_loader = DepositLoader(conn)
            deposits = list(deposit_extractor.extract_deposits(days_back))
            deposit_loader.load_deposits(deposits)
            
            # Process trades
            trade_extractor = TradeExtractor(affiliate_id)
            trade_loader = TradeLoader(conn)
            trades = list(trade_extractor.extract_trades(days_back))
            trade_loader.load_trades(trades)
            
            # Process assets
            asset_extractor = AssetExtractor(affiliate_id)
            asset_loader = AssetLoader(conn)
            assets = list(asset_extractor.extract_assets(days_back))
            asset_loader.load_assets(assets)
            
        logger.info("Successfully completed bronze layer ETL process")
        
    except Exception as e:
        logger.error(f"Error in bronze layer ETL: {e}")
        raise

def run_silver_etl(conn) -> None:
    """Run the silver layer ETL process."""
    logger.info("Starting silver layer ETL process")
    
    try:
        # Drop all dependent objects first
        affiliate_transformer = AffiliateTransformer(conn)
        affiliate_transformer.drop_dependent_objects()

        # Transform affiliate data first (required for foreign keys)
        affiliate_transformer.create_affiliate_table()
        affiliate_transformer.transform_affiliates()

        # Transform customer data
        customer_transformer = CustomerTransformer(conn)
        customer_transformer.create_customer_table()
        customer_transformer.transform_customers()

        # Transform deposit data
        deposit_transformer = DepositTransformer(conn)
        deposit_transformer.create_deposit_table()
        deposit_transformer.transform_deposits()

        # Transform trade data
        trade_transformer = TradeTransformer(conn)
        trade_transformer.create_trade_table()
        trade_transformer.transform_trades()
        
        logger.info("Successfully completed silver layer ETL process")
        
    except Exception as e:
        logger.error(f"Error in silver layer ETL: {e}")
        raise

def run_gold_etl(conn) -> None:
    """Run the gold layer ETL process."""
    logger.info("Starting gold layer ETL process")
    
    try:
        # Initialize view manager
        view_manager = GoldViewManager(conn)
        
        # Create all views
        view_manager.create_all_views()
        
        logger.info("Successfully completed gold layer ETL process")
        
    except Exception as e:
        logger.error(f"Error in gold layer ETL: {e}")
        raise

def run_full_pipeline(days_back: int = 7) -> None:
    """Run the complete ETL pipeline."""
    logger.info("Starting complete ETL pipeline")
    
    conn = None
    try:
        # Create persistent connection
        conn = create_db_connection()
        
        # Run each layer in sequence
        run_bronze_etl(conn, days_back)
        run_silver_etl(conn)
        run_gold_etl(conn)
        
        logger.info("Successfully completed full ETL pipeline")
        
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def run_dashboard(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start the FastAPI dashboard server."""
    logger.info(f"Starting dashboard server on {host}:{port}")
    try:
        uvicorn.run(
            "src.dashboard.app.main:app",
            host=host,
            port=port,
            reload=True
        )
    except Exception as e:
        logger.error(f"Dashboard server failed: {e}")
        sys.exit(1)

def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Crypto Data Platform')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # ETL pipeline parser
    etl_parser = subparsers.add_parser('etl', help='Run ETL pipeline')
    etl_parser.add_argument(
        '--layer',
        choices=['bronze', 'silver', 'gold', 'all', 'bitget'],
        default='all',
        help='Specify which layer to process (default: all)'
    )
    etl_parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days of data to process (default: 7)'
    )
    
    # Dashboard parser
    dashboard_parser = subparsers.add_parser('dashboard', help='Start dashboard server')
    dashboard_parser.add_argument(
        '--host',
        type=str,
        default="0.0.0.0",
        help='Host to bind the server to (default: 0.0.0.0)'
    )
    dashboard_parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='Port to bind the server to (default: 8000)'
    )
    
    args = parser.parse_args()
    
    if args.command == 'etl':
        try:
            if args.layer == 'bitget':
                run_bitget_etl()
            else:
                with get_db_connection() as conn:
                    if args.layer == 'bronze':
                        run_bronze_etl(conn, args.days)
                    elif args.layer == 'silver':
                        run_silver_etl(conn)
                    elif args.layer == 'gold':
                        run_gold_etl(conn)
                    else:  # 'all'
                        run_full_pipeline(args.days)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            sys.exit(1)
    elif args.command == 'dashboard':
        run_dashboard(args.host, args.port)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main() 