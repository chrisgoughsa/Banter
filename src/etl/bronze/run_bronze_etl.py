#!/usr/bin/env python3
"""
Main script for running the bronze layer ETL process.
"""
import logging
import sys
from pathlib import Path
from typing import List

from src.config.settings import BRONZE_DIR
from src.utils.db import get_db_connection, DatabaseError
from src.etl.bronze.extractors import (
    CustomerExtractor, DepositExtractor,
    TradeExtractor, AssetExtractor
)
from src.etl.bronze.loaders import (
    CustomerLoader, DepositLoader,
    TradeLoader, AssetLoader
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_affiliate_ids() -> List[str]:
    """Get list of affiliate IDs from the bronze directory."""
    affiliate_dirs = [d for d in BRONZE_DIR.iterdir() if d.is_dir() and d.name.startswith('affiliate')]
    return [d.name.replace('affiliate', '') for d in affiliate_dirs]

def process_affiliate(affiliate_id: str, conn, days_back: int = 7) -> None:
    """Process all data types for a single affiliate."""
    logger.info(f"Processing affiliate {affiliate_id}")
    
    try:
        # Process customers
        customer_extractor = CustomerExtractor(affiliate_id)
        customer_loader = CustomerLoader(conn)
        customer_loader.create_customers_table()
        customers = list(customer_extractor.extract_customers(days_back))
        customer_loader.load_customers(customers)
        logger.info(f"Processed {len(customers)} customers for affiliate {affiliate_id}")

        # Process deposits
        deposit_extractor = DepositExtractor(affiliate_id)
        deposit_loader = DepositLoader(conn)
        deposit_loader.create_deposits_table()
        deposits = list(deposit_extractor.extract_deposits(days_back))
        deposit_loader.load_deposits(deposits)
        logger.info(f"Processed {len(deposits)} deposits for affiliate {affiliate_id}")

        # Process trades
        trade_extractor = TradeExtractor(affiliate_id)
        trade_loader = TradeLoader(conn)
        trade_loader.create_trades_table()
        trades = list(trade_extractor.extract_trades(days_back))
        trade_loader.load_trades(trades)
        logger.info(f"Processed {len(trades)} trades for affiliate {affiliate_id}")

        # Process assets
        asset_extractor = AssetExtractor(affiliate_id)
        asset_loader = AssetLoader(conn)
        asset_loader.create_assets_table()
        assets = list(asset_extractor.extract_assets(days_back))
        asset_loader.load_assets(assets)
        logger.info(f"Processed {len(assets)} assets for affiliate {affiliate_id}")

    except Exception as e:
        logger.error(f"Error processing affiliate {affiliate_id}: {e}")
        raise

def main():
    """Main ETL process."""
    logger.info("Starting bronze layer ETL process")
    
    try:
        # Get list of affiliate IDs to process
        affiliate_ids = get_affiliate_ids()
        if not affiliate_ids:
            logger.error("No affiliate directories found")
            sys.exit(1)
        
        logger.info(f"Found {len(affiliate_ids)} affiliates to process")
        
        # Process each affiliate
        with get_db_connection() as conn:
            for affiliate_id in affiliate_ids:
                process_affiliate(affiliate_id, conn)
                
        logger.info("Successfully completed bronze layer ETL process")
        
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 