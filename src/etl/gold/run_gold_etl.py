#!/usr/bin/env python3
"""
Main script for running the gold layer ETL process.
"""
import logging
import sys

from src.utils.db import get_db_connection, DatabaseError
from src.etl.gold.views import GoldViewManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_gold_views(conn) -> None:
    """Create gold layer views."""
    try:
        # Initialize view manager
        view_manager = GoldViewManager(conn)
        
        # Create all views
        view_manager.create_all_views()
        
    except Exception as e:
        logger.error(f"Error creating gold views: {e}")
        raise

def main():
    """Main ETL process."""
    logger.info("Starting gold layer ETL process")
    
    try:
        # Get database connection
        with get_db_connection() as conn:
            # Create gold views
            create_gold_views(conn)
            
        logger.info("Successfully completed gold layer ETL process")
        
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 