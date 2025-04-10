"""
Data extractors for the bronze layer.
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Generator
from datetime import datetime, timedelta

import pandas as pd

from src.config.settings import BRONZE_DIR
from src.utils.data_quality import validate_data_quality

logger = logging.getLogger(__name__)

class BronzeExtractor:
    """Base class for bronze layer data extraction."""
    
    def __init__(self, affiliate_id: str):
        self.affiliate_id = affiliate_id
        self.affiliate_dir = BRONZE_DIR / f"affiliate{affiliate_id}"
        self.data_dir = None  # Will be set by child classes
        
    def get_date_directories(self, days_back: int = 7) -> List[Path]:
        """Get list of date directories to process."""
        if self.data_dir is None:
            raise ValueError("data_dir must be set by child class")
            
        date_dirs = []
        logger.info(f"Looking for date directories in: {self.data_dir}")
        
        # Get current date
        current_date = datetime.now()
        
        # Get all year directories
        for year_dir in self.data_dir.glob("[0-9][0-9][0-9][0-9]"):
            logger.info(f"Found year directory: {year_dir}")
            year = int(year_dir.name)
            
            # Get all month directories
            for month_dir in year_dir.glob("[0-9][0-9]"):
                logger.info(f"Found month directory: {month_dir}")
                month = int(month_dir.name)
                
                # Get all day directories
                for day_dir in month_dir.glob("[0-9][0-9]"):
                    logger.info(f"Found day directory: {day_dir}")
                    day = int(day_dir.name)
                    
                    # Check if the date is within the range
                    dir_date = datetime(year, month, day)
                    if (current_date - dir_date).days <= days_back:
                        if day_dir.exists() and any(day_dir.glob("*.json")):
                            date_dirs.append(day_dir)
                            logger.info(f"Added directory {day_dir} to processing list")
        
        logger.info(f"Found {len(date_dirs)} date directories with JSON files in {self.data_dir}")
        return date_dirs

    def read_json_file(self, file_path: Path) -> Dict[str, Any]:
        """Read and parse a JSON file."""
        try:
            with open(file_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None

class CustomerExtractor(BronzeExtractor):
    """Extract customer data from raw JSON files."""
    
    def __init__(self, affiliate_id: str):
        super().__init__(affiliate_id)
        self.data_dir = self.affiliate_dir / "customer_list"
        logger.info(f"Initialized CustomerExtractor with data_dir: {self.data_dir}")
    
    def extract_customers(self, days_back: int = 7) -> Generator[Dict[str, Any], None, None]:
        """Extract customer data from JSON files."""
        date_dirs = self.get_date_directories(days_back)
        logger.info(f"Processing {len(date_dirs)} date directories for customers")
        
        for date_dir in date_dirs:
            logger.info(f"Processing directory: {date_dir}")
            for file_path in date_dir.glob("*.json"):
                logger.info(f"Processing file: {file_path}")
                data = self.read_json_file(file_path)
                if data and data.get("data"):
                    for customer in data["data"]:
                        yield {
                            "affiliate_id": self.affiliate_id,
                            "client_id": customer["uid"],
                            "register_time": datetime.fromtimestamp(int(customer["registerTime"]) / 1000),
                            "source_file": str(file_path),
                            "load_time": datetime.now()
                        }
                else:
                    logger.warning(f"No data found in file: {file_path}")

class DepositExtractor(BronzeExtractor):
    """Extract deposit data from raw JSON files."""
    
    def __init__(self, affiliate_id: str):
        super().__init__(affiliate_id)
        self.data_dir = self.affiliate_dir / "deposits"
        logger.info(f"Initialized DepositExtractor with data_dir: {self.data_dir}")
    
    def extract_deposits(self, days_back: int = 7) -> Generator[Dict[str, Any], None, None]:
        """Extract deposit data from JSON files."""
        date_dirs = self.get_date_directories(days_back)
        logger.info(f"Processing {len(date_dirs)} date directories for deposits")
        
        for date_dir in date_dirs:
            logger.info(f"Processing directory: {date_dir}")
            for file_path in date_dir.glob("*.json"):
                logger.info(f"Processing file: {file_path}")
                data = self.read_json_file(file_path)
                if data and data.get("data"):
                    for deposit in data["data"]:
                        yield {
                            "affiliate_id": self.affiliate_id,
                            "client_id": deposit["uid"],
                            "order_id": deposit["orderId"],
                            "deposit_time": datetime.fromtimestamp(int(deposit["depositTime"]) / 1000),
                            "deposit_coin": deposit["depositCoin"],
                            "deposit_amount": float(deposit["depositAmount"]),
                            "source_file": str(file_path),
                            "load_time": datetime.now()
                        }
                else:
                    logger.warning(f"No data found in file: {file_path}")

class TradeExtractor(BronzeExtractor):
    """Extract trade data from raw JSON files."""
    
    def __init__(self, affiliate_id: str):
        super().__init__(affiliate_id)
        self.data_dir = self.affiliate_dir / "trade_activities"
        logger.info(f"Initialized TradeExtractor with data_dir: {self.data_dir}")
    
    def extract_trades(self, days_back: int = 7) -> Generator[Dict[str, Any], None, None]:
        """Extract trade data from JSON files."""
        date_dirs = self.get_date_directories(days_back)
        logger.info(f"Processing {len(date_dirs)} date directories for trades")
        
        for date_dir in date_dirs:
            logger.info(f"Processing directory: {date_dir}")
            for file_path in date_dir.glob("*.json"):
                logger.info(f"Processing file: {file_path}")
                data = self.read_json_file(file_path)
                if data and data.get("data"):
                    for trade in data["data"]:
                        yield {
                            "affiliate_id": self.affiliate_id,
                            "client_id": trade["uid"],
                            "trade_volume": float(trade["volumn"]),  # Note: API uses "volumn" instead of "volume"
                            "trade_time": datetime.fromtimestamp(int(trade["time"]) / 1000),
                            "source_file": str(file_path),
                            "load_time": datetime.now()
                        }
                else:
                    logger.warning(f"No data found in file: {file_path}")

class AssetExtractor(BronzeExtractor):
    """Extract asset data from raw JSON files."""
    
    def __init__(self, affiliate_id: str):
        super().__init__(affiliate_id)
        self.data_dir = self.affiliate_dir / "assets"
        logger.info(f"Initialized AssetExtractor with data_dir: {self.data_dir}")
    
    def extract_assets(self, days_back: int = 7) -> Generator[Dict[str, Any], None, None]:
        """Extract asset data from JSON files."""
        date_dirs = self.get_date_directories(days_back)
        logger.info(f"Processing {len(date_dirs)} date directories for assets")
        
        for date_dir in date_dirs:
            logger.info(f"Processing directory: {date_dir}")
            for file_path in date_dir.glob("*.json"):
                logger.info(f"Processing file: {file_path}")
                data = self.read_json_file(file_path)
                if data and data.get("data"):
                    for asset in data["data"]:
                        yield {
                            "affiliate_id": self.affiliate_id,
                            "client_id": asset["uid"],
                            "balance": float(asset["balance"]),
                            "update_time": datetime.fromtimestamp(int(asset["uTime"]) / 1000),
                            "remark": asset["remark"],
                            "source_file": str(file_path),
                            "load_time": datetime.now()
                        }
                else:
                    logger.warning(f"No data found in file: {file_path}") 