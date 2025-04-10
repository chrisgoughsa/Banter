import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from loguru import logger
from ..api.bitget_client import BitgetClient
from ..models.etl_models import (
    ETLConfig,
    CustomerRecord,
    TradeRecord,
    DepositRecord,
    AssetRecord,
    BronzeRecord
)

class BitgetETL:
    """ETL pipeline for Bitget API data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ETL pipeline.
        
        Args:
            config: Configuration dictionary containing Bitget credentials
        """
        self.client = BitgetClient(config)
        self.base_path = Path("data/bronze")
        self.timestamp = datetime.now()
        self.etl_config = ETLConfig()
        
    def _save_to_bronze(self, records: List[BronzeRecord], endpoint: str, affiliate_id: str, page: int = 1) -> None:
        """
        Save validated records to bronze layer with date-based organization and pagination.
        
        Args:
            records: List of validated records
            endpoint: API endpoint name
            affiliate_id: ID of the affiliate
            page: Page number for paginated responses
        """
        # Create date-based directory structure
        date_path = self.timestamp.strftime("%Y/%m/%d")
        save_path = self.base_path / affiliate_id / endpoint / date_path
        save_path.mkdir(parents=True, exist_ok=True)
        
        # Convert records to dict and add metadata
        data = {
            "records": [record.dict() for record in records],
            "metadata": {
                "affiliate_id": affiliate_id,
                "endpoint": endpoint,
                "page": page,
                "timestamp": self.timestamp.isoformat(),
                "total_records": len(records)
            }
        }
        
        # Save file with page number
        filename = f"page_{page}.json"
        file_path = save_path / filename
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Saved {endpoint} data for affiliate {affiliate_id} to {file_path}")
    
    def extract_customer_list(self, affiliate_id: str) -> None:
        """Extract and save customer list data with pagination."""
        try:
            page_no = 1
            has_more = True
            
            while has_more:
                records = self.client.get_customer_list(
                    affiliate_id,
                    page_no=page_no,
                    page_size=self.etl_config.batch_size
                )
                
                if records:
                    self._save_to_bronze(records, "customer_list", affiliate_id, page_no)
                    has_more = len(records) == self.etl_config.batch_size
                else:
                    has_more = False
                    
                page_no += 1
                
        except Exception as e:
            logger.error(f"Failed to extract customer list for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def extract_trade_activities(self, affiliate_id: str, client_id: str) -> None:
        """Extract and save trade activities data with pagination."""
        try:
            page_no = 1
            has_more = True
            
            while has_more:
                records = self.client.get_trade_activities(affiliate_id, client_id)
                
                if records:
                    self._save_to_bronze(records, "trade_activities", affiliate_id, page_no)
                    has_more = len(records) == self.etl_config.batch_size
                else:
                    has_more = False
                    
                page_no += 1
                
        except Exception as e:
            logger.error(f"Failed to extract trade activities for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def extract_deposits(self, affiliate_id: str, client_id: str) -> None:
        """Extract and save deposits data with pagination."""
        try:
            page_no = 1
            has_more = True
            
            while has_more:
                records = self.client.get_deposits(affiliate_id, client_id)
                
                if records:
                    self._save_to_bronze(records, "deposits", affiliate_id, page_no)
                    has_more = len(records) == self.etl_config.batch_size
                else:
                    has_more = False
                    
                page_no += 1
                
        except Exception as e:
            logger.error(f"Failed to extract deposits for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def extract_assets(self, affiliate_id: str, client_id: str) -> None:
        """Extract and save assets data with pagination."""
        try:
            page_no = 1
            has_more = True
            
            while has_more:
                records = self.client.get_assets(affiliate_id, client_id)
                
                if records:
                    self._save_to_bronze(records, "assets", affiliate_id, page_no)
                    has_more = len(records) == self.etl_config.batch_size
                else:
                    has_more = False
                    
                page_no += 1
                
        except Exception as e:
            logger.error(f"Failed to extract assets for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def run_etl(self, affiliate_id: str, client_id: str = None) -> None:
        """
        Run the complete ETL pipeline for an affiliate.
        
        Args:
            affiliate_id: ID of the affiliate
            client_id: Optional client ID for client-specific endpoints
        """
        logger.info(f"Starting ETL for affiliate {affiliate_id}")
        
        # Extract affiliate-level data
        self.extract_customer_list(affiliate_id)
        
        # Extract client-level data if client_id is provided
        if client_id:
            self.extract_trade_activities(affiliate_id, client_id)
            self.extract_deposits(affiliate_id, client_id)
            self.extract_assets(affiliate_id, client_id)
            
        logger.info(f"Completed ETL for affiliate {affiliate_id}") 