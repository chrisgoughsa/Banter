import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from loguru import logger
from ..config.settings import config
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
                "batch": batch,
                "timestamp": self.timestamp.isoformat(),
                "total_records": len(records)
            }
        }
        
        # Save file with page number
        filename = f"batch_{batch}.json"
        file_path = save_path / filename
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Saved {endpoint} data for affiliate {affiliate_id} to {file_path}")
    
    def extract_customer_list(self, affiliate_id: str, page_no: int = None, page_size: int = None) -> None:
        """Extract and save customer list data with pagination."""
        try:
                page_no = 1
                page_size = self.etl_config.batch_size
                has_more = True

                while has_more:
                    logger.info(f"Fetching customer list page {page_no} for affiliate {affiliate_id}")
                    records = self.client.get_customer_list(
                        affiliate_id,
                        page_no=page_no,
                        page_size=page_size
                    )

                    if records:
                        self._save_to_bronze(records, "customer_list", affiliate_id, page_no)
                        has_more = len(records) == page_size
                    else:
                        logger.info(f"No more records found for affiliate {affiliate_id} on page {page_no}")
                        has_more = False

                    page_no += 1

            except Exception as e:
                logger.error(f"Failed to extract customer list for affiliate {affiliate_id}: {str(e)}")
                raise
    
    def extract_trade_activities(
        self,
        affiliate_id: str,
        client_id: Optional[str] = None,
        page_no: Optional[int] = None,
        page_size: Optional[int] = None,

    ) -> None:
        """
        Extract and save trade activity data for an affiliate's clients, using Bitget's max page size (1000).
        
        If page_no is provided, fetch only that page. Otherwise, fetch all pages until a page returns < 1000 records.

        Args:
            affiliate_id: ID of the affiliate
            client_id: Optional client UID (None = all clients)
            page_no: Optional specific page number to fetch (debug or partial reprocess)
            page_size: Optional number of records per page (default = 1000)
        """
        try:
            max_page_size = config.max_page_size

            if page_no is not None:
                logger.info(
                    f"Fetching trade activities page {page_no} for affiliate {affiliate_id}, "
                    f"client={client_id or 'ALL'}, start={start_time}, end={end_time}"
                )
                records = self.client.get_trade_activities(
                    affiliate_id=affiliate_id,
                    client_id=client_id,
                    page_no=page_no,
                    page_size=effective_page_size,
                    start_time=start_time,
                    end_time=end_time
                )
                if records:
                    self._save_to_bronze(records, "trade_activities", affiliate_id, page_no)
                else:
                    logger.info(f"No trade activity records on page {page_no}")
                return

            # Full pagination
            current_batch = 1
            while True:
            logger.info(
                f"Fetching trade activities page {current_page} for affiliate {affiliate_id}, "
                f"client={client_id or 'ALL'}, start={start_time}, end={end_time}"
            )
            records = self.client.get_trade_activities(
                affiliate_id=affiliate_id,
                client_id=client_id,
                page_no=current_page,
                page_size=effective_page_size,
                start_time=start_time,
                end_time=end_time
            )

                if not records:
                    logger.info(f"No trade activity records returned for batch {current_batch}")
                    break

                self._save_to_bronze(records, "trade_activities", affiliate_id, current_batch)

                if len(records) < max_page_size:
                    logger.info(f"Final batch reached: {current_batch}")
                    break

                current_batch += 1

        except Exception as e:
            logger.error(f"Failed to extract trade activities for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def extract_deposits(
        self,
        affiliate_id: str,
        client_id: Optional[str] = None,
        page_no: Optional[int] = None,
        page_size: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> None:
        """
        Extract and save deposit records for a client (or all clients) using pagination and optional time range.
        
        Args:
            affiliate_id: ID of the affiliate
            client_id: Optional UID of a specific client (None = all clients)
            batch_no: Optional batch number to fetch (used for debugging or partial reprocess)
            page_size: Optional number of records per batch (default = config.max_page_size)
            start_time: Optional start timestamp in ms
            end_time: Optional end timestamp in ms
        """
        try:
            effective_page_size = page_size or config.max_page_size

            if page_no is not None:
                logger.info(
                    f"Fetching deposits batch {page_no} for affiliate {affiliate_id}, "
                    f"client={client_id or 'ALL'}, start={start_time}, end={end_time}"
                )
                records = self.client.get_deposits(
                    affiliate_id=affiliate_id,
                    client_id=client_id,
                    page_no=page_no,
                    page_size=effective_page_size,
                    start_time=start_time,
                    end_time=end_time
                )
                if records:
                    self._save_to_bronze(records, "deposits", affiliate_id, page_no)
                else:
                    logger.info(f"No deposit records found on page {page_no}")
                return

            # Full pagination loop
            current_batch = 1
            while True:
                logger.info(
                    f"Fetching deposits batch {current_batch} for affiliate {affiliate_id}, "
                    f"client={client_id or 'ALL'}, start={start_time}, end={end_time}"
                )
                records = self.client.get_deposits(
                    affiliate_id=affiliate_id,
                    client_id=client_id,
                    page_no=current_batch,
                    page_size=effective_page_size,
                    start_time=start_time,
                    end_time=end_time
                )

                if not records:
                    logger.info(f"No deposit records found in batch {current_batch}")
                    break

                self._save_to_bronze(records, "deposits", affiliate_id, current_batch)

                if len(records) < effective_page_size:
                    logger.info(f"Final batch reached: {current_batch}")
                    break

                current_batch += 1

        except Exception as e:
            logger.error(f"Failed to extract deposits for affiliate {affiliate_id}: {str(e)}")
            raise
    
    def extract_assets(
        self,
        affiliate_id: str,
        client_id: Optional[str] = None,
        page_no: Optional[int] = None,
        page_size: Optional[int] = None
    ) -> None:
        """
        Extract and save asset data for a client (or all clients) using pagination.

        Args:
            affiliate_id: ID of the affiliate
            client_id: Optional UID of the client
            page_no: Optional page number to fetch (used for debugging or partial reprocessing)
            page_size: Optional number of records per batch (default = config.max_page_size)
        """
        try:
            effective_page_size = page_size or config.max_page_size

            if page_no is not None:
                logger.info(
                    f"Fetching assets page {page_no} for affiliate {affiliate_id}, client={client_id or 'ALL'}"
                )
                records = self.client.get_assets(
                    affiliate_id=affiliate_id,
                    client_id=client_id,
                    page_no=batch_no,
                    page_size=effective_page_size
                )
                if records:
                    self._save_to_bronze(records, "assets", affiliate_id, page_no)
                else:
                    logger.info(f"No asset records found on page {page_no}")
                return

            # Full pagination loop
            current_batch = 1
            while True:
                logger.info(
                    f"Fetching assets batch {current_batch} for affiliate {affiliate_id}, client={client_id or 'ALL'}"
                )
                records = self.client.get_assets(
                    affiliate_id=affiliate_id,
                    client_id=client_id,
                    page_no=current_batch,
                    page_size=effective_page_size
                )

                if not records:
                    logger.info(f"No asset records found in batch {current_batch}")
                    break

                self._save_to_bronze(records, "assets", affiliate_id, current_batch)

                if len(records) < effective_page_size:
                    logger.info(f"Final batch reached: {current_batch}")
                    break

                current_batch += 1

        except Exception as e:
            logger.error(f"Failed to extract assets for affiliate {affiliate_id}: {str(e)}")
            raise

    
    def run_etl(
        self,
        affiliate_id: str,
        client_id: Optional[str] = None,
        page_no: Optional[int] = None,
        page_size: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> None:
        """
        Run the complete ETL pipeline for an affiliate. Can optionally run a targeted ETL
        for a specific client, page, and time range.

        Args:
            affiliate_id: ID of the affiliate
            client_id: (Optional) UID of the client to pull data for
            page_no: (Optional) Specific page number to fetch for all endpoints
            page_size: (Optional) Number of records per page (default: 1000, max: 1000)
            start_time: (Optional) Start of time window (timestamp in ms)
            end_time: (Optional) End of time window (timestamp in ms)
        """
        logger.info(f"Starting ETL for affiliate {affiliate_id} (client={client_id}, page_no={page_no})")

        # Extract customer list (only available at affiliate level)
        if client_id is None:
            self.extract_customer_list(affiliate_id, page_no=page_no, page_size=page_size)

        # Trade Activities
        self.extract_trade_activities(
            affiliate_id,
            client_id=client_id,
            page_no=page_no,
            page_size=page_size,
            start_time=start_time,
            end_time=end_time
        )

        # Deposits
        self.extract_deposits(
            affiliate_id,
            client_id=client_id,
            page_no=page_no,
            page_size=page_size,
            start_time=start_time,
            end_time=end_time
        )

        # Assets
        self.extract_assets(
            affiliate_id,
            client_id=client_id,
            page_no=page_no,
            page_size=page_size
        )

        logger.info(f"Completed ETL for affiliate {affiliate_id}")
