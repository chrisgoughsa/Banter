import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from loguru import logger
from ..config.settings import ETL_CONFIG
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

    def _save_last_timestamp(self, affiliate_id: str, endpoint: str, timestamp: int):
        checkpoint_dir = Path("logs/checkpoints")
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_file = checkpoint_dir / f"last_timestamp_{affiliate_id}_{endpoint}.txt"
        with open(checkpoint_file, "w") as f:
            f.write(str(timestamp))

    def _load_last_timestamp(self, affiliate_id: str, endpoint: str) -> int:
        checkpoint_file = Path("logs/checkpoints") / f"last_timestamp_{affiliate_id}_{endpoint}.txt"
        try:
            with open(checkpoint_file, "r") as f:
                return int(f.read())
        except FileNotFoundError:
            return int((datetime.utcnow() - timedelta(minutes=10)).timestamp() * 1000)

    ### TO DO: investigate a better way to do this that can scale
    def _get_uid_cache_path(self, affiliate_id: str) -> Path:
        cache_dir = Path("./.bitget_cache")
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"seen_uids_{affiliate_id}.json"

    def _load_seen_uids(self, affiliate_id: str) -> set:
        path = self._get_uid_cache_path(affiliate_id)
        if path.exists():
            with open(path, "r") as f:
                return set(json.load(f))
        return set()

    def _store_seen_uids(self, affiliate_id: str, seen_uids: set) -> None:
        path = self._get_uid_cache_path(affiliate_id)
        with open(path, "w") as f:
            json.dump(list(seen_uids), f)
        
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
                "start_time": start_time,
                "end_time": end_time,
                "record_timestamp": self.timestamp.isoformat(),
                "records_fetched": len(records),
                "pages_fetched": page
            }
        }
        
        # Save file with page number
        filename = f"batch_{batch}.json"
        file_path = save_path / filename
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Saved {endpoint} data for affiliate {affiliate_id} to {file_path}")

    def extract_customer_list(
        self,
        affiliate_id: str,
        page_size: Optional[int] = None,
        page_no: Optional[int] = None
    ) -> None:
        """
        Extract and save the customer list for an affiliate.
        If `page_no` is provided, fetch and save only that specific page.
        Otherwise, fetch all pages starting from page 1 until no new records remain.

        Args:
            affiliate_id: ID of the affiliate
            page_size: Optional number of records per page (default = config value or 1000)
            page_no: Optional specific page number to fetch (default is full pagination)
        """
        try:
            effective_page_size = page_size or self.etl_config.max_page_size or 1000
            seen_uids = self._load_seen_uids(affiliate_id)
            pages_to_fetch = [page_no] if page_no is not None else range(1, 999999)  # effectively "until break"

            for current_page in pages_to_fetch:
                logger.info(f"Fetching customer list page {current_page} for affiliate {affiliate_id}")
                records = self.client.get_customer_list(
                    affiliate_id=affiliate_id,
                    page_no=current_page,
                    page_size=effective_page_size
                )

                if not records:
                    logger.info(f"No customer records found on page {current_page}")
                    break

                # If polling mode: skip already seen
                new_records = (
                    records if page_no is not None  # direct page fetch mode
                    else [rec for rec in records if rec["uid"] not in seen_uids]
                )

                if not new_records:
                    logger.info(f"All records on page {current_page} already processed.")
                    break

                self._save_to_bronze(new_records, "customer_list", affiliate_id, current_page)

                if page_no is not None or len(records) < effective_page_size:
                    logger.info(f"Final page reached: {current_page}")
                    break

                # Only add new uids if polling
                if page_no is None:
                    seen_uids.update(rec["uid"] for rec in new_records)

            if page_no is None:
                self._store_seen_uids(affiliate_id, seen_uids)

        except Exception as e:
            logger.error(f"Failed to extract customer list for affiliate {affiliate_id}: {str(e)}")
            raise

    def extract_trade_activities(
        self,
        affiliate_id: str,
        client_id: Optional[str] = None,
        page_size: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> None:
        """
        Extract and save trade activity data for an affiliate's clients using time-based pagination.

        Args:
            affiliate_id: ID of the affiliate
            client_id: Optional client UID (None = all clients)
            page_size: Optional number of records per page (default = config value or 1000)
            start_time: Optional start time in milliseconds (defaults to last saved timestamp)
            end_time: Optional end time in milliseconds (defaults to now + time window)
        """
        try:
            effective_page_size = page_size or self.etl_config.max_page_size or 1000

            # Resolve default time window if needed
            start_ts = start_time or self._load_last_timestamp(affiliate_id, "trade_activities")
            end_ts = end_time or (start_ts + int(self.time_window.total_seconds() * 1000))

            current_page = 1
            has_more = True

            logger.info(
                f"Fetching trade activities from {start_ts} to {end_ts} for affiliate {affiliate_id}, "
                f"client={client_id or 'ALL'}"
            )

            while has_more:
                try:
                    records = self.client.get_trade_activities(
                        affiliate_id=affiliate_id,
                        client_id=client_id,
                        page_no=current_page,
                        page_size=effective_page_size,
                        start_time=start_ts,
                        end_time=end_ts
                    )

                    if not records:
                        logger.info(f"No trade activity records on page {current_page}")
                        break

                    self._save_to_bronze(records, "trade_activities", affiliate_id, current_page)

                    if len(records) < effective_page_size:
                        has_more = False
                    else:
                        current_page += 1

                except Exception as e:
                    logger.error(f"Error fetching trade activities page {current_page}: {e}")
                    has_more = False

            # Save next start time only if we used default
            if start_time is None and end_time is None:
                self._save_last_timestamp(affiliate_id, "trade_activities", end_ts)

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
            page_no: Optional page number to fetch (debug mode)
            page_size: Optional number of records per page (default = config.max_page_size)
            start_time: Optional start timestamp in ms (defaults to last saved timestamp)
            end_time: Optional end timestamp in ms (defaults to start_time + time window)
        """
        try:
            effective_page_size = page_size or self.etl_config.max_page_size or 1000

            # Normal run: incremental time-based pagination
            start_ts = start_time or self._load_last_timestamp(affiliate_id, "deposits")
            end_ts = end_time or (start_ts + int(self.time_window.total_seconds() * 1000))

            current_page = 1
            has_more = True

            logger.info(
                f"Fetching deposits from {start_ts} to {end_ts} for affiliate {affiliate_id}, "
                f"client={client_id or 'ALL'}"
            )

            while has_more:
                try:
                    records = self.client.get_deposits(
                        affiliate_id=affiliate_id,
                        client_id=client_id,
                        page_no=current_page,
                        page_size=effective_page_size,
                        start_time=start_ts,
                        end_time=end_ts
                    )

                    if not records:
                        logger.info(f"No deposit records found on page {current_page}")
                        break

                    self._save_to_bronze(records, "deposits", affiliate_id, current_page)

                    if len(records) < effective_page_size:
                        logger.info(f"Final page reached: {current_page}")
                        break

                    current_page += 1

                except Exception as e:
                    logger.error(f"Error fetching deposits page {current_page}: {e}")
                    has_more = False

            # Save timestamp only for default-run mode
            if start_time is None and end_time is None:
                self._save_last_timestamp(affiliate_id, "deposits", end_ts)

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
        effective_page_size = page_size or self.etl_config.max_page_size or 1000

        # Full pagination loop
        current_page = 1
        while True:
            logger.info(
                f"Fetching assets page {current_page} for affiliate {affiliate_id}, client={client_id or 'ALL'}"
            )
            records = self.client.get_assets(
                affiliate_id=affiliate_id,
                client_id=client_id,
                page_no=current_page,
                page_size=effective_page_size
            )

            if not records:
                logger.info(f"No asset records found on page {current_page}")
                break

            self._save_to_bronze(records, "assets", affiliate_id, current_page)

            if len(records) < effective_page_size:
                logger.info(f"Final page reached: {current_page}")
                break

            current_page += 1

    except Exception as e:
        logger.error(f"Failed to extract assets for affiliate {affiliate_id}: {str(e)}")
        raise

    
    ### TO DO: add a function to run the ETL for a specific affiliate in future so that we can run it more frequently
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
