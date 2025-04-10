import time
import hmac
import base64
import hashlib
from typing import Dict, Any, Optional, List
import requests
from loguru import logger
from requests.exceptions import RequestException
from ..models.bitget_models import BitgetConfig, APIResponse
from ..models.etl_models import CustomerRecord, TradeRecord, DepositRecord, AssetRecord
import threading

class RateLimiter:
    """Token bucket rate limiter implementation."""
    
    def __init__(self, rate: float, burst: int):
        """
        Initialize the rate limiter.
        
        Args:
            rate: Number of tokens to add per second
            burst: Maximum number of tokens that can be accumulated
        """
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()
        
    def acquire(self) -> None:
        """Acquire a token, waiting if necessary."""
        with self.lock:
            now = time.time()
            # Add new tokens based on elapsed time
            time_passed = now - self.last_update
            new_tokens = time_passed * self.rate
            self.tokens = min(self.burst, self.tokens + new_tokens)
            self.last_update = now
            
            # Wait if no tokens available
            while self.tokens < 1:
                time.sleep(1 / self.rate)
                now = time.time()
                time_passed = now - self.last_update
                new_tokens = time_passed * self.rate
                self.tokens = min(self.burst, self.tokens + new_tokens)
                self.last_update = now
                
            self.tokens -= 1

class BitgetClient:
    """Client for interacting with the Bitget Broker API."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Bitget API client.
        
        Args:
            config: Configuration dictionary containing affiliate credentials
        """
        # Validate config using Pydantic model
        self.config = BitgetConfig(**config)
        self.base_url = self.config.base_url.rstrip('/')
        self.affiliates = {
            aff.id: {
                'name': aff.name,
                'api_key': aff.api_key,
                'api_secret': aff.api_secret,
                'api_passphrase': aff.api_passphrase
            }
            for aff in self.config.affiliates
        }
        # Initialize rate limiter (10 requests per second)
        self.rate_limiter = RateLimiter(rate=10.0, burst=10)
        
    def _get_timestamp(self) -> str:
        """Generate the required timestamp in milliseconds."""
        return str(int(time.time() * 1000))
    
    def _sign_request(self, api_secret: str, timestamp: str, method: str, request_path: str, body: str = '') -> str:
        """
        Create the ACCESS-SIGN for API authentication.
        
        Args:
            api_secret: API secret for the affiliate
            timestamp: Current timestamp in milliseconds
            method: HTTP method (GET, POST, etc.)
            request_path: API endpoint path
            body: Request body for POST requests
            
        Returns:
            Base64 encoded signature
        """
        if body and method.upper() == "POST":
            body_str = body
        else:
            body_str = ''
            
        message = f'{timestamp}{method.upper()}{request_path}{body_str}'
        signature = hmac.new(
            api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode()
    
    def _get_headers(self, affiliate_id: str, timestamp: str, signature: str) -> Dict[str, str]:
        """Generate headers for API requests."""
        affiliate = self.affiliates[affiliate_id]
        return {
            'ACCESS-KEY': affiliate['api_key'],
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': affiliate['api_passphrase'],
            'Content-Type': 'application/json',
            'locale': 'en-US'
        }
    
    def _make_request(self, affiliate_id: str, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> APIResponse:
        """
        Make an authenticated request to the Bitget API.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Request parameters
            
        Returns:
            API response as a Pydantic model
            
        Raises:
            RequestException: If the API request fails
            ValueError: If the response validation fails
        """
        if affiliate_id not in self.affiliates:
            raise ValueError(f"Invalid affiliate ID: {affiliate_id}")
            
        # Apply rate limiting
        self.rate_limiter.acquire()
            
        timestamp = self._get_timestamp()
        url = f"{self.base_url}{endpoint}"
        
        # Convert params to JSON string for POST requests
        body = ''
        if method.upper() == "POST" and params:
            body = str(params)
            
        signature = self._sign_request(
            self.affiliates[affiliate_id]['api_secret'],
            timestamp,
            method,
            endpoint,
            body
        )
        
        headers = self._get_headers(affiliate_id, timestamp, signature)
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.post(url, headers=headers, json=params)
                
            response.raise_for_status()
            return APIResponse(**response.json())
            
        except RequestException as e:
            logger.error(f"API request failed for affiliate {affiliate_id}: {str(e)}")
            if hasattr(e.response, 'json'):
                logger.error(f"Error response: {e.response.json()}")
            raise
    
    def get_customer_list(self, affiliate_id: str, start_time: Optional[int] = None, end_time: Optional[int] = None, page_no: int = 1, page_size: int = 10) -> List[CustomerRecord]:
        """
        Get list of customers/affiliates.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            start_time: Start time for filtering customers
            end_time: End time for filtering customers
            page_no: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            List of validated customer records
        """
        endpoint = '/api/broker/v1/agent/customerList'
        params = {
            "pageNo": str(page_no),
            "pageSize": str(page_size),
            "startTime": start_time,
            "endTime": end_time
        }
        
        logger.info(f"Fetching customer list for affiliate {affiliate_id} (page {page_no}, size {page_size})")
        response = self._make_request(affiliate_id, "POST", endpoint, params)
        
        if response.data:
            return [CustomerRecord(**record) for record in response.data]
        return []
    
    def get_trade_activities(self, affiliate_id: str, client_id: str, start_time: Optional[int] = None, end_time: Optional[int] = None, page_no: int = 1, page_size: int = 100) -> List[TradeRecord]:
        """
        Get trade activities for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            
        Returns:
            List of validated trade records
        """
        endpoint = '/api/broker/v1/agent/customerTradeVolumnList'
        params = {
            "clientId": client_id,
            "startTime": start_time,
            "endTime": end_time,
            "pageNo": page_no,
            "pageSize": page_size
        }
        logger.info(f"Fetching trade activities for client {client_id} under affiliate {affiliate_id}")
        response = self._make_request(affiliate_id, "POST", endpoint, params)
        
        if response.data:
            return [TradeRecord(**record) for record in response.data]
        return []
    
    def get_deposits(self, affiliate_id: str, client_id: str, start_time: Optional[int] = None, end_time: Optional[int] = None, page_no: int = 1, page_size: int = 100) -> List[DepositRecord]:
        """
        Get deposit history for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            start_time: Start time for filtering deposits
            end_time: End time for filtering deposits
            page_no: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            List of validated deposit records
        """
        endpoint = '/api/broker/v1/agent/DepositList'
        params = {
            "clientId": client_id,
            "startTime": start_time,
            "endTime": end_time,
            "pageNo": page_no,
            "pageSize": page_size
        }
        logger.info(f"Fetching deposits for client {client_id} under affiliate {affiliate_id}")
        response = self._make_request(affiliate_id, "POST", endpoint, params)
        
        if response.data:
            return [DepositRecord(**record) for record in response.data]
        return []
    
    def get_assets(self, affiliate_id: str, client_id: str,page_no: int = 1, page_size: int = 100) -> List[AssetRecord]:
        """
        Get asset information for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            page_no: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            List of validated asset records
        """
        endpoint = '/api/broker/v1/agent/AccounAssetsList'
        params = {
            "clientId": client_id,
            "pageNo": page_no,
            "pageSize": page_size
        }
        logger.info(f"Fetching assets for client {client_id} under affiliate {affiliate_id}")
        response = self._make_request(affiliate_id, "POST", endpoint, params)
        
        if response.data:
            return [AssetRecord(**record) for record in response.data]
        return []
    