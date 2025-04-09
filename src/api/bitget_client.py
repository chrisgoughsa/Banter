import time
import hmac
import base64
import hashlib
from typing import Dict, Any, Optional, List
import requests
from loguru import logger
from requests.exceptions import RequestException

class BitgetClient:
    """Client for interacting with the Bitget Broker API."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Bitget API client.
        
        Args:
            config: Configuration dictionary containing affiliate credentials
        """
        self.base_url = config['base_url'].rstrip('/')
        self.affiliates = {
            aff['id']: {
                'name': aff['name'],
                'api_key': aff['api_key'],
                'api_secret': aff['api_secret'],
                'api_passphrase': aff['api_passphrase']
            }
            for aff in config['affiliates']
        }
        
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
    
    def _make_request(self, affiliate_id: str, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make an authenticated request to the Bitget API.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Request parameters
            
        Returns:
            API response as a dictionary
            
        Raises:
            RequestException: If the API request fails
        """
        if affiliate_id not in self.affiliates:
            raise ValueError(f"Invalid affiliate ID: {affiliate_id}")
            
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
            return response.json()
            
        except RequestException as e:
            logger.error(f"API request failed for affiliate {affiliate_id}: {str(e)}")
            if hasattr(e.response, 'json'):
                logger.error(f"Error response: {e.response.json()}")
            raise
    
    def get_customer_list(self, affiliate_id: str, page_no: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """
        Get list of customers/affiliates.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            page_no: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            Customer list response
        """
        endpoint = '/api/broker/v1/agent/customerList'
        params = {
            "pageNo": str(page_no),
            "pageSize": str(page_size)
        }
        
        logger.info(f"Fetching customer list for affiliate {affiliate_id} (page {page_no}, size {page_size})")
        return self._make_request(affiliate_id, "POST", endpoint, params)
    
    def get_account_info(self, affiliate_id: str) -> Dict[str, Any]:
        """
        Get account information.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            
        Returns:
            Account information response
        """
        endpoint = '/api/broker/v1/account/info'
        logger.info(f"Fetching account information for affiliate {affiliate_id}")
        return self._make_request(affiliate_id, "GET", endpoint)
    
    def get_trade_activities(self, affiliate_id: str, client_id: str) -> Dict[str, Any]:
        """
        Get trade activities for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            
        Returns:
            Trade activities response
        """
        endpoint = '/api/broker/v1/agent/tradeList'
        params = {
            "clientId": client_id
        }
        logger.info(f"Fetching trade activities for client {client_id} under affiliate {affiliate_id}")
        return self._make_request(affiliate_id, "POST", endpoint, params)
    
    def get_deposits(self, affiliate_id: str, client_id: str) -> Dict[str, Any]:
        """
        Get deposit history for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            
        Returns:
            Deposit history response
        """
        endpoint = '/api/broker/v1/agent/depositList'
        params = {
            "clientId": client_id
        }
        logger.info(f"Fetching deposits for client {client_id} under affiliate {affiliate_id}")
        return self._make_request(affiliate_id, "POST", endpoint, params)
    
    def get_assets(self, affiliate_id: str, client_id: str) -> Dict[str, Any]:
        """
        Get asset information for a client.
        
        Args:
            affiliate_id: ID of the affiliate making the request
            client_id: ID of the client
            
        Returns:
            Asset information response
        """
        endpoint = '/api/broker/v1/agent/assetList'
        params = {
            "clientId": client_id
        }
        logger.info(f"Fetching assets for client {client_id} under affiliate {affiliate_id}")
        return self._make_request(affiliate_id, "POST", endpoint, params)
    