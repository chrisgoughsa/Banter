import pytest
from unittest.mock import patch, MagicMock
from src.api.bitget_client import BitgetClient, RateLimiter
from src.models.bitget_models import BitgetConfig, APIResponse
from src.models.etl_models import CustomerRecord, TradeRecord, DepositRecord, AssetRecord

@pytest.fixture
def mock_config():
    return {
        'base_url': 'https://api.bitget.com',
        'affiliates': [
            {
                'id': 'test_affiliate',
                'name': 'Test Affiliate',
                'api_key': 'test_key',
                'api_secret': 'test_secret',
                'api_passphrase': 'test_passphrase'
            }
        ]
    }

@pytest.fixture
def bitget_client(mock_config):
    return BitgetClient(mock_config)

def test_rate_limiter():
    """Test the rate limiter functionality."""
    limiter = RateLimiter(rate=10.0, burst=10)
    
    # Test token consumption
    assert limiter.tokens == 10
    limiter.acquire()
    assert limiter.tokens == 9
    
    # Test token replenishment
    with patch('time.time', return_value=1.0):
        limiter.acquire()
        assert limiter.tokens == 8

def test_client_initialization(mock_config):
    """Test BitgetClient initialization."""
    client = BitgetClient(mock_config)
    assert client.base_url == 'https://api.bitget.com'
    assert 'test_affiliate' in client.affiliates
    assert client.affiliates['test_affiliate']['api_key'] == 'test_key'

@patch('requests.post')
def test_get_customer_list(mock_post, bitget_client):
    """Test customer list retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'code': '00000',
        'msg': 'success',
        'data': [
            {
                'clientId': 'test_client',
                'registerTime': '2023-01-01T00:00:00Z',
                'status': 'active'
            }
        ]
    }
    mock_post.return_value = mock_response
    
    customers = bitget_client.get_customer_list('test_affiliate')
    assert len(customers) == 1
    assert isinstance(customers[0], CustomerRecord)
    assert customers[0].client_id == 'test_client'

@patch('requests.post')
def test_get_trade_activities(mock_post, bitget_client):
    """Test trade activities retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'code': '00000',
        'msg': 'success',
        'data': [
            {
                'tradeId': 'trade1',
                'symbol': 'BTC/USDT',
                'volume': '1.0',
                'type': 'buy'
            }
        ]
    }
    mock_post.return_value = mock_response
    
    trades = bitget_client.get_trade_activities('test_affiliate', 'test_client')
    assert len(trades) == 1
    assert isinstance(trades[0], TradeRecord)
    assert trades[0].symbol == 'BTC/USDT'

@patch('requests.post')
def test_get_deposits(mock_post, bitget_client):
    """Test deposits retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'code': '00000',
        'msg': 'success',
        'data': [
            {
                'depositId': 'deposit1',
                'amount': '1000.0',
                'coin': 'USDT',
                'status': 'completed'
            }
        ]
    }
    mock_post.return_value = mock_response
    
    deposits = bitget_client.get_deposits('test_affiliate', 'test_client')
    assert len(deposits) == 1
    assert isinstance(deposits[0], DepositRecord)
    assert deposits[0].coin == 'USDT'

@patch('requests.post')
def test_get_assets(mock_post, bitget_client):
    """Test assets retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'code': '00000',
        'msg': 'success',
        'data': [
            {
                'assetId': 'asset1',
                'symbol': 'BTC',
                'balance': '1.5',
                'status': 'active'
            }
        ]
    }
    mock_post.return_value = mock_response
    
    assets = bitget_client.get_assets('test_affiliate', 'test_client')
    assert len(assets) == 1
    assert isinstance(assets[0], AssetRecord)
    assert assets[0].symbol == 'BTC'

def test_invalid_affiliate(bitget_client):
    """Test handling of invalid affiliate ID."""
    with pytest.raises(ValueError):
        bitget_client._make_request('invalid_affiliate', 'GET', '/test')

@patch('requests.post')
def test_api_error_handling(mock_post, bitget_client):
    """Test API error handling."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception('API Error')
    mock_post.return_value = mock_response
    
    with pytest.raises(Exception):
        bitget_client.get_customer_list('test_affiliate')

def test_rate_limiter_thread_safety():
    """Test rate limiter thread safety."""
    import threading
    limiter = RateLimiter(rate=10.0, burst=10)
    
    def acquire_token():
        limiter.acquire()
    
    threads = [threading.Thread(target=acquire_token) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    
    assert limiter.tokens == 5  # 10 - 5 threads 