import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime
from src.etl.bitget_etl import BitgetETL
from src.models.bitget_models import BitgetConfig
from src.models.etl_models import (
    CustomerRecord,
    TradeRecord,
    DepositRecord,
    AssetRecord
)

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
def mock_customer_record():
    return CustomerRecord(
        client_id='test_client',
        register_time='2023-01-01T00:00:00Z',
        status='active'
    )

@pytest.fixture
def mock_trade_record():
    return TradeRecord(
        trade_id='trade1',
        symbol='BTC/USDT',
        volume='1.0',
        type='buy'
    )

@pytest.fixture
def mock_deposit_record():
    return DepositRecord(
        deposit_id='deposit1',
        amount='1000.0',
        coin='USDT',
        status='completed'
    )

@pytest.fixture
def mock_asset_record():
    return AssetRecord(
        asset_id='asset1',
        symbol='BTC',
        balance='1.5',
        status='active'
    )

@pytest.fixture
def bitget_etl(mock_config):
    with patch('src.etl.bitget_etl.BitgetClient'):
        return BitgetETL(mock_config)

def test_etl_initialization(bitget_etl, mock_config):
    """Test ETL initialization."""
    assert bitget_etl.base_path == Path("data/bronze")
    assert isinstance(bitget_etl.timestamp, datetime)

@patch('builtins.open')
@patch('json.dump')
def test_save_to_bronze(mock_json_dump, mock_open, bitget_etl, mock_customer_record):
    """Test saving records to bronze layer."""
    records = [mock_customer_record]
    bitget_etl._save_to_bronze(records, "customer_list", "test_affiliate")
    
    # Verify file was opened for writing
    mock_open.assert_called_once()
    # Verify JSON dump was called
    mock_json_dump.assert_called_once()

@patch('src.etl.bitget_etl.BitgetClient.get_customer_list')
def test_extract_customer_list(mock_get_customers, bitget_etl, mock_customer_record):
    """Test customer list extraction."""
    mock_get_customers.return_value = [mock_customer_record]
    
    with patch.object(bitget_etl, '_save_to_bronze') as mock_save:
        bitget_etl.extract_customer_list('test_affiliate')
        mock_save.assert_called_once()

@patch('src.etl.bitget_etl.BitgetClient.get_trade_activities')
def test_extract_trade_activities(mock_get_trades, bitget_etl, mock_trade_record):
    """Test trade activities extraction."""
    mock_get_trades.return_value = [mock_trade_record]
    
    with patch.object(bitget_etl, '_save_to_bronze') as mock_save:
        bitget_etl.extract_trade_activities('test_affiliate', 'test_client')
        mock_save.assert_called_once()

@patch('src.etl.bitget_etl.BitgetClient.get_deposits')
def test_extract_deposits(mock_get_deposits, bitget_etl, mock_deposit_record):
    """Test deposits extraction."""
    mock_get_deposits.return_value = [mock_deposit_record]
    
    with patch.object(bitget_etl, '_save_to_bronze') as mock_save:
        bitget_etl.extract_deposits('test_affiliate', 'test_client')
        mock_save.assert_called_once()

@patch('src.etl.bitget_etl.BitgetClient.get_assets')
def test_extract_assets(mock_get_assets, bitget_etl, mock_asset_record):
    """Test assets extraction."""
    mock_get_assets.return_value = [mock_asset_record]
    
    with patch.object(bitget_etl, '_save_to_bronze') as mock_save:
        bitget_etl.extract_assets('test_affiliate', 'test_client')
        mock_save.assert_called_once()

def test_run_etl(bitget_etl):
    """Test complete ETL run."""
    with patch.object(bitget_etl, 'extract_customer_list') as mock_customers, \
         patch.object(bitget_etl, 'extract_trade_activities') as mock_trades, \
         patch.object(bitget_etl, 'extract_deposits') as mock_deposits, \
         patch.object(bitget_etl, 'extract_assets') as mock_assets:
        
        bitget_etl.run_etl('test_affiliate')
        
        mock_customers.assert_called_once()
        mock_trades.assert_called_once()
        mock_deposits.assert_called_once()
        mock_assets.assert_called_once()

def test_error_handling(bitget_etl):
    """Test error handling in ETL process."""
    with patch.object(bitget_etl, 'extract_customer_list', side_effect=Exception('Test error')):
        with pytest.raises(Exception) as exc_info:
            bitget_etl.run_etl('test_affiliate')
        assert str(exc_info.value) == 'Test error'

def test_pagination_handling(bitget_etl, mock_customer_record):
    """Test handling of paginated responses."""
    with patch('src.etl.bitget_etl.BitgetClient.get_customer_list') as mock_get_customers:
        # Simulate multiple pages of data
        mock_get_customers.side_effect = [
            [mock_customer_record] * 10,  # First page
            [mock_customer_record] * 5,   # Second page (partial)
            []                            # Empty page (end)
        ]
        
        with patch.object(bitget_etl, '_save_to_bronze') as mock_save:
            bitget_etl.extract_customer_list('test_affiliate')
            assert mock_save.call_count == 2  # Called for each non-empty page 