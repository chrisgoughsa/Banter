import pytest
from unittest.mock import patch, MagicMock
from src.utils.db import get_db_connection, DatabaseError, create_db_connection
from src.etl.bronze.loaders import (
    CustomerLoader,
    DepositLoader,
    TradeLoader,
    AssetLoader
)
from src.etl.silver.transformers import (
    AffiliateTransformer,
    CustomerTransformer,
    DepositTransformer,
    TradeTransformer
)
from src.etl.gold.views import GoldViewManager

@pytest.fixture
def mock_conn():
    """Create a mock database connection."""
    mock = MagicMock()
    mock.cursor.return_value.__enter__.return_value = mock
    return mock

@pytest.fixture
def customer_loader(mock_conn):
    return CustomerLoader(mock_conn)

@pytest.fixture
def deposit_loader(mock_conn):
    return DepositLoader(mock_conn)

@pytest.fixture
def trade_loader(mock_conn):
    return TradeLoader(mock_conn)

@pytest.fixture
def asset_loader(mock_conn):
    return AssetLoader(mock_conn)

def test_customer_loader_create_table(customer_loader, mock_conn):
    """Test customer table creation."""
    customer_loader.create_customers_table()
    mock_conn.execute.assert_called_once()
    assert "CREATE TABLE" in mock_conn.execute.call_args[0][0]

def test_deposit_loader_create_table(deposit_loader, mock_conn):
    """Test deposit table creation."""
    deposit_loader.create_deposits_table()
    mock_conn.execute.assert_called_once()
    assert "CREATE TABLE" in mock_conn.execute.call_args[0][0]

def test_trade_loader_create_table(trade_loader, mock_conn):
    """Test trade table creation."""
    trade_loader.create_trades_table()
    mock_conn.execute.assert_called_once()
    assert "CREATE TABLE" in mock_conn.execute.call_args[0][0]

def test_asset_loader_create_table(asset_loader, mock_conn):
    """Test asset table creation."""
    asset_loader.create_assets_table()
    mock_conn.execute.assert_called_once()
    assert "CREATE TABLE" in mock_conn.execute.call_args[0][0]

def test_customer_loader_insert(customer_loader, mock_conn):
    """Test customer data insertion."""
    test_customer = {
        'client_id': 'test1',
        'register_time': '2023-01-01T00:00:00Z',
        'status': 'active'
    }
    customer_loader.load_customers([test_customer])
    mock_conn.execute.assert_called_once()
    assert "INSERT INTO" in mock_conn.execute.call_args[0][0]

def test_deposit_loader_insert(deposit_loader, mock_conn):
    """Test deposit data insertion."""
    test_deposit = {
        'deposit_id': 'dep1',
        'amount': '1000.0',
        'coin': 'USDT',
        'status': 'completed'
    }
    deposit_loader.load_deposits([test_deposit])
    mock_conn.execute.assert_called_once()
    assert "INSERT INTO" in mock_conn.execute.call_args[0][0]

def test_trade_loader_insert(trade_loader, mock_conn):
    """Test trade data insertion."""
    test_trade = {
        'trade_id': 'trade1',
        'symbol': 'BTC/USDT',
        'volume': '1.0',
        'type': 'buy'
    }
    trade_loader.load_trades([test_trade])
    mock_conn.execute.assert_called_once()
    assert "INSERT INTO" in mock_conn.execute.call_args[0][0]

def test_asset_loader_insert(asset_loader, mock_conn):
    """Test asset data insertion."""
    test_asset = {
        'asset_id': 'asset1',
        'symbol': 'BTC',
        'balance': '1.5',
        'status': 'active'
    }
    asset_loader.load_assets([test_asset])
    mock_conn.execute.assert_called_once()
    assert "INSERT INTO" in mock_conn.execute.call_args[0][0]

def test_affiliate_transformer(mock_conn):
    """Test affiliate transformation."""
    transformer = AffiliateTransformer(mock_conn)
    transformer.create_affiliate_table()
    transformer.transform_affiliates()
    assert mock_conn.execute.call_count >= 2

def test_customer_transformer(mock_conn):
    """Test customer transformation."""
    transformer = CustomerTransformer(mock_conn)
    transformer.create_customer_table()
    transformer.transform_customers()
    assert mock_conn.execute.call_count >= 2

def test_deposit_transformer(mock_conn):
    """Test deposit transformation."""
    transformer = DepositTransformer(mock_conn)
    transformer.create_deposit_table()
    transformer.transform_deposits()
    assert mock_conn.execute.call_count >= 2

def test_trade_transformer(mock_conn):
    """Test trade transformation."""
    transformer = TradeTransformer(mock_conn)
    transformer.create_trade_table()
    transformer.transform_trades()
    assert mock_conn.execute.call_count >= 2

def test_gold_view_manager(mock_conn):
    """Test gold view creation."""
    manager = GoldViewManager(mock_conn)
    manager.create_all_views()
    assert mock_conn.execute.call_count >= 6  # One call per view

def test_database_error_handling(mock_conn):
    """Test database error handling."""
    mock_conn.execute.side_effect = Exception('Database error')
    loader = CustomerLoader(mock_conn)
    with pytest.raises(DatabaseError):
        loader.create_customers_table()

def test_connection_pooling():
    """Test database connection pooling."""
    with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
        mock_pool.return_value.getconn.return_value = MagicMock()
        conn = get_db_connection()
        assert conn is not None
        mock_pool.return_value.putconn.assert_not_called()  # Connection not returned yet

def test_connection_cleanup():
    """Test proper connection cleanup."""
    with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
        mock_conn = MagicMock()
        mock_pool.return_value.getconn.return_value = mock_conn
        
        with get_db_connection() as conn:
            pass
            
        mock_pool.return_value.putconn.assert_called_once_with(mock_conn) 