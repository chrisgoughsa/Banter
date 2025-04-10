"""
Database connection utilities.
"""
import os
import logging
from typing import Optional
from psycopg2 import connect, Error
from psycopg2.extensions import connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database errors."""
    pass

def get_db_connection() -> connection:
    """
    Get a database connection using environment variables.
    
    Returns:
        connection: A PostgreSQL database connection
        
    Raises:
        DatabaseError: If connection fails
    """
    try:
        conn = connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'crypto_data'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres')
        )
        logger.info("Successfully connected to database")
        return conn
    except Error as e:
        logger.error(f"Database connection failed: {e}")
        raise DatabaseError(f"Failed to connect to database: {e}")

def create_db_connection() -> connection:
    """
    Create a persistent database connection.
    
    Returns:
        connection: A PostgreSQL database connection
        
    Raises:
        DatabaseError: If connection fails
    """
    return get_db_connection() 