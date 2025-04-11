"""
Database connection utilities.
"""
import os
import logging
from typing import Optional, Any, List, Tuple, Generator
from psycopg2 import connect, Error
from psycopg2.extensions import connection,cursor
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database errors."""
    pass


# def get_db_connection() -> connection:
#     """
#     Get a database connection using environment variables.
    
#     Returns:
#         connection: A PostgreSQL database connection
        
#     Raises:
#         DatabaseError: If connection fails
#     """
#     try:
#         conn = connect(
#             host=os.getenv('DB_HOST', 'localhost'),
#             port=os.getenv('DB_PORT', '5432'),
#             database=os.getenv('DB_NAME', 'crypto_data'),
#             user=os.getenv('DB_USER', 'postgres'),
#             password=os.getenv('DB_PASSWORD', 'postgres')
#         )
#         logger.info("Successfully connected to database")
#         return conn
#     except Error as e:
#         logger.error(f"Database connection failed: {e}")
#         raise DatabaseError(f"Failed to connect to database: {e}")

def get_db_connection() -> connection:
    """
    Get a database connection using environment variables.
    
    Returns:
        connection: A PostgreSQL database connection
        
    Raises:
        DatabaseError: If connection fails
    """
    try:
        # Check if running on Render
        if os.getenv('RENDER'):
            # Use container-specific PostgreSQL settings
            conn = connect(
                host='localhost',
                port=5432,
                database='crypto_data',
                user='postgres',
                password='postgres',
                # Add these settings for better reliability in container
                connect_timeout=5,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
        else:
            # Use environment variables for local/development
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

# def create_db_connection() -> connection:
#     """
#     Create a database connection.
#     """
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         return conn
#     except Exception as e:
#         logger.error(f"Database connection error: {e}")
#         raise DatabaseError(f"Failed to connect to database: {e}")

# @contextmanager
# def get_db_connection() -> Generator[connection, None, None]:
#     """
#     Context manager for database connections.
#     Ensures connections are properly closed after use.
#     """
#     conn = None
#     try:
#         conn = create_db_connection()
#         yield conn
#     except Exception as e:
#         logger.error(f"Database connection error: {e}")
#         raise DatabaseError(f"Failed to connect to database: {e}")
#     finally:
#         if conn:
#             conn.close()

@contextmanager
def get_db_cursor(conn: connection, cursor_factory: Any = RealDictCursor) -> Generator[cursor, None, None]:
    """
    Context manager for database cursors.
    Ensures cursors are properly closed after use.
    """
    cur = None
    try:
        cur = conn.cursor(cursor_factory=cursor_factory)
        yield cur
    finally:
        if cur:
            cur.close()

def execute_query(conn: connection, query: str, params: tuple = None) -> list:
    """
    Execute a query and return results.
    For queries that don't return results (like CREATE TABLE), returns an empty list.
    """
    with get_db_cursor(conn) as cur:
        try:
            cur.execute(query, params)
            # Only try to fetch results if the query is a SELECT
            if query.strip().upper().startswith('SELECT'):
                # Check if we're using RealDictCursor
                if isinstance(cur, RealDictCursor):
                    return [dict(row) for row in cur.fetchall()]
                else:
                    return cur.fetchall()
            conn.commit()
            return []
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            conn.rollback()
            raise DatabaseError(f"Failed to execute query: {e}")

def execute_batch(conn: connection, query: str, params_list: list) -> None:
    """
    Execute a batch of queries.
    """
    with get_db_cursor(conn) as cur:
        try:
            cur.executemany(query, params_list)
            conn.commit()
        except Exception as e:
            logger.error(f"Batch execution error: {e}")
            conn.rollback()
            raise DatabaseError(f"Failed to execute batch: {e}")

def table_exists(conn: connection, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    """
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """
    with get_db_cursor(conn) as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone()['exists']

def get_table_columns(conn: connection, table_name: str) -> list:
    """
    Get the column names for a table.
    """
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
    """
    with get_db_cursor(conn) as cur:
        cur.execute(query, (table_name,))
        return [row['column_name'] for row in cur.fetchall()] 