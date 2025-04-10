"""
Script to reset the database and run setup.
"""
import logging
from psycopg2.extensions import connection
from src.utils.db import execute_query, DatabaseError, get_db_connection
from src.utils.db_setup import create_database, create_all_tables

logger = logging.getLogger(__name__)

def clear_database(conn: connection) -> None:
    """Clear all tables in the database."""
    try:
        # Drop all tables
        execute_query(conn, """
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        logger.info("All tables dropped successfully")
    except DatabaseError as e:
        logger.error(f"Failed to clear database: {e}")
        raise

def reset_database() -> None:
    """Reset the database by clearing it and running setup."""
    with get_db_connection() as conn:
        try:
            # Clear the database
            clear_database(conn)
            
            # Create database and tables
            create_database(conn)
            create_all_tables(conn)
            
            logger.info("Database reset and setup completed successfully")
            
        except Exception as e:
            logger.error(f"Database reset failed: {e}")
            raise

if __name__ == "__main__":
    reset_database() 