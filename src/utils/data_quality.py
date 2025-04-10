"""
Data quality utilities for the ETL pipeline.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime
import json

import pandas as pd
from psycopg2.extensions import connection

from src.config.settings import DATA_QUALITY_CONFIG
from src.utils.db import execute_query, DatabaseError

logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Custom exception for data quality issues."""
    pass

def check_null_percentage(df: pd.DataFrame, column: str) -> float:
    """
    Calculate the percentage of null values in a column.
    """
    null_count = df[column].isnull().sum()
    total_count = len(df)
    return (null_count / total_count) * 100 if total_count > 0 else 0

def check_duplicate_percentage(df: pd.DataFrame, columns: List[str]) -> float:
    """
    Calculate the percentage of duplicate records based on specified columns.
    """
    duplicate_count = df.duplicated(subset=columns).sum()
    total_count = len(df)
    return (duplicate_count / total_count) * 100 if total_count > 0 else 0

def validate_data_quality(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """
    Perform data quality checks on a DataFrame.
    Returns a dictionary with quality metrics and validation results.
    """
    quality_metrics = {
        'table_name': table_name,
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'null_percentages': {},
        'duplicate_percentage': 0,
        'validation_passed': True,
        'issues': []
    }

    # Check null percentages for each column
    for column in df.columns:
        null_pct = check_null_percentage(df, column)
        quality_metrics['null_percentages'][column] = null_pct
        
        if null_pct > DATA_QUALITY_CONFIG['max_null_percentage']:
            quality_metrics['validation_passed'] = False
            quality_metrics['issues'].append(
                f"Column '{column}' has {null_pct:.2f}% null values (threshold: {DATA_QUALITY_CONFIG['max_null_percentage']}%)"
            )

    # Check duplicates
    duplicate_pct = check_duplicate_percentage(df, df.columns.tolist())
    quality_metrics['duplicate_percentage'] = duplicate_pct
    
    if duplicate_pct > DATA_QUALITY_CONFIG['max_duplicate_percentage']:
        quality_metrics['validation_passed'] = False
        quality_metrics['issues'].append(
            f"Table has {duplicate_pct:.2f}% duplicate records (threshold: {DATA_QUALITY_CONFIG['max_duplicate_percentage']}%)"
        )

    return quality_metrics

def log_data_quality_metrics(conn: connection, metrics: Dict[str, Any]) -> None:
    """
    Log data quality metrics to the database.
    """
    try:
        # Convert numpy types to Python native types
        metrics = {
            'table_name': str(metrics['table_name']),
            'timestamp': str(metrics['timestamp']),
            'total_records': int(metrics['total_records']),
            'null_percentages': json.dumps(metrics['null_percentages']),  # Convert dict to JSON string
            'duplicate_percentage': float(metrics['duplicate_percentage']),
            'validation_passed': bool(metrics['validation_passed']),
            'issues': metrics['issues']
        }

        query = """
            INSERT INTO etl_data_quality_log (
                table_name,
                check_timestamp,
                total_records,
                null_percentages,
                duplicate_percentage,
                validation_passed,
                issues
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            metrics['table_name'],
            metrics['timestamp'],
            metrics['total_records'],
            metrics['null_percentages'],
            metrics['duplicate_percentage'],
            metrics['validation_passed'],
            metrics['issues']
        )
        execute_query(conn, query, params)
        
    except DatabaseError as e:
        logger.error(f"Failed to log data quality metrics: {e}")
        raise

def create_data_quality_table(conn: connection) -> None:
    """
    Create the data quality logging table if it doesn't exist.
    """
    try:
        query = """
            CREATE TABLE IF NOT EXISTS etl_data_quality_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                check_timestamp TIMESTAMP NOT NULL,
                total_records INTEGER NOT NULL,
                null_percentages JSONB NOT NULL,
                duplicate_percentage FLOAT NOT NULL,
                validation_passed BOOLEAN NOT NULL,
                issues TEXT[]
            )
        """
        execute_query(conn, query)
        
    except DatabaseError as e:
        logger.error(f"Failed to create data quality table: {e}")
        raise 