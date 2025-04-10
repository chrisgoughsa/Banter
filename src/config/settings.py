"""
Configuration settings for the ETL pipeline.
"""
import os
from pathlib import Path
from typing import Dict, Any

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'crypto_data_platform'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# ETL Configuration
ETL_CONFIG = {
    'batch_size': int(os.getenv('ETL_BATCH_SIZE', 1000)),
    'max_retries': int(os.getenv('ETL_MAX_RETRIES', 3)),
    'retry_delay': int(os.getenv('ETL_RETRY_DELAY', 5)),
}

# Logging Configuration
LOG_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
}

# Data Quality Thresholds
DATA_QUALITY_CONFIG = {
    'max_null_percentage': float(os.getenv('MAX_NULL_PERCENTAGE', 0.1)),
    'max_duplicate_percentage': float(os.getenv('MAX_DUPLICATE_PERCENTAGE', 0.05)),
}

# API Configuration
API_CONFIG = {
    'title': 'Crypto Data Platform Dashboard',
    'cors_origins': os.getenv('CORS_ORIGINS', '*').split(','),
    'api_prefix': '/api/v1',
} 