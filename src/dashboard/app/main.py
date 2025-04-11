from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List, Dict, Any
import json
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Crypto Data Platform Dashboard")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(os.path.dirname(current_dir), 'static')
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "crypto_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/api/etl-status")
async def get_etl_status():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        affiliate_name as data_source,
                        last_updated as last_load_time,
                        etl_status,
                        total_records,
                        success_count,
                        error_count,
                        partial_count
                    FROM gold_etl_affiliate_dashboard
                    ORDER BY affiliate_name
                """)
                result = cur.fetchall()
                logger.info(f"ETL Status data: {result}")
                return result
    except Exception as e:
        logger.error(f"Error in get_etl_status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/top-affiliates")
async def get_top_affiliates(limit: int = 10):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT
                        affiliate_name,
                        new_signups_30d,
                        trading_volume_30d,
                        monthly_activation_rate,
                        avg_trade_size
                    FROM gold_affiliate_performance
                    WHERE affiliate_name IS NOT NULL
                    ORDER BY trading_volume_30d DESC
                    LIMIT %s
                """, (limit,))
                result = cur.fetchall()
                logger.info(f"Top Affiliates data: {result}")
                return result
    except Exception as e:
        logger.error(f"Error in get_top_affiliates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl-issues")
async def get_etl_issues():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        affiliate_name as data_source,
                        last_updated as last_load_time,
                        error_count,
                        partial_count
                    FROM gold_etl_affiliate_dashboard
                    WHERE affiliate_name IS NOT NULL
                """)
                result = cur.fetchall()
                logger.info(f"ETL Issues data: {result}")
                return result
    except Exception as e:
        logger.error(f"Error in get_etl_issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/affiliate-metrics")
async def get_affiliate_metrics():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT
                        affiliate_name,
                        total_customers,
                        new_signups_30d,
                        active_customers_30d,
                        trading_volume_30d,
                        monthly_activation_rate,
                        avg_trade_size
                    FROM gold_affiliate_performance
                    WHERE affiliate_name IS NOT NULL
                    ORDER BY trading_volume_30d DESC
                """)
                result = cur.fetchall()
                logger.info(f"Affiliate Metrics data: {result}")
                return result
    except Exception as e:
        logger.error(f"Error in get_affiliate_metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def read_root():
    return FileResponse(os.path.join(static_dir, 'index.html')) 