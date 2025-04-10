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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crypto_data_platform',
    'user': 'postgres',
    'password': 'postgres'
}

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

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

@app.get("/api/etl-status")
async def get_etl_status():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        affiliate_name as data_source,
                        last_updated as last_load_time,
                        'SUCCESS' as etl_status,
                        total_customers as total_records,
                        new_signups_30d as success_count,
                        0 as error_count,
                        0 as partial_count
                    FROM gold_etl_affiliate_dashboard
                    ORDER BY affiliate_name
                """)
                return cur.fetchall()
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
                    FROM gold_etl_affiliate_dashboard
                    WHERE affiliate_name IS NOT NULL
                    ORDER BY trading_volume_30d DESC
                    LIMIT %s
                """, (limit,))
                return cur.fetchall()
    except Exception as e:
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
                        0 as error_count,
                        0 as partial_count
                    FROM gold_etl_affiliate_dashboard
                    WHERE affiliate_name IS NOT NULL
                """)
                return cur.fetchall()
    except Exception as e:
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
                    FROM gold_etl_affiliate_dashboard
                    WHERE affiliate_name IS NOT NULL
                    ORDER BY trading_volume_30d DESC
                """)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def read_root():
    return FileResponse(os.path.join(static_dir, 'index.html')) 