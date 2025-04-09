import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import random
from typing import Dict, Any, List

def generate_customer_list_data(count: int = 10) -> Dict[str, Any]:
    """Generate sample customer list data."""
    data = []
    for _ in range(count):
        data.append({
            "uid": str(random.randint(100000000, 999999999)),
            "registerTime": str(int((datetime.now() - timedelta(days=random.randint(0, 365))).timestamp() * 1000))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_deposits_data(count: int = 10) -> Dict[str, Any]:
    """Generate sample deposits data."""
    data = []
    for _ in range(count):
        data.append({
            "orderId": str(random.randint(100000000, 999999999)),
            "uid": str(random.randint(100000000, 999999999)),
            "depositTime": str(int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000)),
            "depositCoin": random.choice(["USDT", "BTC", "ETH"]),
            "depositAmount": str(round(random.uniform(100, 10000), 2))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_trade_volume_data(count: int = 10) -> Dict[str, Any]:
    """Generate sample trade volume data."""
    data = []
    for _ in range(count):
        data.append({
            "uid": str(random.randint(100000000, 999999999)),
            "volumn": str(round(random.uniform(1000, 100000), 2)),
            "time": str(int((datetime.now() - timedelta(days=random.randint(0, 7))).timestamp() * 1000))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_assets_data(count: int = 10) -> Dict[str, Any]:
    """Generate sample assets data."""
    data = []
    for _ in range(count):
        data.append({
            "balance": str(round(random.uniform(1000, 100000), 2)),
            "uid": str(random.randint(100000000, 999999999)),
            "uTime": str(int((datetime.now() - timedelta(days=random.randint(0, 1))).timestamp() * 1000)),
            "remark": random.choice(["sub account exceed 5", "main account", "sub account 1"])
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_sample_data(base_path: str = "data/bronze", num_affiliates: int = 10, days: int = 7):
    """Generate sample data for all affiliates and endpoints."""
    base_path = Path(base_path)
    
    # Generate data for each affiliate
    for affiliate_id in range(1, num_affiliates + 1):
        affiliate_dir = base_path / f"affiliate{affiliate_id}"
        
        # Generate data for each day
        for day in range(days):
            current_date = datetime.now() - timedelta(days=day)
            date_path = current_date.strftime("%Y/%m/%d")
            
            # Generate and save customer list data
            customer_list_path = affiliate_dir / "customer_list" / date_path
            customer_list_path.mkdir(parents=True, exist_ok=True)
            with open(customer_list_path / "page_1.json", "w") as f:
                json.dump(generate_customer_list_data(), f, indent=2)
            
            # Generate and save deposits data
            deposits_path = affiliate_dir / "deposits" / date_path
            deposits_path.mkdir(parents=True, exist_ok=True)
            with open(deposits_path / "page_1.json", "w") as f:
                json.dump(generate_deposits_data(), f, indent=2)
            
            # Generate and save trade activities data
            trade_activities_path = affiliate_dir / "trade_activities" / date_path
            trade_activities_path.mkdir(parents=True, exist_ok=True)
            with open(trade_activities_path / "page_1.json", "w") as f:
                json.dump(generate_trade_volume_data(), f, indent=2)
            
            # Generate and save assets data
            assets_path = affiliate_dir / "assets" / date_path
            assets_path.mkdir(parents=True, exist_ok=True)
            with open(assets_path / "page_1.json", "w") as f:
                json.dump(generate_assets_data(), f, indent=2)

if __name__ == "__main__":
    generate_sample_data() 