import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import random
from typing import Dict, Any, List

def generate_customer_list_data(affiliate_id: int, count: int = None) -> Dict[str, Any]:
    """Generate sample customer list data with varying sizes based on affiliate."""
    # Each affiliate gets a different base number of customers
    base_customers = random.randint(50, 500)
    # Daily variation in new customers (some affiliates grow faster)
    daily_growth = random.randint(1, 20) if random.random() < 0.7 else 0
    
    actual_count = count if count else base_customers + daily_growth
    data = []
    customer_uids = [str(random.randint(100000000, 999999999)) for _ in range(actual_count)]
    
    # Different affiliates have different customer acquisition patterns
    if affiliate_id % 3 == 0:  # Newer affiliates
        max_days = 30
    elif affiliate_id % 3 == 1:  # Medium-term affiliates
        max_days = 180
    else:  # Older affiliates
        max_days = 365
    
    for uid in customer_uids:
        data.append({
            "uid": uid,
            "registerTime": str(int((datetime.now() - timedelta(days=random.randint(0, max_days))).timestamp() * 1000))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_deposits_data(affiliate_id: int, customer_uids: List[str], count: int = None) -> Dict[str, Any]:
    """Generate sample deposits data with varying amounts based on affiliate."""
    # Different affiliates have different deposit patterns
    if affiliate_id % 4 == 0:  # High-roller affiliates
        min_deposit = 1000
        max_deposit = 50000
    elif affiliate_id % 4 == 1:  # Mid-tier affiliates
        min_deposit = 500
        max_deposit = 10000
    else:  # Regular affiliates
        min_deposit = 100
        max_deposit = 5000
    
    actual_count = count if count else random.randint(len(customer_uids) // 4, len(customer_uids))
    data = []
    
    for _ in range(actual_count):
        uid = random.choice(customer_uids)
        data.append({
            "orderId": str(random.randint(100000000, 999999999)),
            "uid": uid,
            "depositTime": str(int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000)),
            "depositCoin": random.choice(["USDT", "BTC", "ETH"]),
            "depositAmount": str(round(random.uniform(min_deposit, max_deposit), 2))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_trade_volume_data(affiliate_id: int, customer_uids: List[str], count: int = None) -> Dict[str, Any]:
    """Generate sample trade volume data with varying patterns based on affiliate."""
    # Different affiliates have different trading patterns
    if affiliate_id % 3 == 0:  # High-volume affiliates
        min_volume = 5000
        max_volume = 500000
        activity_rate = 0.8
    elif affiliate_id % 3 == 1:  # Medium-volume affiliates
        min_volume = 2000
        max_volume = 100000
        activity_rate = 0.5
    else:  # Low-volume affiliates
        min_volume = 1000
        max_volume = 50000
        activity_rate = 0.3
    
    # Number of active traders
    active_traders = int(len(customer_uids) * activity_rate)
    actual_count = count if count else random.randint(active_traders, active_traders * 3)
    data = []
    
    for _ in range(actual_count):
        # More active customers trade more frequently
        uid = random.choice(customer_uids[:active_traders]) if random.random() < 0.7 else random.choice(customer_uids)
        # Some days have higher trading volume
        volume_multiplier = 2 if random.random() < 0.2 else 1
        data.append({
            "uid": uid,
            "volumn": str(round(random.uniform(min_volume, max_volume) * volume_multiplier, 2)),
            "time": str(int((datetime.now() - timedelta(days=random.randint(0, 7))).timestamp() * 1000))
        })
    
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": str(int(datetime.now().timestamp() * 1000)),
        "data": data
    }

def generate_assets_data(affiliate_id: int, customer_uids: List[str], count: int = None) -> Dict[str, Any]:
    """Generate sample assets data with varying balances based on affiliate."""
    # Different affiliates have different asset patterns
    if affiliate_id % 3 == 0:  # High-balance affiliates
        min_balance = 5000
        max_balance = 1000000
    elif affiliate_id % 3 == 1:  # Medium-balance affiliates
        min_balance = 2000
        max_balance = 200000
    else:  # Low-balance affiliates
        min_balance = 1000
        max_balance = 50000
    
    actual_count = count if count else len(customer_uids)
    data = []
    
    for _ in range(actual_count):
        uid = random.choice(customer_uids)
        data.append({
            "balance": str(round(random.uniform(min_balance, max_balance), 2)),
            "uid": uid,
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
            
            # First generate customer list data and get the UIDs
            customer_list_path = affiliate_dir / "customer_list" / date_path
            customer_list_path.mkdir(parents=True, exist_ok=True)
            customer_data = generate_customer_list_data(affiliate_id)
            with open(customer_list_path / "page_1.json", "w") as f:
                json.dump(customer_data, f, indent=2)
            
            # Extract customer UIDs for use in other data types
            customer_uids = [customer["uid"] for customer in customer_data["data"]]
            
            # Generate and save deposits data using the same customer UIDs
            deposits_path = affiliate_dir / "deposits" / date_path
            deposits_path.mkdir(parents=True, exist_ok=True)
            with open(deposits_path / "page_1.json", "w") as f:
                json.dump(generate_deposits_data(affiliate_id, customer_uids), f, indent=2)
            
            # Generate and save trade activities data using the same customer UIDs
            trade_activities_path = affiliate_dir / "trade_activities" / date_path
            trade_activities_path.mkdir(parents=True, exist_ok=True)
            with open(trade_activities_path / "page_1.json", "w") as f:
                json.dump(generate_trade_volume_data(affiliate_id, customer_uids), f, indent=2)
            
            # Generate and save assets data using the same customer UIDs
            assets_path = affiliate_dir / "assets" / date_path
            assets_path.mkdir(parents=True, exist_ok=True)
            with open(assets_path / "page_1.json", "w") as f:
                json.dump(generate_assets_data(affiliate_id, customer_uids), f, indent=2)

if __name__ == "__main__":
    generate_sample_data() 