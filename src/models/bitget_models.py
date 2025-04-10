from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator

class BitgetConfig(BaseModel):
    """Configuration model for Bitget API."""
    base_url: str
    affiliates: List['AffiliateConfig']

class AffiliateConfig(BaseModel):
    """Configuration model for a Bitget affiliate."""
    id: str
    name: str
    api_key: str
    api_secret: str
    api_passphrase: str

class Customer(BaseModel):
    """Model for customer data from Bitget API."""
    uid: str
    registerTime: int
    source_file: Optional[str] = None
    load_time: Optional[datetime] = None

    @validator('registerTime')
    def convert_timestamp(cls, v):
        return datetime.fromtimestamp(v / 1000)

class Trade(BaseModel):
    """Model for trade data from Bitget API."""
    uid: str
    volumn: float  # Note: API uses "volumn" instead of "volume"
    time: int
    source_file: Optional[str] = None
    load_time: Optional[datetime] = None

    @validator('time')
    def convert_timestamp(cls, v):
        return datetime.fromtimestamp(v / 1000)

class Deposit(BaseModel):
    """Model for deposit data from Bitget API."""
    uid: str
    orderId: str
    depositTime: int
    depositCoin: str
    depositAmount: float
    source_file: Optional[str] = None
    load_time: Optional[datetime] = None

    @validator('depositTime')
    def convert_timestamp(cls, v):
        return datetime.fromtimestamp(v / 1000)

class Asset(BaseModel):
    """Model for asset data from Bitget API."""
    uid: str
    balance: float
    uTime: int
    remark: Optional[str] = None
    source_file: Optional[str] = None
    load_time: Optional[datetime] = None

    @validator('uTime')
    def convert_timestamp(cls, v):
        return datetime.fromtimestamp(v / 1000)

class APIResponse(BaseModel):
    """Base model for Bitget API responses."""
    code: str
    msg: str
    data: Optional[List[BaseModel]] = None
    hasMore: Optional[bool] = None

# Update forward references
BitgetConfig.update_forward_refs() 