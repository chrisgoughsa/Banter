from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator, model_validator

class ETLConfig(BaseModel):
    """Configuration model for ETL pipeline."""
    batch_size: int = Field(gt=0, default=1000)
    max_retries: int = Field(ge=0, default=3)
    retry_delay: int = Field(ge=0, default=5)

class ETLStatus(BaseModel):
    """Model for tracking ETL status."""
    data_source: str
    last_load_time: datetime
    last_api_run: datetime
    total_records: int = Field(ge=0)
    success_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    partial_count: int = Field(ge=0)

    @model_validator(mode='after')
    def validate_counts(self) -> 'ETLStatus':
        total = self.total_records
        success = self.success_count
        error = self.error_count
        partial = self.partial_count
        
        if success + error + partial != total:
            raise ValueError("Sum of success, error, and partial counts must equal total records")
        return self

class BronzeRecord(BaseModel):
    """Base model for bronze layer records."""
    source_file: str
    load_time: datetime
    load_status: str = Field(pattern='^(SUCCESS|ERROR|PARTIAL)$')

class CustomerRecord(BronzeRecord):
    """Model for customer data in bronze layer."""
    affiliate_id: str
    client_id: str
    register_time: datetime

class TradeRecord(BronzeRecord):
    """Model for trade data in bronze layer."""
    affiliate_id: str
    client_id: str
    trade_volume: float = Field(ge=0)
    trade_time: datetime

class DepositRecord(BronzeRecord):
    """Model for deposit data in bronze layer."""
    affiliate_id: str
    client_id: str
    order_id: str
    deposit_time: datetime
    deposit_coin: str
    deposit_amount: float = Field(ge=0)

class AssetRecord(BronzeRecord):
    """Model for asset data in bronze layer."""
    affiliate_id: str
    client_id: str
    balance: float = Field(ge=0)
    update_time: datetime
    remark: Optional[str] = None

class APIResponse(BaseModel):
    """Base model for API responses."""
    code: str
    msg: str
    data: Optional[List[Dict[str, Any]]] = None
    hasMore: Optional[bool] = None

    @validator('code')
    def validate_code(cls, v):
        if not v.isdigit():
            raise ValueError("API response code must be numeric")
        return v 