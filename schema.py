from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field


class DataSourceConfig(BaseModel):
    source_name: str
    source_type: str  # 'api', 'csv', 'database'
    config: Dict[str, Any]
    schedule: Optional[str] = None  # cron expression for scheduling


class StockPrice(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str


class ForexRate(BaseModel):
    from_currency: str
    to_currency: str
    timestamp: datetime
    rate: float
    source: str


class CryptoPrice(BaseModel):
    symbol: str
    timestamp: datetime
    price: float
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    source: str


class EconomicIndicator(BaseModel):
    indicator: str
    timestamp: datetime
    value: float
    country: str
    source: str


class DataIngestionRequest(BaseModel):
    source: str
    data_type: str  # 'stock', 'forex', 'crypto', 'economic'
    symbols: List[str] = []
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    params: Dict[str, Any] = {}


class DataIngestionResponse(BaseModel):
    request_id: str
    status: str  # 'success', 'error', 'pending'
    message: str
    records_count: Optional[int] = None
    errors: List[str] = []


class TransformationRequest(BaseModel):
    source_path: str
    destination_path: str
    transformation_type: str  # 'clean', 'normalize', 'aggregate'
    params: Dict[str, Any] = {}


class QueryRequest(BaseModel):
    dataset: str
    query_type: str  # 'timeseries', 'correlation', 'moving_average', etc.
    params: Dict[str, Any] = {}


class DatasetInfo(BaseModel):
    name: str
    data_type: str
    source: str
    record_count: int
    first_date: datetime
    last_date: datetime
    symbols: List[str]
    layer: str  # 'bronze', 'silver', 'gold'
    file_path: str
    created_at: datetime
    updated_at: datetime