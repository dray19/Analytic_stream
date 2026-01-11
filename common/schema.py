from pydantic import BaseModel, Field,ConfigDict, field_validator
from typing import List,Optional, Literal
from datetime import datetime

class HourData(BaseModel):
    hour: int
    lmp: float
    mcc: float
    mlc: float

class PNodeData(BaseModel):
    pnodeName: str
    hours: List[HourData]

class MarketEvent(BaseModel):
    day: str
    priceType: str
    timestamp: datetime
    pnodes: List[PNodeData]



class FlattenedMarketEvent(BaseModel):
    """
    Schema for the flattened Kafka stream topic: market-events-agg

    Expected message format:

    {
      "day": "2026-01-02",
      "priceType": "ExPost",
      "timestamp": "2026-01-02 00:00:00",
      "hour": 12,
      "lmp": 41.22,
      "mcc": 0.14,
      "mlc": 1.18,
      "pnodeName": "MEC.MECB"
    }
    """

    model_config = ConfigDict(extra="forbid")

    day: str = Field(..., description="Market day YYYY-MM-DD")
    priceType: str = Field(..., description="Price type e.g. ExPost, DayAhead")
    timestamp: str = Field(..., description="Timestamp string (as emitted by producer)")
    hour: int = Field(..., ge=0, le=23, description="Hour ending / hour index 0-23")

    pnodeName: str = Field(..., min_length=1, description="Pricing node name")

    lmp: Optional[float] = Field(None, description="Locational Marginal Price")
    mcc: Optional[float] = Field(None, description="Marginal Congestion Component")
    mlc: Optional[float] = Field(None, description="Marginal Loss Component")

    @field_validator("lmp", "mcc", "mlc", mode="before")
    @classmethod
    def _cast_numeric(cls, v):
        if v is None or v == "":
            return None
        return float(v)

    @field_validator("hour", mode="before")
    @classmethod
    def _cast_hour(cls, v):
        if isinstance(v, str) and v.strip() != "":
            return int(float(v))
        return v