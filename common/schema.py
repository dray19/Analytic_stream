from pydantic import BaseModel, Field
from typing import List
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