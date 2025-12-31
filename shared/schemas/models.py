from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class StandardMetric(BaseModel):
    ticker: str
    date: str  # ISO format
    value: float