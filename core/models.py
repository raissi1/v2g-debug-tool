from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

@dataclass
class Event:
    timestamp: datetime
    source: str
    name: str
    values: Dict[str, Any]