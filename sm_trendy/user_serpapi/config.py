from typing import Literal, Optional

from pydantic import BaseModel


class SerpAPIConfig(BaseModel):
    api_key: str
    engine: str = "google_trends"
    q: str
    geo: Optional[str] = None
    data_type: Literal[
        "TIMESERIES", "GEO_MAP", "GEO_MAP_0", "RELATED_TOPICS", "RELATED_QUERIES"
    ] = "TIMESERIES"
    tz: Optional[str] = "120"
    cat: Optional[Literal["0"]] = None
    date: str = "today 5-y"
