# -*- coding: utf-8 -*-
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class ScrapedData(BaseModel):
    """Model for scraped data with metadata"""
    source_url: str
    timestamp: float
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    raw_html: Optional[str] = Field(default=None, exclude=True)  # Store raw HTML for recovery but exclude from JSON