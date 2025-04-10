from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


class DataResponse(BaseModel):
    """Response schema for data endpoints"""
    
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of data records"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadata about the data"
    )
    from_cache: bool = Field(
        default=False,
        description="Whether the data was retrieved from cache"
    )


class ErrorResponse(BaseModel):
    """Error response schema"""
    
    error: str = Field(
        ...,
        description="Error code"
    )
    message: str = Field(
        ...,
        description="Error message"
    )
    resolution: Optional[str] = Field(
        None,
        description="Suggested resolution for the error"
    )


class DataFilter(BaseModel):
    """Filter parameters for data queries"""
    
    start_year: int = Field(
        1970, 
        description="Starting year for data",
        ge=1970,
        le=2023
    )
    end_year: int = Field(
        2023, 
        description="Ending year for data",
        ge=1970,
        le=2023
    )
    region: Optional[str] = Field(
        None, 
        description="Filter by geographical region"
    )
    product: Optional[str] = Field(
        None, 
        description="Filter by product type"
    )