import os
import secrets
from typing import List, Dict, Any, Optional, Union

from pydantic import BaseModel


class Settings(BaseModel):
    """API Configuration Settings"""
    
    # API Info
    API_V1_STR: str = "/v1"
    PROJECT_NAME: str = "Vini Data API"
    
    # Security
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days
    
    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost",
        "http://localhost:8080",
        "http://localhost:3000",
        "https://api.vinidata.embrapa",
    ]
    
    # Data Sources
    VITIBRASIL_BASE_URL: str = "http://vitibrasil.cnpuv.embrapa.br/index.php"
    
    # Cache Configuration
    CACHE_TTL: int = 3600 * 24  # 24 hours
    CACHE_MAX_SIZE: int = 100  # Number of items in LRU cache
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./vinidata.db")
    
    # External Services
    NOTIFICATION_WEBHOOK_URL: Optional[str] = os.getenv("NOTIFICATION_WEBHOOK_URL")
    
    # Monitoring
    ENABLE_METRICS: bool = True


# Create global settings instance
settings = Settings()