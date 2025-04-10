# -*- coding: utf-8 -*-
"""
Main application file for Vini Data API - Embrapa Vitivinicultura
"""
import logging
import time
from fastapi import FastAPI, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
import prometheus_client

from app.api.v1.api import api_router
from app.core.config import settings
from app.core.errors import APIErrorHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Create a new registry instead of using the default one
# This ensures we don't have duplicate metrics
CUSTOM_REGISTRY = prometheus_client.CollectorRegistry(auto_describe=True)

# Define Prometheus metrics with our custom registry
REQUEST_COUNTER = Counter('api_requests', 'Contagem de requisições', ['endpoint', 'method'], registry=CUSTOM_REGISTRY)
REQUEST_LATENCY = Histogram('api_request_latency_seconds', 'Latência das requisições em segundos', ['endpoint'], registry=CUSTOM_REGISTRY)
ERROR_COUNTER = Counter('api_errors', 'Erros por tipo', ['error_code'], registry=CUSTOM_REGISTRY)

app = FastAPI(
    title="Vini Data API",
    description="API para dados vitivinícolas da Embrapa",
    version="1.0.0",
)

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add custom middleware for metrics tracking
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    endpoint = request.url.path
    method = request.method
    
    # Increment request counter
    REQUEST_COUNTER.labels(endpoint=endpoint, method=method).inc()
    
    try:
        response = await call_next(request)
        
        # Record request duration
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_time)
        
        return response
    except Exception as exc:
        # Track errors by type
        error_type = type(exc).__name__
        ERROR_COUNTER.labels(error_code=error_type).inc()
        raise

# Expose Prometheus metrics endpoint
@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(content=generate_latest(CUSTOM_REGISTRY), media_type=CONTENT_TYPE_LATEST)

# Include API router
app.include_router(api_router, prefix="/api")

# Custom OpenAPI schema
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Vini Data API - Embrapa Vitivinicultura",
        version="1.0.0",
        description="API para acesso estruturado a dados vitivinícolas do portal VitiBrasil",
        routes=app.routes,
    )
    
    # Add customizations to the OpenAPI schema here
    if "components" not in openapi_schema:
        openapi_schema["components"] = {}
    
    # Define common response patterns
    openapi_schema["components"]["responses"] = {
        "Standard200": {
            "description": "OK - Dados encontrados"
        },
        "Standard206": {
            "description": "Partial Content - Dados parciais (timeout)"
        },
        "Standard429": {
            "description": "Too Many Requests"
        },
        "Standard503": {
            "description": "Service Unavailable - Fonte de dados offline"
        },
        "ErrorResponse": {
            "description": "Resposta de erro padrão",
            "content": {
                "application/json": {
                    "example": {
                        "error": "VITI_003",
                        "message": "Timeout na conexão com a fonte de dados",
                        "resolution": "Tente novamente com intervalo de datas menor"
                    }
                }
            }
        }
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    import uvicorn
    # Alterando de 0.0.0.0 para 127.0.0.1 (localhost)
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

