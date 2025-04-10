"""
Error handling module for the Vini Data API
"""
import logging
import traceback
from fastapi import Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

class APIErrorHandler:
    """
    Global error handler for standardized API responses
    """
    
    ERROR_CODES = {
        "VITI_001": "Erro de validação de dados",
        "VITI_002": "Recurso não encontrado",
        "VITI_003": "Timeout na conexão com a fonte de dados",
        "VITI_004": "Erro na estrutura dos dados fonte",
        "VITI_005": "Erro de autenticação",
        "VITI_500": "Erro interno do servidor"
    }
    
    async def handle_exception(self, request: Request, exc: Exception):
        """
        Handle exceptions and return standardized error responses
        """
        error_code = "VITI_500"
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        message = str(exc) or self.ERROR_CODES[error_code]
        resolution = "Contate o suporte se o problema persistir"
        
        # Log the exception with stack trace
        logger.error(f"Exception processing request: {request.url}")
        logger.error(traceback.format_exc())
        
        # Map common exceptions to appropriate error codes
        if hasattr(exc, 'status_code'):
            status_code = exc.status_code
            
            if status_code == 404:
                error_code = "VITI_002"
                message = self.ERROR_CODES[error_code]
                resolution = "Verifique o caminho e parâmetros da requisição"
            elif status_code == 401 or status_code == 403:
                error_code = "VITI_005"
                message = self.ERROR_CODES[error_code]
                resolution = "Verifique suas credenciais de autenticação"
        
        # Record the error in Prometheus metrics
        from main import ERROR_COUNTER
        ERROR_COUNTER.labels(error_code=error_code).inc()
                
        return JSONResponse(
            status_code=status_code,
            content={
                "error": error_code,
                "message": message,
                "resolution": resolution
            }
        )
