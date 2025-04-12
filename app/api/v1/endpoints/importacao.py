import logging
from typing import Optional, List
from fastapi import APIRouter, Query, Depends, HTTPException, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_429_TOO_MANY_REQUESTS
import pandas as pd
import json
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq

from app.services.data_service import vini_data_service
from app.schemas.data import DataResponse, ErrorResponse, DataFilter

router = APIRouter()
security = HTTPBearer()
logger = logging.getLogger(__name__)

# JWT validation function (simplified for example)
async def has_access(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # In a real application, we would validate JWT here
    # For now, we'll just check if a token is provided
    if not credentials:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Não autorizado"
        )
    return True


@router.get(
    "/",
    response_model=DataResponse,
    responses={
        404: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def get_importacao(
    start_year: int = Query(1970, description="Ano inicial", ge=1970, le=2025),
    end_year: int = Query(2023, description="Ano final", ge=1970, le=2025),
    subcategoria: str = Query(..., description="Subcategoria de importação (obrigatória)", 
                             enum=["vinhos", "espumantes", "sucos", "passas", "frescas"]),
    origem: Optional[str] = Query(None, description="País ou região de origem da importação"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados de importações de vinhos e derivados
    
    - **vinhos**: Importação de vinhos de mesa, finos e de mesa
    - **espumantes**: Importação de vinhos espumantes e frisantes
    - **sucos**: Importação de sucos de uva e derivados
    - **passas**: Importação de uvas passa
    - **frescas**: Importação de uvas frescas
    """
    try:
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="importacao",
            start_year=start_year,
            end_year=end_year,
            subcategory=subcategoria,
            product_type=None,  # Removido filtro por produto
            origin=origem,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Adicione a subcategoria em cada registro usando o valor fornecido
            for item in result["data"]:
                item["subcategoria"] = subcategoria
        
        # Handle different output formats
        if format == "csv":
            if not result.get("data"):
                return Response(content="", media_type="text/csv")
                
            df = pd.DataFrame(result["data"])
            csv_data = df.to_csv(index=False)
            return Response(content=csv_data, media_type="text/csv")
            
        elif format == "parquet":
            if not result.get("data"):
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND,
                    detail="Nenhum dado encontrado para os critérios especificados"
                )
                
            df = pd.DataFrame(result["data"])
            table = pa.Table.from_pandas(df)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink)
            return Response(
                content=sink.getvalue().to_pybytes(),
                media_type="application/octet-stream",
                headers={"Content-Disposition": "attachment; filename=importacao.parquet"}
            )
            
        # Default: return JSON
        return result
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados de importação: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "VITI_003",
                "message": "Erro ao processar dados de importação",
                "resolution": "Tente novamente com intervalo de datas menor"
            }
        )