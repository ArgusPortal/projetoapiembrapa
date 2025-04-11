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
async def get_processamento(
    start_year: int = Query(1970, description="Ano inicial", ge=1970, le=2025),
    end_year: int = Query(2023, description="Ano final", ge=1970, le=2025),
    subcategoria: Optional[str] = Query(None, description="Subcategoria de processamento", 
                                        enum=["viniferas", "americanas", "mesa", "semclassificacao"]),
    tipo_uva: Optional[str] = Query(None, description="Filtrar por tipo específico de uva (ex: Cabernet, Isabel, etc)"),
    regiao: Optional[str] = Query(None, description="Filtrar por região geográfica"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados sobre processamento industrial de uvas e vinhos
    
    - **viniferas**: Processamento de uvas viníferas (ex: Cabernet Sauvignon, Merlot, Chardonnay)
    - **americanas**: Processamento de uvas americanas e híbridas (ex: Isabel, Bordô, Concord)
    - **mesa**: Processamento de uvas de mesa (ex: Itália, Niágara, Rubi)
    - **semclassificacao**: Processamento de uvas sem classificação específica
    """
    try:
        # Passamos a subcategoria diretamente, incluindo "semclassificacao"
        # para que o serviço de dados possa utilizar o arquivo de fallback correto
        
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="processamento",
            start_year=start_year,
            end_year=end_year,
            subcategory=subcategoria,  # Passamos a subcategoria diretamente
            region=regiao,
            product_type=tipo_uva,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Definição das palavras-chave para identificação de subcategorias
            cultivar_mapping = {
                "viniferas": ["cabernet", "merlot", "chardonnay", "tannat", "pinot", "sauvignon", "syrah", "viognier", "malbec"],
                "americanas": ["isabel", "bordô", "bordo", "niágara", "niagara", "concord", "jacquez", "herbemont", "seibel"],
                "mesa": ["italia", "itália", "rubi", "benitaka", "red globe", "thompson"]
            }
            
            # Aplicamos a classificação apenas se não tivermos uma subcategoria específica
            # ou se precisamos adicionar metadados de subcategoria aos resultados
            if not subcategoria or subcategoria != "semclassificacao":
                for item in result["data"]:
                    # Se não tem subcategoria, vamos identificar
                    if "subcategoria" not in item:
                        # Verifica cultivar para identificar a subcategoria
                        cultivar = item.get("Cultivar", "").lower() if "Cultivar" in item else ""
                        is_classified = False
                        
                        if cultivar:
                            for subcategoria_nome, palavras_chave in cultivar_mapping.items():
                                if any(palavra in cultivar for palavra in palavras_chave):
                                    item["subcategoria"] = subcategoria_nome
                                    is_classified = True
                                    break
                        
                        # Se não identificou por palavras chave, coloca como sem classificação
                        if not is_classified:
                            item["subcategoria"] = "semclassificacao"
                    
                    # Se estamos filtrando por uma subcategoria específica diferente de semclassificacao
                    if subcategoria and subcategoria != "semclassificacao":
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
                headers={"Content-Disposition": "attachment; filename=processamento.parquet"}
            )
            
        # Default: return JSON
        return result
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados de processamento: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "VITI_003",
                "message": "Erro ao processar dados de processamento industrial",
                "resolution": "Tente novamente com intervalo de datas menor"
            }
        )