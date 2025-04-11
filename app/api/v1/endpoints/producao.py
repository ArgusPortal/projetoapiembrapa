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
async def get_producao(
    start_year: int = Query(1970, description="Ano inicial", ge=1970, le=2025),
    end_year: int = Query(2023, description="Ano final", ge=1970, le=2025),
    subcategoria: Optional[str] = Query(None, description="Subcategoria de produção", 
                                       enum=["uvas", "vinhos", "sucos", "derivados"]),
    produto: Optional[str] = Query(None, description="Filtrar por tipo específico de produto"),
    regiao: Optional[str] = Query(None, description="Região geográfica"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados de produção de uvas, vinhos e sucos
    
    - **uvas**: Produção de uvas para processamento e consumo in natura
    - **vinhos**: Produção de vinhos finos, de mesa e espumantes
    - **sucos**: Produção de sucos integrais e concentrados
    - **derivados**: Produção de outros derivados de uva e vinho
    """
    try:
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="producao",
            start_year=start_year,
            end_year=end_year,
            subcategory=subcategoria,
            region=regiao,
            product_type=produto,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Se não foi especificada uma subcategoria, mas temos vários tipos de dados,
            # identifique a subcategoria de cada registro
            if not subcategoria:
                # Tente identificar a subcategoria de cada registro com base no produto
                produto_mapping = {
                    "uvas": ["uva", "videira", "parreiral", "cultivar", "vitis", "niágara", "itália", "bordô", "cabernet"],
                    "vinhos": ["vinho", "vinificação", "tinto", "branco", "rosé", "rose", "mesa", "fino"],
                    "sucos": ["suco", "mosto", "integral", "concentrado", "bebida", "néctar"],
                    "derivados": ["derivado", "fermentado", "aguardente", "grappa", "bagaceira", "cooler", "filtrado"]
                }
                
                for item in result["data"]:
                    # Verifica se já tem subcategoria identificada
                    if "subcategoria" not in item:
                        # Verifica o texto de produto para identificar a subcategoria
                        # Procura em diferentes campos que podem conter informação do produto
                        item_text = ""
                        for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item", "Item", "Nome"]:
                            if campo in item and item[campo]:
                                item_text += str(item[campo]).lower() + " "
                        
                        # Verifica se o texto do item contém palavras-chave das subcategorias
                        for subcategoria_nome, palavras_chave in produto_mapping.items():
                            if any(palavra in item_text for palavra in palavras_chave):
                                item["subcategoria"] = subcategoria_nome
                                break
                        
                        # Se mesmo assim não identificou, tenta usar a unidade de medida
                        if "subcategoria" not in item and "Unidade" in item:
                            unidade = str(item["Unidade"]).lower()
                            if "kg" in unidade or "ton" in unidade:
                                item["subcategoria"] = "uvas"
                            elif "l" in unidade or "litro" in unidade:
                                item["subcategoria"] = "vinhos"
                        
                        # Se mesmo assim não identificou, coloca como uvas (mais comum)
                        if "subcategoria" not in item:
                            item["subcategoria"] = "uvas"
            else:
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
                headers={"Content-Disposition": "attachment; filename=producao.parquet"}
            )
            
        # Default: return JSON
        return result
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados de produção: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "VITI_003",
                "message": "Erro ao processar dados de produção",
                "resolution": "Tente novamente com intervalo de datas menor"
            }
        )