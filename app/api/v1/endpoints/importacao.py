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
    subcategoria: Optional[str] = Query(None, description="Subcategoria de importação", 
                                       enum=["vinhos", "espumantes", "sucos", "passas", "frescas"]),
    produto: Optional[str] = Query(None, description="Tipo específico de produto dentro da subcategoria"),
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
            product_type=produto,
            origin=origem,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Se não foi especificada uma subcategoria, mas temos vários tipos de dados,
            # identifique a subcategoria de cada registro
            if not subcategoria:
                # Tente identificar a subcategoria de cada registro com base no produto
                produto_mapping = {
                    "vinhos": ["vinho", "cabernet", "merlot", "chardonnay", "vinhos de mesa", "vinho fino", "tinto", "branco"],
                    "espumantes": ["espumante", "champagne", "moscatel", "frisante", "prosecco", "brut", "cava"],
                    "sucos": ["suco", "néctar", "bebida", "concentrado", "integral"],
                    "passas": ["passa", "passas", "uva passa", "uva seca", "sultana", "raisins"],
                    "frescas": ["fresca", "frescas", "uva fresca", "mesa", "in natura", "thompson", "crimson"]
                }
                
                for item in result["data"]:
                    # Verifica se já tem subcategoria identificada
                    if "subcategoria" not in item:
                        # Verifica o texto de produto para identificar a subcategoria
                        # Procura em diferentes campos que podem conter informação do produto
                        item_text = ""
                        for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item"]:
                            if campo in item and item[campo]:
                                item_text += str(item[campo]).lower() + " "
                        
                        # Verifica se o texto do item contém palavras-chave das subcategorias
                        for subcategoria_nome, palavras_chave in produto_mapping.items():
                            if any(palavra in item_text for palavra in palavras_chave):
                                item["subcategoria"] = subcategoria_nome
                                break
                        
                        # Se ainda não identificou a subcategoria, verifica se é um país conhecido por exportar um tipo específico
                        if "subcategoria" not in item and ("País" in item or "Pais" in item or "país" in item or "pais" in item):
                            pais = str(item.get("País", item.get("Pais", item.get("país", item.get("pais", ""))))).lower()
                            
                            # Países mais conhecidos por exportar vinhos
                            if pais in ["chile", "argentina", "frança", "franca", "portugal", "espanha", "italia", "itália"]:
                                item["subcategoria"] = "vinhos"
                            # Países mais conhecidos por exportar uvas frescas
                            elif pais in ["chile", "argentina", "estados unidos", "eua"]:
                                item["subcategoria"] = "frescas"
                        
                        # Se mesmo assim não identificou, coloca como a mais comum (vinhos)
                        if "subcategoria" not in item:
                            item["subcategoria"] = "vinhos"
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