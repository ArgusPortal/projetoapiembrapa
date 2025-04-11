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
async def get_comercializacao(
    start_year: int = Query(1970, description="Ano inicial", ge=1970, le=2025),
    end_year: int = Query(2023, description="Ano final", ge=1970, le=2025),
    canal: Optional[str] = Query(None, description="Canal de comercialização", 
                                enum=["varejo", "grandes_redes", "exportacao_indireta", "venda_direta"]),
    subcategoria: Optional[str] = Query(None, description="Subcategoria de produto", 
                                       enum=["vinhos", "espumantes", "sucos", "uvas"]),
    produto: Optional[str] = Query(None, description="Tipo específico de produto"),
    regiao: Optional[str] = Query(None, description="Região geográfica"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados de comercialização no mercado interno
    
    Canais de comercialização:
    - **varejo**: Comercialização através do varejo tradicional
    - **grandes_redes**: Comercialização através de grandes redes varejistas
    - **exportacao_indireta**: Exportação via intermediários
    - **venda_direta**: Venda direta ao consumidor
    
    Subcategorias de produtos:
    - **vinhos**: Comercialização de vinhos finos e de mesa
    - **espumantes**: Comercialização de espumantes e frisantes
    - **sucos**: Comercialização de sucos de uva
    - **uvas**: Comercialização de uvas in natura
    """
    try:
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="comercializacao",
            start_year=start_year,
            end_year=end_year,
            subcategory=subcategoria,
            channel=canal,
            product_type=produto,
            region=regiao,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Se não foi especificado um canal, tente identificar em cada registro
            if not canal:
                # Tente identificar o canal de cada registro com base em palavras-chave
                canal_mapping = {
                    "varejo": ["varejo", "pequeno comércio", "pequeno comercio", "loja", "mercearia", "empório", "emporio"],
                    "grandes_redes": ["supermercado", "atacado", "rede", "hipermercado", "atacarejo", "wholesale", "carrefour", "pão de açúcar", "walmart"],
                    "exportacao_indireta": ["exportação", "exportacao", "intermediário", "intermediario", "trading", "comercial export"],
                    "venda_direta": ["venda direta", "consumidor final", "ecommerce", "e-commerce", "online", "própria", "propria", "vinícola", "vinicola"]
                }
                
                for item in result["data"]:
                    # Verifica se já tem canal identificado
                    if "canal" not in item:
                        # Verifica texto para identificar o canal
                        item_text = ""
                        for campo in ["Canal", "canal", "Vendedor", "vendedor", "Distribuição", "distribuicao", "Distribuicao", "Origem"]:
                            if campo in item and item[campo]:
                                item_text += str(item[campo]).lower() + " "
                        
                        # Verifica se o texto do item contém palavras-chave dos canais
                        for canal_nome, palavras_chave in canal_mapping.items():
                            if any(palavra in item_text for palavra in palavras_chave):
                                item["canal"] = canal_nome
                                break
                        
                        # Se não identificou o canal, verifica pelo volume/valor
                        if "canal" not in item and "Volume" in item:
                            volume = float(item["Volume"]) if isinstance(item["Volume"], (int, float)) or (isinstance(item["Volume"], str) and item["Volume"].isdigit()) else 0
                            # Volumes maiores tendem a ser grandes redes
                            if volume > 10000:
                                item["canal"] = "grandes_redes"
                            # Volumes menores tendem a ser varejo
                            elif volume > 0:
                                item["canal"] = "varejo"
                        
                        # Se mesmo assim não identificou, coloca como varejo (mais comum)
                        if "canal" not in item:
                            item["canal"] = "varejo"
            else:
                # Adicione o canal em cada registro usando o valor fornecido
                for item in result["data"]:
                    item["canal"] = canal
            
            # Se não foi especificada uma subcategoria, tente identificar em cada registro
            if not subcategoria:
                # Tente identificar a subcategoria de cada registro com base no produto
                produto_mapping = {
                    "vinhos": ["vinho", "tinto", "branco", "rosé", "rose", "mesa", "fino", "cabernet", "merlot", "chardonnay"],
                    "espumantes": ["espumante", "frisante", "champagne", "moscatel", "prosecco", "brut"],
                    "sucos": ["suco", "néctar", "bebida", "mosto", "integral", "concentrado"],
                    "uvas": ["uva", "fresca", "in natura", "niágara", "itália", "italia", "rubi", "benitaka"]
                }
                
                for item in result["data"]:
                    # Verifica se já tem subcategoria identificada
                    if "subcategoria" not in item:
                        # Verifica o texto de produto para identificar a subcategoria
                        # Procura em diferentes campos que podem conter informação do produto
                        item_text = ""
                        for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item", "Item", "Categoria"]:
                            if campo in item and item[campo]:
                                item_text += str(item[campo]).lower() + " "
                        
                        # Verifica se o texto do item contém palavras-chave das subcategorias
                        for subcategoria_nome, palavras_chave in produto_mapping.items():
                            if any(palavra in item_text for palavra in palavras_chave):
                                item["subcategoria"] = subcategoria_nome
                                break
                                
                        # Se mesmo assim não identificou, coloca como vinhos (mais comum)
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
                headers={"Content-Disposition": "attachment; filename=comercializacao.parquet"}
            )
            
        # Default: return JSON
        return result
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados de comercialização: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "VITI_003",
                "message": "Erro ao processar dados de comercialização",
                "resolution": "Tente novamente com intervalo de datas menor"
            }
        )