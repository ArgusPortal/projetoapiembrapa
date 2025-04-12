import logging
from typing import Optional, List
from fastapi import APIRouter, Query, Depends, HTTPException, Response, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_429_TOO_MANY_REQUESTS
import pandas as pd
import json
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
import os

from app.services.data_service import vini_data_service
from app.schemas.data import DataResponse, ErrorResponse, DataFilter
from app.services.scraper.adaptive_scraper import CultivarClassifier

router = APIRouter()
security = HTTPBearer()
logger = logging.getLogger(__name__)

# Inicializa o classificador de cultivares com base de conhecimento padrão
# Verifica se existe um arquivo de base de conhecimento persistente
knowledge_base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                                 "data", "cultivar_knowledge_base.json")

# Cria o diretório data se não existir
os.makedirs(os.path.dirname(knowledge_base_path), exist_ok=True)

# Inicializa o classificador, carregando base de conhecimento se existir
cultivar_classifier = CultivarClassifier(
    knowledge_base_path=knowledge_base_path if os.path.exists(knowledge_base_path) else None
)

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
    subcategoria: str = Query(..., description="Subcategoria de processamento (obrigatória)", 
                             enum=["viniferas", "americanas", "mesa", "semclassificacao", "autodetect"]),
    tipo_uva: Optional[str] = Query(None, description="Filtrar por tipo específico de uva (ex: Cabernet, Isabel, etc)"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    auto_classificar: bool = Query(False, description="Classificar automaticamente cultivares com subcategoria desconhecida"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados sobre processamento industrial de uvas e vinhos
    
    - **viniferas**: Processamento de uvas viníferas (ex: Cabernet Sauvignon, Merlot, Chardonnay)
    - **americanas**: Processamento de uvas americanas e híbridas (ex: Isabel, Bordô, Concord)
    - **mesa**: Processamento de uvas de mesa (ex: Itália, Niágara, Rubi)
    - **semclassificacao**: Processamento de uvas sem classificação específica
    - **autodetect**: Tenta classificar automaticamente todas as cultivares (novo)
    """
    try:
        # Se subcategoria for "autodetect", usamos "semclassificacao" para obter todos os dados
        # e depois aplicamos a classificação automática
        data_subcategory = "semclassificacao" if subcategoria == "autodetect" else subcategoria
        
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="processamento",
            start_year=start_year,
            end_year=end_year,
            subcategory=data_subcategory,
            region=None,
            product_type=tipo_uva,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Aplicamos a classificação de cultivares se:
            # 1. subcategoria é "autodetect", ou
            # 2. auto_classificar=True e temos cultivares sem classificação
            if subcategoria == "autodetect" or auto_classificar:
                # Determinamos o campo que contém o nome da cultivar
                # Normalmente é "cultivar", "variedade", "uva" ou algo similar
                cultivar_field = next((field for field in ["cultivar", "variedade", "uva", "Cultivar", "Variedade", "Uva"]
                                      if any(field in item for item in result["data"])), None)
                
                if cultivar_field:
                    # Usa o classificador avançado para classificar as cultivares
                    result["data"] = cultivar_classifier.batch_classify(result["data"], cultivar_field)
                    
                    # Adiciona contagem de classificação nos metadados
                    tipo_counts = {
                        "viniferas": sum(1 for item in result["data"] if item.get("cultivar_type") == "viniferas"),
                        "americanas": sum(1 for item in result["data"] if item.get("cultivar_type") == "americanas"),
                        "mesa": sum(1 for item in result["data"] if item.get("cultivar_type") == "mesa"),
                        "unknown": sum(1 for item in result["data"] if item.get("cultivar_type") == "unknown"),
                    }
                    
                    # Adiciona a contagem aos metadados
                    if "metadata" not in result:
                        result["metadata"] = {}
                    result["metadata"]["classificacao"] = tipo_counts
                    
                    # Se quisermos filtrar por subcategoria específica em modo autodetect
                    if subcategoria == "autodetect" and subcategoria != "semclassificacao":
                        # Filtra apenas os resultados que correspondem à subcategoria solicitada
                        result["data"] = [item for item in result["data"] 
                                         if item.get("cultivar_type") == subcategoria]
            
            # Adiciona a subcategoria como metadado em cada item se não for autodetect
            elif subcategoria != "semclassificacao":
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

@router.post(
    "/feedback",
    response_model=dict,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
async def submit_cultivar_feedback(
    feedback: dict = Body(..., example={"cultivar_name": "Cabernet Franc", "correct_type": "viniferas"}),
    _: bool = Depends(has_access)
):
    """
    Submete feedback para melhorar a classificação de cultivares
    
    Este endpoint permite que usuários forneçam a classificação correta para uma 
    cultivar, melhorando o sistema de classificação automática.
    
    - **cultivar_name**: Nome da cultivar a ser classificada
    - **correct_type**: Tipo correto (viniferas, americanas, mesa)
    """
    try:
        cultivar_name = feedback.get("cultivar_name")
        correct_type = feedback.get("correct_type")
        
        if not cultivar_name or not correct_type:
            raise HTTPException(
                status_code=400,
                detail="Os campos 'cultivar_name' e 'correct_type' são obrigatórios"
            )
            
        if correct_type not in ["viniferas", "americanas", "mesa"]:
            raise HTTPException(
                status_code=400,
                detail="O campo 'correct_type' deve ser 'viniferas', 'americanas' ou 'mesa'"
            )
        
        # Adiciona o feedback ao classificador
        cultivar_classifier.feedback(cultivar_name, correct_type)
        
        # Salva a base de conhecimento atualizada
        cultivar_classifier.export_knowledge_base(knowledge_base_path)
        
        return {
            "status": "success",
            "message": f"Feedback para '{cultivar_name}' como '{correct_type}' adicionado com sucesso"
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao processar feedback: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "VITI_005",
                "message": "Erro ao processar feedback de classificação",
                "resolution": "Verifique os dados enviados e tente novamente"
            }
        )