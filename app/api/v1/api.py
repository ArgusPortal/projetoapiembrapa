from fastapi import APIRouter

from app.api.v1.endpoints import producao, processamento, comercializacao, exportacao, importacao, auth

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(
    auth.router, prefix="/auth", tags=["Authentication"]
)
api_router.include_router(
    producao.router, prefix="/producao", tags=["Dados de Produção"]
)
api_router.include_router(
    processamento.router, prefix="/processamento", tags=["Processamento Industrial"]
)
api_router.include_router(
    comercializacao.router, prefix="/comercializacao", tags=["Comercialização Interna"]
)
api_router.include_router(
    exportacao.router, prefix="/exportacao", tags=["Exportação"]
)
api_router.include_router(
    importacao.router, prefix="/importacao", tags=["Importação"]
)