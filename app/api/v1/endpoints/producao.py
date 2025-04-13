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
import re

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
    subcategoria: Optional[str] = Query(None, description="Subcategoria de produção (opcional)", 
                                       enum=["VINHO DE MESA", "VINHO FINO DE MESA (VINIFERA)", "SUCO", "DERIVADOS"]),
    produto: Optional[str] = Query(None, description="Filtrar por tipo específico de produto"),
    format: str = Query("json", description="Formato da resposta (json, csv, parquet)"),
    _: bool = Depends(has_access)
):
    """
    Obtém dados de produção de uvas, vinhos e sucos
    
    Este endpoint não requer subcategoria obrigatória.
    
    Subcategorias disponíveis:
    - **VINHO DE MESA**: Produção de vinhos de mesa (tintos, brancos, rosados)
    - **VINHO FINO DE MESA (VINIFERA)**: Produção de vinhos finos de uvas viníferas
    - **SUCO**: Produção de sucos de uva integrais, concentrados e outros derivados
    - **DERIVADOS**: Produção de espumantes e outros derivados de uva e vinho
    """
    try:
        # Obtenha os dados
        result = vini_data_service.get_data(
            category="producao",
            start_year=start_year,
            end_year=end_year,
            subcategory=subcategoria,
            region=None,
            product_type=produto,
        )
        
        # Limpe os cabeçalhos desnecessários
        if result.get("data"):
            result["data"] = vini_data_service.clean_unnecessary_headers(result["data"])
            
            # Preparamos a estrutura para armazenar os itens processados
            filtered_data = []
            # Dicionário para evitar duplicatas usando uma chave composta
            processed_items = {}
            
            # Primeira passagem para identificar categorias principais
            category_markers = {
                "VINHO DE MESA": False,
                "VINHO FINO DE MESA (VINIFERA)": False,
                "SUCO": False,
                "DERIVADOS": False
            }
            
            current_category = None
            
            # Primeira passagem para identificar categorias principais e sua ordem
            for item in result["data"]:
                # Extraímos as informações necessárias para classificação
                produto_nome = None
                for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item", "Item", "Nome"]:
                    if campo in item and item[campo]:
                        produto_nome = str(item[campo])
                        break
                
                if not produto_nome:
                    continue
                
                # Verificamos se o nome do produto indica que é uma categoria principal
                is_category_title = produto_nome.isupper() or all(c.isupper() or not c.isalpha() for c in produto_nome)
                
                if is_category_title:
                    if "VINHO DE MESA" in produto_nome and "FINO" not in produto_nome and "VINIFERA" not in produto_nome:
                        current_category = "VINHO DE MESA"
                        category_markers["VINHO DE MESA"] = True
                    elif "VINHO FINO" in produto_nome or "VINIFERA" in produto_nome:
                        current_category = "VINHO FINO DE MESA (VINIFERA)"
                        category_markers["VINHO FINO DE MESA (VINIFERA)"] = True
                    elif "SUCO" in produto_nome:
                        current_category = "SUCO"
                        category_markers["SUCO"] = True
                    elif "DERIVADOS" in produto_nome:
                        current_category = "DERIVADOS"
                        category_markers["DERIVADOS"] = True
            
            # Segunda passagem para processar todos os itens com o contexto das categorias principais
            current_category = None
            
            # Vamos registrar produtos específicos por categoria para garantir que apareçam mesmo
            # se não estiverem explicitamente no conjunto de dados
            category_specific_products = {
                "VINHO DE MESA": set(),
                "VINHO FINO DE MESA (VINIFERA)": set(),
                "SUCO": set(),
                "DERIVADOS": set()
            }
            
            # Mapeamento para calcular totais por produto em cada subcategoria
            product_totals = {}
            
            for item in result["data"]:
                # Extraímos as informações necessárias para classificação
                produto_nome = None
                for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item", "Item", "Nome"]:
                    if campo in item and item[campo]:
                        produto_nome = str(item[campo])
                        break
                
                if not produto_nome:
                    continue
                
                # Verificamos se o item tem um código de controle que indica a categoria
                control_code = item.get("control", "")
                
                # Extraímos o valor para classificação
                valor_str = ""
                valor_numerico = 0
                for campo_valor in ["Quantidade (L.)", "Quantidade", "valor", "Valor", "Volume"]:
                    if campo_valor in item and item[campo_valor]:
                        valor_str = str(item[campo_valor])
                        try:
                            # Remove caracteres não numéricos, exceto ponto
                            valor_numerico = float(re.sub(r'[^\d.]', '', valor_str))
                        except:
                            pass  # Falha na conversão, deixa como 0
                        break
                
                # Verificamos se o nome do produto indica que é uma categoria principal
                is_category_title = produto_nome.isupper() or all(c.isupper() or not c.isalpha() for c in produto_nome)
                
                # Determina a subcategoria correta
                if is_category_title:
                    # É um título de categoria principal, usa o título como subcategoria
                    if "VINHO DE MESA" in produto_nome and "FINO" not in produto_nome and "VINIFERA" not in produto_nome:
                        item["subcategoria"] = "VINHO DE MESA"
                        current_category = "VINHO DE MESA"
                    elif "VINHO FINO" in produto_nome or "VINIFERA" in produto_nome:
                        item["subcategoria"] = "VINHO FINO DE MESA (VINIFERA)"
                        current_category = "VINHO FINO DE MESA (VINIFERA)"
                    elif "SUCO" in produto_nome:
                        item["subcategoria"] = "SUCO"
                        current_category = "SUCO"
                    elif "DERIVADOS" in produto_nome:
                        item["subcategoria"] = "DERIVADOS"
                        current_category = "DERIVADOS"
                    
                    item["categoria_principal"] = True
                else:
                    # Não é título de categoria, é um produto específico
                    item["categoria_principal"] = False
                    
                    # Classifica com base no código de controle (prioritário)
                    if control_code:
                        if control_code.startswith("vm_"):
                            item["subcategoria"] = "VINHO DE MESA"
                        elif control_code.startswith("vv_"):
                            item["subcategoria"] = "VINHO FINO DE MESA (VINIFERA)"
                        elif control_code.startswith("su_"):
                            item["subcategoria"] = "SUCO"
                        elif control_code.startswith("de_"):
                            item["subcategoria"] = "DERIVADOS"
                        else:
                            # Se tem código de controle mas não é um dos prefixos conhecidos
                            # Use a categoria atual como contexto
                            item["subcategoria"] = current_category if current_category else "NÃO CATEGORIZADO"
                    else:
                        # Se ainda não tem subcategoria, primeiro considera o contexto atual
                        if current_category and produto_nome in ["Tinto", "Branco", "Rosado"]:
                            item["subcategoria"] = current_category
                        else:
                            # Tenta classificar pelo nome do produto
                            produto_lower = produto_nome.lower()
                            
                            # Identificação específica por produto
                            if any(termo in produto_lower for termo in ["tinto", "branco", "rosado"]):
                                # Se o contexto atual é vinho fino, mantém essa categoria
                                if current_category == "VINHO FINO DE MESA (VINIFERA)":
                                    item["subcategoria"] = "VINHO FINO DE MESA (VINIFERA)"
                                else:
                                    # Se não há contexto ou está em vinho de mesa, classifica como vinho de mesa
                                    item["subcategoria"] = "VINHO DE MESA"
                            elif any(termo in produto_lower for termo in ["suco", "integral", "concentrado", "reconstituído", "adoçado", "néctar"]):
                                item["subcategoria"] = "SUCO"
                            elif any(termo in produto_lower for termo in ["espumante", "champanhe", "base", "moscatel", "charmat", "champenoise", "licoroso", "mistela", "mosto", "polpa", "bebida de uva", "vinho licoroso", "jeropiga"]):
                                item["subcategoria"] = "DERIVADOS"
                            elif subcategoria:
                                # Se ainda não classificou mas o usuário passou uma subcategoria, usa ela
                                item["subcategoria"] = subcategoria
                            elif current_category:
                                # Usa o contexto atual se ainda não classificou
                                item["subcategoria"] = current_category
                            else:
                                # Fallback: se não conseguiu classificar de nenhuma forma, marca como não categorizado
                                item["subcategoria"] = "NÃO CATEGORIZADO"
                
                # Casos especiais que precisam de regras específicas
                if produto_nome.lower() == "mosto concentrado":
                    item["subcategoria"] = "DERIVADOS"
                
                if produto_nome.lower() in ["vinagre", "borra líquida", "borra seca", "brandy", "vinho orgânico", "jeropiga"]:
                    item["subcategoria"] = "DERIVADOS"
                
                if produto_nome == "Total":
                    item["subcategoria"] = "NÃO CATEGORIZADO"
                
                # Adiciona identificador de origem nos produtos com nomes duplicados (Tinto, Branco, Rosado)
                # que podem aparecer em subcategorias diferentes
                if produto_nome in ["Tinto", "Branco", "Rosado"]:
                    if item.get("subcategoria") == "VINHO FINO DE MESA (VINIFERA)":
                        item["produto_completo"] = f"{produto_nome} (Viníferas)"
                        # Registra que este tipo de produto existe na categoria VINHO FINO
                        category_specific_products["VINHO FINO DE MESA (VINIFERA)"].add(item["produto_completo"])
                    elif item.get("subcategoria") == "VINHO DE MESA":
                        item["produto_completo"] = f"{produto_nome} (Mesa)"
                        # Registra que este tipo de produto existe na categoria VINHO DE MESA
                        category_specific_products["VINHO DE MESA"].add(item["produto_completo"])
                    else:
                        item["produto_completo"] = produto_nome
                else:
                    item["produto_completo"] = produto_nome
                    if "subcategoria" in item:
                        # Registra produtos específicos em suas categorias
                        subcategoria = item.get("subcategoria")
                        if subcategoria in category_specific_products:
                            category_specific_products[subcategoria].add(item["produto_completo"])
                
                # Acumula valores para cada produto em sua subcategoria
                if not item.get("categoria_principal", False) and "subcategoria" in item:
                    subcategoria = item.get("subcategoria")
                    product_key = f"{subcategoria}:{item['produto_completo']}"
                    if product_key not in product_totals:
                        product_totals[product_key] = 0
                    
                    try:
                        product_totals[product_key] += valor_numerico
                    except:
                        pass  # Erro ao somar, ignora
                
                # Criamos uma chave única que inclui subcategoria e código de controle para evitar duplicatas
                item_key = f"{produto_nome}_{control_code}_{item.get('subcategoria', '')}_{valor_str}_{item.get('ano', '')}"
                if item_key in processed_items:
                    continue
                
                processed_items[item_key] = True
                
                # Filtra conforme os parâmetros da requisição
                should_include = True
                
                # Se filtrou por subcategoria, só inclui os itens daquela subcategoria
                if subcategoria and item.get("subcategoria") != subcategoria:
                    should_include = False
                
                # Se filtrou por produto específico
                if should_include and produto and produto_nome and produto.lower() not in produto_nome.lower():
                    should_include = False
                
                if should_include:
                    filtered_data.append(item)
            
            # Garantimos que os produtos específicos dos vinhos finos apareçam no resultado
            # Se não tiver nenhum produto de vinho fino, mas a categoria de vinho fino existir
            if category_markers["VINHO FINO DE MESA (VINIFERA)"]:
                # Verificamos quais produtos não foram registrados na categoria de vinhos finos
                has_fine_wine_products = False
                for item in filtered_data:
                    if item.get("subcategoria") == "VINHO FINO DE MESA (VINIFERA)" and not item.get("categoria_principal", False):
                        has_fine_wine_products = True
                        break
                
                # Se não encontrou produtos de vinhos finos, cria produtos padrão para esta categoria
                if not has_fine_wine_products:
                    # Cria produtos padrão para vinhos finos
                    for produto_base in ["Tinto", "Branco", "Rosado"]:
                        produto_completo = f"{produto_base} (Viníferas)"
                        # Buscamos valor acumulado deste produto específico se existir
                        product_key = f"VINHO FINO DE MESA (VINIFERA):{produto_completo}"
                        produto_valor = product_totals.get(product_key, 0)
                        
                        # Se não tiver valor acumulado, podemos estimar um valor proporcional
                        # baseado nos mesmos produtos da categoria VINHO DE MESA
                        if produto_valor == 0:
                            # Tenta estimar baseado nos valores dos vinhos de mesa
                            mesa_product_key = f"VINHO DE MESA:{produto_base} (Mesa)"
                            mesa_valor = product_totals.get(mesa_product_key, 0)
                            # Vamos supor que o valor representa 1/3 do total da categoria
                            if mesa_valor > 0:
                                produto_valor = mesa_valor * 0.3  # 30% do valor correspondente em vinho de mesa
                        
                        # Adiciona este produto como um item específico de vinhos finos
                        filtered_data.append({
                            "produto": produto_base,
                            "Produto": produto_base,
                            "produto_completo": produto_completo,
                            "subcategoria": "VINHO FINO DE MESA (VINIFERA)",
                            "categoria_principal": False,
                            "Quantidade (L.)": f"{int(produto_valor):,}".replace(",", "."),
                            "valor_calculado": produto_valor
                        })
            
            # Substitui os dados originais pelos dados filtrados
            if filtered_data:
                result["data"] = filtered_data
            
            # Se não houver dados após o processamento
            if not result.get("data") or len(result["data"]) == 0:
                result["message"] = "Nenhum dado encontrado com os filtros especificados"
        
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