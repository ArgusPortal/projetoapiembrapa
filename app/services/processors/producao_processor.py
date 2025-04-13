import logging
import re
from typing import Dict, List, Any, Optional
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

class ProducaoProcessor:
    """
    Processador específico para dados de produção de uvas, vinhos e derivados
    """
    
    # Mapeamento de produtos conhecidos para suas subcategorias corretas
    PRODUTOS_CONHECIDOS = {
        # Produtos de SUCO
        "suco de uva integral": "SUCO",
        "suco de uva concentrado": "SUCO",
        "suco de uva adoçado": "SUCO",
        "suco de uva orgânico": "SUCO",
        "suco de uva reconstituído": "SUCO",
        "néctar de uva": "SUCO",
        "suco": "SUCO",
        
        # Produtos que às vezes são classificados incorretamente como vinho de mesa mas são DERIVADOS
        "filtrado": "DERIVADOS",
        "frisante": "DERIVADOS",
        "vinho leve": "DERIVADOS",
        "destilado": "DERIVADOS",
        "bagaceira": "DERIVADOS",
        "licor de bagaceira": "DERIVADOS",
        "vinho composto": "DERIVADOS",
        "pisco": "DERIVADOS", 
        "destilado alcoólico simples de bagaceira": "DERIVADOS",
        "vinho acidificado": "DERIVADOS",
        "outros derivados": "DERIVADOS",
        "compostos": "DERIVADOS",
        "espumante": "DERIVADOS",
        "champagne": "DERIVADOS",
        "sidra": "DERIVADOS",
        "vinho licoroso": "DERIVADOS",
        "vinho moscatel espumante": "DERIVADOS",
        
        # Produtos de VINHO DE MESA
        "tinto": "VINHO DE MESA",  # Contexto decide
        "branco": "VINHO DE MESA", # Contexto decide
        "rosado": "VINHO DE MESA", # Contexto decide
        "vinho de mesa tinto": "VINHO DE MESA",
        "vinho de mesa branco": "VINHO DE MESA",
        "vinho de mesa rosado": "VINHO DE MESA",
        "vinho de mesa tinto suave": "VINHO DE MESA",
        "vinho de mesa branco suave": "VINHO DE MESA",
        "vinho colonial": "VINHO DE MESA",
        "vinho de mesa": "VINHO DE MESA",
        
        # Produtos de VINHO FINO
        "tinto fino": "VINHO FINO DE MESA (VINIFERA)",
        "branco fino": "VINHO FINO DE MESA (VINIFERA)",
        "rosado fino": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino tinto": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino branco": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino rosado": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino de mesa": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino tinto seco": "VINHO FINO DE MESA (VINIFERA)",
        "vinho fino branco seco": "VINHO FINO DE MESA (VINIFERA)",
        "vinho vinífera": "VINHO FINO DE MESA (VINIFERA)",
        "vinho cabernet": "VINHO FINO DE MESA (VINIFERA)",
        "vinho merlot": "VINHO FINO DE MESA (VINIFERA)",
        "vinho chardonnay": "VINHO FINO DE MESA (VINIFERA)",
        "vinho sauvignon blanc": "VINHO FINO DE MESA (VINIFERA)",
        
        # Produtos de DERIVADOS
        "mosto concentrado": "DERIVADOS",
        "vinagre": "DERIVADOS",
        "borra líquida": "DERIVADOS", 
        "borra seca": "DERIVADOS",
        "brandy": "DERIVADOS",
        "vinho orgânico": "DERIVADOS",
        "jeropiga": "DERIVADOS"
    }
    
    def __init__(self, db_path: Optional[str] = None):
        """Inicializa o processador com conexão opcional ao banco de dados SQLite"""
        self.db_path = db_path
        self.db_conn = None
        if db_path:
            try:
                self.db_conn = sqlite3.connect(db_path)
                self._ensure_database_structure()
                logger.info(f"Conexão com banco de dados SQLite estabelecida: {db_path}")
            except Exception as e:
                logger.error(f"Erro ao conectar com banco de dados: {str(e)}")
                self.db_conn = None
    
    def _ensure_database_structure(self):
        """Garante que o banco de dados tem a estrutura necessária"""
        if not self.db_conn:
            return
            
        try:
            cursor = self.db_conn.cursor()
            
            # Tabela de produtos
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS produtos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE,
                subcategoria TEXT NOT NULL,
                ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                contagem INTEGER DEFAULT 1
            )
            ''')
            
            # Tabela para estatísticas de classificação
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS estatisticas_classificacao (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produto TEXT NOT NULL,
                subcategoria_atribuida TEXT NOT NULL,
                subcategoria_anterior TEXT,
                confianca REAL,
                data_classificacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.db_conn.commit()
            logger.info("Estrutura de banco de dados verificada/criada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao criar estrutura do banco de dados: {str(e)}")
    
    def _register_product_classification(self, produto: str, subcategoria: str):
        """Registra a classificação de um produto no banco de dados para aprendizado contínuo"""
        if not self.db_conn:
            return
            
        try:
            cursor = self.db_conn.cursor()
            
            # Verificar se o produto já existe
            cursor.execute("SELECT subcategoria, contagem FROM produtos WHERE nome = ? COLLATE NOCASE", (produto,))
            result = cursor.fetchone()
            
            timestamp = datetime.now().isoformat()
            
            if result:
                subcategoria_anterior, contagem = result
                
                # Se a subcategoria é a mesma, apenas incrementamos a contagem
                if subcategoria_anterior == subcategoria:
                    cursor.execute(
                        "UPDATE produtos SET contagem = contagem + 1, ultima_atualizacao = ? WHERE nome = ? COLLATE NOCASE",
                        (timestamp, produto)
                    )
                else:
                    # Se a nova classificação é diferente, registramos nas estatísticas
                    # e atualizamos se a nova classificação for considerada mais confiável
                    cursor.execute(
                        "INSERT INTO estatisticas_classificacao (produto, subcategoria_atribuida, subcategoria_anterior, data_classificacao) VALUES (?, ?, ?, ?)",
                        (produto, subcategoria, subcategoria_anterior, timestamp)
                    )
                    
                    # Se a nova classificação tem mais ocorrências, atualizamos
                    cursor.execute(
                        "SELECT COUNT(*) FROM estatisticas_classificacao WHERE produto = ? AND subcategoria_atribuida = ?",
                        (produto, subcategoria)
                    )
                    novas_ocorrencias = cursor.fetchone()[0]
                    
                    cursor.execute(
                        "SELECT COUNT(*) FROM estatisticas_classificacao WHERE produto = ? AND subcategoria_atribuida = ?",
                        (produto, subcategoria_anterior)
                    )
                    antigas_ocorrencias = cursor.fetchone()[0]
                    
                    if novas_ocorrencias > antigas_ocorrencias:
                        cursor.execute(
                            "UPDATE produtos SET subcategoria = ?, ultima_atualizacao = ? WHERE nome = ? COLLATE NOCASE",
                            (subcategoria, timestamp, produto)
                        )
            else:
                # Produto novo, inserimos no banco
                cursor.execute(
                    "INSERT INTO produtos (nome, subcategoria, ultima_atualizacao) VALUES (?, ?, ?)",
                    (produto, subcategoria, timestamp)
                )
            
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Erro ao registrar classificação do produto '{produto}': {str(e)}")
    
    @staticmethod
    def process_data(data: List[Dict[str, Any]], subcategoria: Optional[str] = None, 
                     produto: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Processa e classifica os dados de produção
        
        Args:
            data: Lista de dados brutos
            subcategoria: Filtro opcional de subcategoria
            produto: Filtro opcional de produto
            
        Returns:
            Lista de dados processados e classificados
        """
        try:
            if not data:
                logger.warning("Dados vazios fornecidos para processamento")
                return []
                
            logger.info(f"Iniciando processamento de {len(data)} registros de produção")
            if subcategoria:
                logger.info(f"Filtrando por subcategoria: {subcategoria}")
            if produto:
                logger.info(f"Filtrando por produto: {produto}")
                
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
            
            category_order = []
            current_category = None
            
            # Primeira passagem para identificar categorias principais e sua ordem
            for item in data:
                try:
                    # Extraímos as informações necessárias para classificação
                    produto_nome = ProducaoProcessor._extract_product_name(item)
                    
                    if not produto_nome:
                        continue
                    
                    # Verificamos se o nome do produto indica que é uma categoria principal
                    is_category_title = produto_nome.isupper() or (all(c.isupper() or not c.isalpha() for c in produto_nome) and len(produto_nome) > 3)
                    
                    if is_category_title:
                        determined_category = ProducaoProcessor._determine_main_category(produto_nome)
                        if determined_category in category_markers and not category_markers[determined_category]:
                            category_markers[determined_category] = True
                            category_order.append(determined_category)
                            current_category = determined_category
                except Exception as e:
                    logger.error(f"Erro ao processar item na primeira passagem: {str(e)}")
                    logger.debug(f"Item problemático: {item}")
                    continue
            
            logger.info(f"Categorias principais encontradas: {category_order}")
            
            # Segunda passagem para processar todos os itens com o contexto das categorias principais
            current_category = None
            processed_count = 0
            skipped_count = 0
            
            # Lista para armazenar todos os itens processados (sem filtro)
            all_processed_items = []
            
            for item in data:
                try:
                    # Extraímos as informações necessárias para classificação
                    produto_nome = ProducaoProcessor._extract_product_name(item)
                    
                    if not produto_nome:
                        skipped_count += 1
                        continue
                    
                    # Verificamos se o item tem um código de controle que indica a categoria
                    control_code = item.get("control", "")
                    
                    # Extraímos o valor para classificação
                    valor_str, valor_numerico = ProducaoProcessor._extract_value(item)
                    
                    # Verificamos se o nome do produto indica que é uma categoria principal
                    is_category_title = produto_nome.isupper() or (all(c.isupper() or not c.isalpha() for c in produto_nome) and len(produto_nome) > 3)
                    
                    # Determina a subcategoria correta
                    if is_category_title:
                        determined_category = ProducaoProcessor._determine_main_category(produto_nome)
                        if determined_category in category_markers:
                            item["subcategoria"] = determined_category
                            current_category = determined_category
                            item["categoria_principal"] = True
                        else:
                            # Se não é uma categoria principal válida, pode ser um título de subcategoria
                            # ou um produto que está em letras maiúsculas
                            item["subcategoria"] = current_category if current_category else "NÃO CATEGORIZADO"
                            item["categoria_principal"] = False
                    else:
                        # Classificação para produtos específicos (não títulos)
                        item["categoria_principal"] = False
                        
                        # Verificar se o produto está no mapeamento de produtos conhecidos (PRIORITÁRIO)
                        produto_lower = produto_nome.lower().strip()
                        if produto_lower in ProducaoProcessor.PRODUTOS_CONHECIDOS:
                            item["subcategoria"] = ProducaoProcessor.PRODUTOS_CONHECIDOS[produto_lower]
                        else:
                            # Se não está no mapeamento de produtos conhecidos, use a classificação normal
                            item["subcategoria"] = ProducaoProcessor._classify_product(
                                produto_nome, control_code, current_category, subcategoria, categoria_filtro=subcategoria
                            )
                    
                    # Aplicar correções específicas para certos produtos
                    ProducaoProcessor._handle_special_cases(item, produto_nome)
                    
                    # Verificar explicitamente os produtos que devem estar na categoria DERIVADOS
                    produto_lower = produto_nome.lower().strip()
                    if any(derivado in produto_lower for derivado in [
                        "composto", "filtrado", "frisante", "vinho leve", "destilado", 
                        "bagaceira", "licor", "pisco", "acidificado", "outros derivados"
                    ]):
                        item["subcategoria"] = "DERIVADOS"
                    
                    # Corrigir casos específicos de subcategoria "SUCO"
                    if produto_lower.startswith("suco") or "suco de uva" in produto_lower or produto_lower == "suco":
                        item["subcategoria"] = "SUCO"
                    
                    # Adiciona um nome completo para o produto, incluindo sua origem se necessário
                    ProducaoProcessor._add_complete_product_name(item, produto_nome)
                    
                    # Criamos uma chave única para evitar duplicatas
                    item_key = f"{produto_nome}_{item.get('subcategoria', '')}_{valor_str}_{item.get('ano', '')}"
                    if item_key in processed_items:
                        skipped_count += 1
                        continue
                    
                    processed_items[item_key] = True
                    
                    # Armazenar todos os itens processados
                    all_processed_items.append(item)
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Erro ao processar item na segunda passagem: {str(e)}")
                    logger.debug(f"Item problemático: {item}")
                    continue
            
            # Aplicar filtros após processar todos os itens
            for item in all_processed_items:
                should_include = True
                
                # Se filtrou por subcategoria, só inclui os itens daquela subcategoria
                if subcategoria and item.get("subcategoria") != subcategoria:
                    should_include = False
                
                # Se filtrou por produto específico (independente da subcategoria)
                if produto and item.get("Produto"):
                    produto_nome = item.get("Produto")
                    # Verifica se o produto buscado corresponde ao nome do produto
                    # de forma case-insensitive (independente da subcategoria)
                    if produto.lower() != produto_nome.lower() and produto.lower() not in produto_nome.lower():
                        should_include = False
                
                if should_include:
                    filtered_data.append(item)
            
            logger.info(f"Processamento concluído: {len(filtered_data)} itens incluídos, {skipped_count} itens ignorados")
            return filtered_data
        except Exception as e:
            logger.error(f"Erro geral no processamento de dados: {str(e)}")
            # Re-lançamos a exceção após o log para permitir tratamento no nível superior
            raise
    
    @staticmethod
    def _extract_product_name(item: Dict[str, Any]) -> Optional[str]:
        """Extrai o nome do produto dos campos possíveis no item"""
        for campo in ["Produto", "produto", "Descrição", "Descricao", "descrição", "descricao", "item", "Item", "Nome"]:
            if campo in item and item[campo]:
                return str(item[campo])
        logger.debug(f"Nome do produto não encontrado no item: {item}")
        return None
    
    @staticmethod
    def _extract_value(item: Dict[str, Any]) -> tuple:
        """Extrai o valor numérico e sua representação em string do item"""
        valor_str = ""
        valor_numerico = 0
        for campo_valor in ["Quantidade (L.)", "Quantidade", "valor", "Valor", "Volume"]:
            if campo_valor in item and item[campo_valor]:
                valor_str = str(item[campo_valor])
                try:
                    # Trata números no formato europeu (ex: 23.615.783)
                    if valor_str.count('.') > 1:
                        # Remove os pontos (separadores de milhares)
                        cleaned_valor = valor_str.replace('.', '')
                        # Converte vírgula para ponto decimal, se existir
                        cleaned_valor = cleaned_valor.replace(',', '.')
                        valor_numerico = float(cleaned_valor)
                    else:
                        # Trata formato padrão ou números com vírgula como separador decimal
                        cleaned_valor = valor_str.replace(',', '.')
                        # Remove caracteres não numéricos, exceto ponto decimal
                        cleaned_valor = ''.join(c for c in cleaned_valor if c.isdigit() or c == '.')
                        valor_numerico = float(cleaned_valor)
                except Exception as e:
                    logger.warning(f"Erro ao converter valor '{valor_str}' para numérico: {str(e)}")
                break
        if not valor_str:
            logger.debug(f"Valor não encontrado no item: {item}")
        return valor_str, valor_numerico
    
    @staticmethod
    def _determine_main_category(produto_nome: str) -> str:
        """Determina a categoria principal com base no nome do produto"""
        produto_upper = produto_nome.upper()
        
        if produto_upper == "SUCO" or "SUCO" in produto_upper and len(produto_upper) < 10:
            return "SUCO"
        elif "VINHO DE MESA" in produto_upper and "FINO" not in produto_upper and "VINIFERA" not in produto_upper:
            return "VINHO DE MESA"
        elif "VINHO FINO" in produto_upper or "VINIFERA" in produto_upper:
            return "VINHO FINO DE MESA (VINIFERA)"
        elif "DERIVADOS" in produto_upper or produto_upper == "DERIVADOS":
            return "DERIVADOS"
        
        logger.debug(f"Não foi possível determinar categoria principal para: '{produto_nome}'")
        return "NÃO CATEGORIZADO"
    
    @staticmethod
    def _classify_product(produto_nome: str, control_code: str, current_category: Optional[str], 
                         subcategoria: Optional[str], categoria_filtro: Optional[str] = None) -> str:
        """Classifica um produto com base no seu nome, código de controle e contexto"""
        # Se o filtro de categoria está definido e podemos usar essa informação
        if categoria_filtro:
            return categoria_filtro
            
        # Normalizamos o nome do produto para comparação
        produto_lower = produto_nome.lower().strip()
        
        # Verificar primeiro no dicionário de produtos conhecidos
        if produto_lower in ProducaoProcessor.PRODUTOS_CONHECIDOS:
            return ProducaoProcessor.PRODUTOS_CONHECIDOS[produto_lower]
            
        # Classifica com base no código de controle (prioritário)
        if control_code:
            if control_code.startswith("vm_"):
                return "VINHO DE MESA"
            elif control_code.startswith("vv_"):
                return "VINHO FINO DE MESA (VINIFERA)"
            elif control_code.startswith("su_"):
                return "SUCO"
            elif control_code.startswith("de_"):
                return "DERIVADOS"
        
        # Classificar com base na descrição do produto (palavras-chave)
        if "suco" in produto_lower or "néctar" in produto_lower:
            return "SUCO"
        elif "vinho" in produto_lower and any(termo in produto_lower for termo in ["fino", "vinífera", "vinifera"]):
            return "VINHO FINO DE MESA (VINIFERA)"
        elif "vinho" in produto_lower and "mesa" in produto_lower:
            return "VINHO DE MESA"
        elif any(termo in produto_lower for termo in ["derivado", "destilado", "fermentado", "espumante", "frisante", "bagaceira", "licor", "pisco", "filtrado", "composto"]):
            return "DERIVADOS"
            
        # Classificação baseada em palavras específicas
        if produto_lower in ["tinto", "branco", "rosado"]:
            # Pela análise, se não temos contexto, tinto/branco/rosado é geralmente vinho de mesa
            if current_category == "VINHO FINO DE MESA (VINIFERA)":
                return "VINHO FINO DE MESA (VINIFERA)"
            else:
                return "VINHO DE MESA"
                            
        # Se não conseguiu classificar por nome ou código, usa o contexto atual
        if not current_category:
            logger.debug(f"Não foi possível classificar produto: '{produto_nome}'")
            return "NÃO CATEGORIZADO"
            
        return current_category
    
    @staticmethod
    def _handle_special_cases(item: Dict[str, Any], produto_nome: str) -> None:
        """Lida com casos especiais de classificação"""
        produto_lower = produto_nome.lower().strip()
        
        # Produtos que frequentemente causam problemas de classificação
        if produto_lower == "total":
            item["subcategoria"] = "NÃO CATEGORIZADO"
            
        # Verificar casos específicos que podem aparecer em contextos variados
        if "vinho" in produto_lower and "orgânico" in produto_lower:
            if "fino" in produto_lower or "vinifera" in produto_lower or "vinífera" in produto_lower:
                item["subcategoria"] = "VINHO FINO DE MESA (VINIFERA)"
            else:
                item["subcategoria"] = "VINHO DE MESA"
                
        # Garantir que todos os sucos estão na categoria correta
        if produto_lower.startswith("suco") or "néctar" in produto_lower:
            item["subcategoria"] = "SUCO"
            
        # Produtos específicos que devem ser DERIVADOS mesmo quando aparecem em outras categorias
        if any(prod in produto_lower for prod in [
            "destilado", "espumante", "filtrado", "frisante", "bagaceira", 
            "licor", "pisco", "brandy", "vinho composto", "compostos", 
            "vinho leve", "vinho acidificado"
        ]):
            item["subcategoria"] = "DERIVADOS"
    
    @staticmethod
    def _add_complete_product_name(item: Dict[str, Any], produto_nome: str) -> None:
        """Adiciona nome completo do produto, incluindo sua origem quando necessário"""
        if produto_nome.lower() in ["tinto", "branco", "rosado"]:
            if item.get("subcategoria") == "VINHO FINO DE MESA (VINIFERA)":
                item["produto_completo"] = f"{produto_nome} (Viníferas)"
            elif item.get("subcategoria") == "VINHO DE MESA":
                item["produto_completo"] = f"{produto_nome} (Mesa)"
            else:
                item["produto_completo"] = produto_nome
        else:
            item["produto_completo"] = produto_nome
    
    def close(self):
        """Fecha conexão com o banco de dados"""
        if self.db_conn:
            try:
                self.db_conn.close()
                logger.info("Conexão com banco de dados SQLite fechada")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão com banco de dados: {str(e)}")
            finally:
                self.db_conn = None
    
    def __del__(self):
        """Destrutor para garantir que recursos são liberados"""
        self.close()

# Instância global do processador para uso em toda a aplicação
producao_processor = ProducaoProcessor(db_path="app/data/producao_classifications.db")