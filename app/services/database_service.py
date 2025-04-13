import os
import sqlite3
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

class DatabaseService:
    """Serviço para gerenciar conexões e operações no banco de dados SQLite"""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Inicializa o serviço de banco de dados
        
        Args:
            db_path: Caminho opcional para o arquivo de banco de dados.
                     Se não for fornecido, usa o configurado em settings.DATABASE_URL
        """
        # Extrai o caminho do arquivo do DATABASE_URL se não for fornecido
        if db_path is None:
            # Remove o prefixo 'sqlite:///' para obter o caminho do arquivo
            if settings.DATABASE_URL.startswith('sqlite:///'):
                db_path = settings.DATABASE_URL[len('sqlite:///'):]
            else:
                db_path = './vinidata.db'  # Fallback para o arquivo padrão
        
        self.db_path = db_path
        self._ensure_dir_exists()
        
        # Inicializa o banco de dados com tabelas básicas
        self._initialize_database()
    
    def _ensure_dir_exists(self):
        """Garante que o diretório do banco de dados exista"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    
    def _initialize_database(self):
        """Cria as tabelas básicas do banco de dados se não existirem"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela de dados de produção
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS producao (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ano INTEGER NOT NULL,
                    estado TEXT NOT NULL,
                    producao_uvas_mesa REAL,
                    producao_uvas_vinho REAL,
                    producao_total_uvas REAL,
                    producao_vinhos REAL,
                    producao_suco_integral REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de dados de exportação
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS exportacao (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ano INTEGER NOT NULL,
                    produto TEXT NOT NULL,
                    valor_dolar REAL,
                    quantidade_kg REAL,
                    preco_medio_dolar REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de dados de importação
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS importacao (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ano INTEGER NOT NULL,
                    produto TEXT NOT NULL,
                    valor_dolar REAL,
                    quantidade_kg REAL,
                    preco_medio_dolar REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela para cache de dados coletados
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_cache (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """
        Contexto para obter uma conexão com o banco de dados
        
        Returns:
            Objeto de conexão SQLite
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            # Configura para retornar rows como dicionários
            conn.row_factory = sqlite3.Row
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SQL e retorna os resultados como lista de dicionários
        
        Args:
            query: String SQL para executar
            params: Parâmetros para a consulta (opcional)
        
        Returns:
            Lista de resultados como dicionários
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            # Converte os resultados em uma lista de dicionários
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def execute_insert(self, query: str, params: tuple = None) -> int:
        """
        Executa uma consulta SQL de inserção e retorna o ID do registro inserido
        
        Args:
            query: String SQL para executar
            params: Parâmetros para a consulta (opcional)
        
        Returns:
            ID do registro inserido
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.lastrowid
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        Executa uma consulta SQL de atualização e retorna o número de linhas afetadas
        
        Args:
            query: String SQL para executar
            params: Parâmetros para a consulta (opcional)
        
        Returns:
            Número de linhas afetadas
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.rowcount
    
    def bulk_insert(self, table: str, columns: List[str], values: List[Tuple]) -> int:
        """
        Insere múltiplos registros em uma tabela
        
        Args:
            table: Nome da tabela
            columns: Lista de nomes de colunas
            values: Lista de tuplas com valores para inserir
        
        Returns:
            Número de registros inseridos
        """
        if not values:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Constrói a consulta SQL
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join(columns)
            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            
            # Executa a inserção em massa
            cursor.executemany(query, values)
            conn.commit()
            return cursor.rowcount