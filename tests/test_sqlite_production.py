import unittest
import os
import sqlite3
import tempfile
import shutil
import time
from pathlib import Path

from app.core.config import settings
from app.services.scraper.adaptive_scraper import AdaptiveScraper
from app.services.data_service import ViniDataService


class TestSQLiteProduction(unittest.TestCase):
    """Testes para avaliar a integração com SQLite em ambiente de produção"""
    
    def setUp(self):
        """Configura ambiente de testes salvando o caminho original do BD"""
        # Salva o caminho original do banco de dados
        self.original_db_path = None
        
        if hasattr(settings, 'SQLITE_DB_PATH'):
            self.original_db_path = settings.SQLITE_DB_PATH
        
        # Cria um diretório temporário para os testes
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "production_test.db")
        
        # Configura o caminho do banco de dados para o arquivo temporário
        if hasattr(settings, 'SQLITE_DB_PATH'):
            settings.SQLITE_DB_PATH = self.db_path
        
        # Variáveis para recursos que precisam ser limpos
        self.conn = None
        self.data_service = None
        self.scraper = None
    
    def tearDown(self):
        """Limpa recursos após os testes"""
        # Restaura configuração original
        if self.original_db_path is not None and hasattr(settings, 'SQLITE_DB_PATH'):
            settings.SQLITE_DB_PATH = self.original_db_path
        
        # Fecha explicitamente todas as conexões
        if hasattr(self, 'scraper') and self.scraper:
            try:
                self.scraper.close()
            except:
                pass
        
        if hasattr(self, 'data_service') and self.data_service and hasattr(self.data_service, 'scraper'):
            try:
                self.data_service.scraper.close()
            except:
                pass
        
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except:
                pass
        
        # Pequena pausa para garantir que as conexões foram fechadas
        time.sleep(0.1)
        
        # Remove diretório temporário e arquivos
        try:
            shutil.rmtree(self.test_dir)
        except PermissionError:
            print(f"Aviso: Não foi possível remover diretório temporário: {self.test_dir}")
    
    def test_production_db_path(self):
        """Verifica se as configurações de caminho do BD estão corretas"""
        # Verifica se a configuração existe
        self.assertTrue(hasattr(settings, 'SQLITE_DB_PATH'), 
                        "A configuração SQLITE_DB_PATH não existe no settings")
        
        # Verifica se o caminho está definido
        self.assertIsNotNone(settings.SQLITE_DB_PATH, 
                            "SQLITE_DB_PATH está definido como None")
        
        # O caminho deve ser uma string
        self.assertIsInstance(settings.SQLITE_DB_PATH, str, 
                            "SQLITE_DB_PATH deve ser uma string")
    
    def test_default_database_creation(self):
        """Verifica se o banco de dados padrão é criado durante a inicialização"""
        # Inicializa scraper sem especificar caminho - deve usar o padrão
        self.scraper = AdaptiveScraper(use_sqlite=True)
        
        # Verifica se o arquivo foi criado no local esperado
        db_path = settings.SQLITE_DB_PATH
        self.assertTrue(os.path.exists(db_path), 
                        f"O banco de dados não foi criado no caminho padrão: {db_path}")
        
        # Verifica se é um arquivo SQLite válido
        self.conn = sqlite3.connect(db_path)
        cursor = self.conn.cursor()
        
        # Verifica se as tabelas esperadas foram criadas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['categories', 'subcategories', 'cultivar_types']
        for table in expected_tables:
            self.assertIn(table, tables, f"Tabela esperada '{table}' não foi encontrada no banco")
        
        # Limpa recursos
        cursor.close()
    
    def test_data_service_production_integration(self):
        """Testa a integração do ViniDataService com o SQLite em produção"""
        # Inicializa o serviço de dados - deve usar o banco configurado
        self.data_service = ViniDataService()
        
        # Verifica se um scraper foi inicializado
        self.assertIsNotNone(self.data_service.scraper, 
                            "O scraper não foi inicializado no serviço de dados")
        
        # Verifica se o scraper está usando SQLite
        self.assertTrue(hasattr(self.data_service.scraper, 'use_sqlite'),
                       "O scraper não tem o atributo use_sqlite")
        
        self.assertTrue(self.data_service.scraper.use_sqlite,
                       "O scraper não está configurado para usar SQLite")
        
        # Verifica se o banco foi criado
        db_path = settings.SQLITE_DB_PATH
        self.assertTrue(os.path.exists(db_path), 
                        f"O banco de dados não foi criado pelo serviço: {db_path}")
    
    def test_file_persistence(self):
        """Testa se os dados são persistidos entre sessões"""
        # Primeiro, cria e popula o banco com alguns dados
        self.scraper = AdaptiveScraper(use_sqlite=True)
        
        # Adiciona um novo cultivar personalizado
        test_cultivar = "TesteUvaEspecial2025"
        test_category = "processamento"
        test_subcategory = "viniferas"
        
        # Adiciona à base de conhecimento
        self.scraper.classifier.add_cultivar(test_cultivar, test_subcategory, 1.0)
        
        # Fecha o scraper atual
        self.scraper.close()
        self.scraper = None
        
        # Garante que as conexões sejam liberadas
        time.sleep(0.2)
        
        # Cria um novo scraper (simula reinicialização da aplicação)
        new_scraper = AdaptiveScraper(use_sqlite=True)
        
        # Testa se o cultivar persistiu
        result = new_scraper.classify_cultivar(test_cultivar, test_category)
        
        # Limpa recursos
        new_scraper.close()
        
        # Verifica o resultado
        self.assertEqual(result, test_subcategory, 
                        f"O cultivar {test_cultivar} não foi persistido entre sessões")


if __name__ == '__main__':
    unittest.main()