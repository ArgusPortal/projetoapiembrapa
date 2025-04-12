import unittest
import os
import sqlite3
import tempfile
import shutil
import time
from pathlib import Path

from app.services.scraper.adaptive_scraper import AdaptiveScraper, CultivarClassifier
from app.services.data_service import ViniDataService


class TestSQLiteIntegration(unittest.TestCase):
    """Testes para avaliar a integração com SQLite no projeto"""
    
    def setUp(self):
        """Configura ambiente de testes com arquivos temporários"""
        # Cria um diretório temporário para os testes
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_classifier.db")
        self.conn = None
        self.classifier = None
        self.scraper = None
    
    def tearDown(self):
        """Limpa recursos após os testes"""
        # Fecha explicitamente todas as conexões
        if hasattr(self, 'scraper') and self.scraper:
            try:
                self.scraper.close()
            except:
                pass
        
        if hasattr(self, 'classifier') and self.classifier:
            try:
                self.classifier.conn.close()
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
    
    def test_sqlite_file_creation(self):
        """Verifica se o arquivo de banco SQLite é criado corretamente"""
        # Inicializa o scraper com um caminho de banco de dados específico
        self.scraper = AdaptiveScraper(use_sqlite=True, db_path=self.db_path)
        
        # Verifica se o arquivo foi criado
        self.assertTrue(os.path.exists(self.db_path), 
                        f"O arquivo de banco de dados {self.db_path} não foi criado")
        
        # Verifica se é um arquivo SQLite válido
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Verifica se as tabelas esperadas foram criadas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['categories', 'subcategories', 'cultivar_types']
        for table in expected_tables:
            self.assertIn(table, tables, f"Tabela esperada '{table}' não foi encontrada no banco")
        
        # Limpa recursos
        cursor.close()
    
    def test_cultivar_classification_database(self):
        """Testa o funcionamento do classificador de cultivares com banco SQLite"""
        # Cria o classificador com banco de dados persistente
        self.classifier = CultivarClassifier(knowledge_base_path=self.db_path)
        
        # Adiciona alguns cultivares de teste com seus tipos corretos
        self.classifier.add_cultivar("Cabernet Sauvignon", "viniferas", 1.0)
        self.classifier.add_cultivar("Niagara", "americanas", 1.0)
        self.classifier.add_cultivar("Niagara Rosada", "americanas", 1.0)  # Adiciona explicitamente
        self.classifier.add_cultivar("Italia", "mesa", 1.0)
        
        # Testa a classificação
        cultivar_tipo, confianca = self.classifier.classify("Cabernet")
        self.assertEqual(cultivar_tipo, "viniferas", "Classificação falhou para Cabernet")
        
        # Agora deve funcionar corretamente pois adicionamos explicitamente
        cultivar_tipo, confianca = self.classifier.classify("Niagara Rosada")
        self.assertEqual(cultivar_tipo, "americanas", "Classificação falhou para Niagara Rosada")
        
        # Fecha a conexão atual
        self.classifier.conn.close()
        
        # Verifica persistência reabrindo o banco
        new_classifier = CultivarClassifier(knowledge_base_path=self.db_path)
        cultivar_tipo, confianca = new_classifier.classify("Italia")
        self.assertEqual(cultivar_tipo, "mesa", "Persistência de dados falhou para Italia")
        new_classifier.conn.close()
    
    def test_adaptive_scraper_sqlite_integration(self):
        """Testa a integração do AdaptiveScraper com o SQLite"""
        # Inicializa o scraper com um banco de dados específico
        self.scraper = AdaptiveScraper(use_sqlite=True, db_path=self.db_path)
        
        # Verifica se as tabelas foram populadas com os dados iniciais
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Verifica as categorias
        cursor.execute("SELECT COUNT(*) FROM categories")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "Não foram encontradas categorias no banco")
        
        # Verifica as subcategorias
        cursor.execute("SELECT COUNT(*) FROM subcategories")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "Não foram encontradas subcategorias no banco")
        
        # Verifica os tipos de cultivares
        cursor.execute("SELECT COUNT(*) FROM cultivar_types")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "Não foram encontrados tipos de cultivares no banco")
        
        # Testa a classificação de um cultivar que deve existir por padrão
        subcategory = self.scraper.classify_cultivar("Cabernet Sauvignon", "processamento")
        self.assertEqual(subcategory, "viniferas", "Classificação falhou para Cabernet Sauvignon")
        
        # Limpa recursos
        cursor.close()
    
    def test_memory_database(self):
        """Testa a utilização do banco SQLite em memória"""
        # Inicializa o scraper com banco em memória
        self.scraper = AdaptiveScraper(use_sqlite=True, db_path=':memory:')
        
        # Testa a classificação
        subcategory = self.scraper.classify_cultivar("Merlot", "processamento")
        self.assertEqual(subcategory, "viniferas", "Classificação em banco em memória falhou")
    
    def test_data_service_with_sqlite(self):
        """Testa a integração do ViniDataService com o SQLite via AdaptiveScraper"""
        # Cria uma instância do ViniDataService
        service = ViniDataService()
        
        # Substitui o scraper padrão com um que usa um banco específico para o teste
        if hasattr(service, 'scraper') and service.scraper:
            try:
                service.scraper.close()
            except:
                pass
        
        # Cria um novo scraper com banco de dados específico
        self.scraper = AdaptiveScraper(use_sqlite=True, db_path=self.db_path)
        
        # Adiciona um cultivar específico para o teste
        self.scraper.init_sqlite_classifier()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Adiciona ou garante que Cabernet Sauvignon esteja no banco como vinífera
        cursor.execute("SELECT id FROM categories WHERE name = 'processamento'")
        category_id = cursor.fetchone()[0]
        
        cursor.execute("SELECT id FROM subcategories WHERE name = 'viniferas' AND category_id = ?", (category_id,))
        result = cursor.fetchone()
        if result:
            subcategory_id = result[0]
            
            # Adiciona o cultivar
            cursor.execute(
                'INSERT OR IGNORE INTO cultivar_types (subcategory_id, name, variants) VALUES (?, ?, ?)',
                (subcategory_id, "Cabernet Sauvignon", "cabernet;cab;cabernet sauvignon")
            )
            conn.commit()
        
        cursor.close()
        conn.close()
        
        # Atribui o scraper ao serviço
        service.scraper = self.scraper
        
        # Verifica a detecção automática de subcategoria
        category = "processamento"
        test_data = [{"cultivar": "Cabernet Sauvignon", "valor": 100}]
        
        subcategory = service.detect_subcategory_from_data(category, test_data)
        self.assertEqual(subcategory, "viniferas", "Falha na detecção de subcategoria com SQLite")


if __name__ == '__main__':
    unittest.main()