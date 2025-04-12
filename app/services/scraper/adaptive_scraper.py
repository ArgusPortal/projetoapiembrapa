# -*- coding: utf-8 -*-
import hashlib
import logging
import time
import sqlite3
import os
import json
import re
import pandas as pd
from typing import Dict, List, Optional, Union, Any, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pydantic import BaseModel, Field


class ScrapedData(BaseModel):
    """Model for scraped data with metadata"""
    source_url: str
    timestamp: float
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    raw_html: Optional[str] = Field(default=None, exclude=True)  # Store raw HTML for recovery but exclude from JSON


class CultivarClassifier:
    """
    Classifier for grape cultivars using SQLite in-memory database
    Provides more reliable classification than string matching
    """
    
    def __init__(self, knowledge_base_path: Optional[str] = None):
        """
        Initialize classifier with optional pre-existing knowledge base
        
        Args:
            knowledge_base_path: Path to JSON file with pre-trained classification data
        """
        self.logger = logging.getLogger(__name__)
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        
        # Create tables for classification
        self._create_tables()
        
        # Initialize with default knowledge
        self._init_default_knowledge()
        
        # Load external knowledge base if provided
        if knowledge_base_path and os.path.exists(knowledge_base_path):
            self.load_knowledge_base(knowledge_base_path)
    
    def _create_tables(self):
        """Create necessary tables for classification"""
        # Cultivars table - stores known cultivar names and their types
        self.cursor.execute('''
        CREATE TABLE cultivars (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            type TEXT,
            confidence FLOAT DEFAULT 1.0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Words table - stores word fragments and their association with types
        self.cursor.execute('''
        CREATE TABLE word_associations (
            id INTEGER PRIMARY KEY,
            word TEXT UNIQUE,
            viniferas_count INTEGER DEFAULT 0,
            americanas_count INTEGER DEFAULT 0,
            mesa_count INTEGER DEFAULT 0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Statistics table
        self.cursor.execute('''
        CREATE TABLE statistics (
            total_classified INTEGER DEFAULT 0,
            accuracy FLOAT DEFAULT 0.0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create indexes for performance
        self.cursor.execute('CREATE INDEX idx_cultivars_name ON cultivars(name)')
        self.cursor.execute('CREATE INDEX idx_word_associations_word ON word_associations(word)')
        
        self.conn.commit()
    
    def _init_default_knowledge(self):
        """Initialize database with default knowledge about cultivars"""
        # Initial set of known cultivars for each type
        cultivar_data = {
            "viniferas": [
                "Alicante Bouschet", "Ancelota", "Aramon", "Alfrocheiro", "Ancellotta",
                "Barbera", "Bonarda", "Cabernet Franc", "Cabernet Sauvignon", "Caladoc",
                "Carmenère", "Castelão", "Corvina", "Dornfelder", "Gamay Noir", 
                "Kanthus", "Magliocco", "Malbec", "Marselan", "Merlot", "Moscato Bailey", 
                "Moscato Preto", "Mourvèdre", "Muscat Noir", "Nebbiolo", "Petit Verdot", 
                "Pinot Meunier", "Pinot Noir", "Pinotage", "Primitivo", "Rebo", 
                "Ruby Cabernet", "Sangiovese", "Syrah", "Tannat", "Tempranillo", 
                "Teroldego", "Touriga Franca", "Touriga Nacional", "Trebbiano", "Trincadeira",
                "Viognier", "Cabernet", "Sauvignon", "Moscato", "Chardonnay",
                "Riesling", "Sauvignon Blanc", "Gewürztraminer", "Semillon", "Chenin Blanc"
            ],
            "americanas": [
                "Isabel", "Concord", "Bordô", "Bordó", "Niagara", "Niágara", "Jacquez", "Herbemont", 
                "Seibel", "BRS Magna", "BRS Violeta", "BRS Rúbea", "BRS Cora",
                "Seyve Villard", "Martha", "Cunningham", "Goethe"
            ],
            "mesa": [
                "Italia", "Itália", "Rubi", "Benitaka", "Red Globe", "Niagara Rosada", 
                "Crimson", "Thompson", "Perlette", "BRS Vitória", 
                "BRS Isis", "BRS Nubia", "BRS Morena", "BRS Clara", "BRS Linda"
            ]
        }
        
        # Add each cultivar to the database
        for cultivar_type, cultivars in cultivar_data.items():
            for cultivar in cultivars:
                self.add_cultivar(cultivar, cultivar_type)
                
                # Also extract words for statistical analysis
                words = self._extract_words(cultivar)
                for word in words:
                    self._update_word_association(word, cultivar_type)
    
    def _extract_words(self, text: str) -> List[str]:
        """
        Extract meaningful word fragments from text for statistical analysis
        
        Args:
            text: Text to extract words from
            
        Returns:
            List of extracted word fragments
        """
        if not text:
            return []
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove non-alphabetic characters
        words = ''.join(c if c.isalpha() or c.isspace() else ' ' for c in text).split()
        
        # Filter out short words (likely not meaningful)
        words = [w for w in words if len(w) > 2]
        
        # Add original words and also substrings for partial matching
        result = set(words)
        
        # Add substrings of longer words for partial matching
        for word in words:
            if len(word) > 5:
                # Add beginning of word (useful for prefixes)
                result.add(word[:4])
                # Add end of word (useful for suffixes)
                result.add(word[-4:])
        
        return list(result)
    
    def _update_word_association(self, word: str, cultivar_type: str):
        """
        Update statistical association between word and cultivar type
        
        Args:
            word: Word fragment to update
            cultivar_type: Type to associate with the word
        """
        # Check if word already exists
        self.cursor.execute('SELECT * FROM word_associations WHERE word = ?', (word,))
        row = self.cursor.fetchone()
        
        if row:
            # Update existing record
            field = f"{cultivar_type}_count"
            self.cursor.execute(f'''
            UPDATE word_associations 
            SET {field} = {field} + 1, last_update = CURRENT_TIMESTAMP
            WHERE word = ?
            ''', (word,))
        else:
            # Create new record
            fields = {'viniferas_count': 0, 'americanas_count': 0, 'mesa_count': 0}
            fields[f"{cultivar_type}_count"] = 1
            
            self.cursor.execute('''
            INSERT INTO word_associations (word, viniferas_count, americanas_count, mesa_count)
            VALUES (?, ?, ?, ?)
            ''', (word, fields['viniferas_count'], fields['americanas_count'], fields['mesa_count']))
        
        self.conn.commit()
    
    def add_cultivar(self, name: str, cultivar_type: str, confidence: float = 1.0):
        """
        Add a cultivar to the knowledge base
        
        Args:
            name: Name of the cultivar
            cultivar_type: Type of the cultivar (viniferas, americanas, mesa)
            confidence: Confidence level (0.0-1.0)
        
        Returns:
            True if the cultivar was added, False if it already exists
        """
        if not name or not cultivar_type:
            return False
            
        # Check if cultivar already exists
        self.cursor.execute('SELECT * FROM cultivars WHERE name = ?', (name.lower(),))
        row = self.cursor.fetchone()
        
        if row:
            # Update confidence if higher than current
            self.cursor.execute('''
            UPDATE cultivars 
            SET confidence = CASE WHEN ? > confidence THEN ? ELSE confidence END,
                type = ?,
                last_update = CURRENT_TIMESTAMP
            WHERE name = ?
            ''', (confidence, confidence, cultivar_type, name.lower()))
            result = False
        else:
            # Add new cultivar
            self.cursor.execute('''
            INSERT INTO cultivars (name, type, confidence)
            VALUES (?, ?, ?)
            ''', (name.lower(), cultivar_type, confidence))
            result = True
            
        # Update statistical word associations
        words = self._extract_words(name)
        for word in words:
            self._update_word_association(word, cultivar_type)
            
        self.conn.commit()
        return result
    
    def classify(self, cultivar_name: str) -> Tuple[str, float]:
        """
        Classify a cultivar name into its type with confidence score
        
        Args:
            cultivar_name: Name of the cultivar to classify
            
        Returns:
            Tuple of (cultivar_type, confidence_score)
            Where cultivar_type is one of: viniferas, americanas, mesa, unknown
        """
        if not cultivar_name:
            return "unknown", 0.0
            
        # Check if we have an exact match in our database
        self.cursor.execute('SELECT type, confidence FROM cultivars WHERE name = ?', (cultivar_name.lower(),))
        row = self.cursor.fetchone()
        
        if row:
            # Direct hit, return with high confidence
            return row[0], row[1]
            
        # No direct hit, try statistical word matching
        words = self._extract_words(cultivar_name)
        if not words:
            return "unknown", 0.0
            
        # Get word associations for each extracted word
        word_scores = {'viniferas': 0, 'americanas': 0, 'mesa': 0}
        total_matches = 0
        
        for word in words:
            self.cursor.execute('''
            SELECT viniferas_count, americanas_count, mesa_count
            FROM word_associations
            WHERE word = ?
            ''', (word,))
            row = self.cursor.fetchone()
            
            if row:
                word_total = sum(row)
                total_matches += 1
                
                # Calculate proportional scores for this word
                if word_total > 0:
                    word_scores['viniferas'] += row[0] / word_total
                    word_scores['americanas'] += row[1] / word_total
                    word_scores['mesa'] += row[2] / word_total
        
        if total_matches == 0:
            return "unknown", 0.0
            
        # Normalize scores by the number of matching words
        for category in word_scores:
            word_scores[category] /= total_matches
            
        # Find the highest scoring category
        best_category = max(word_scores.items(), key=lambda x: x[1])
        category, score = best_category
        
        # Only return the category if the score is above a threshold
        # Higher threshold when we have few word matches
        confidence_threshold = 0.4 if total_matches >= 2 else 0.6
        
        if score >= confidence_threshold:
            return category, score
        else:
            return "unknown", score
    
    def batch_classify(self, data_list: List[Dict[str, Any]], cultivar_field: str) -> List[Dict[str, Any]]:
        """
        Classify a batch of data items based on a cultivar field
        
        Args:
            data_list: List of data dictionaries to classify
            cultivar_field: Field name that contains the cultivar name
            
        Returns:
            List of data dictionaries with added classification information
        """
        result = []
        
        for item in data_list:
            # Create a copy of the item to avoid modifying the original
            new_item = item.copy()
            
            # Classify the cultivar if the field exists
            if cultivar_field in new_item and new_item[cultivar_field]:
                cultivar_type, confidence = self.classify(new_item[cultivar_field])
                
                # Add classification to the item
                new_item["cultivar_type"] = cultivar_type
                new_item["classification_confidence"] = round(confidence, 2)
            else:
                # No cultivar field or empty value
                new_item["cultivar_type"] = "unknown"
                new_item["classification_confidence"] = 0.0
                
            result.append(new_item)
            
        # Update statistics
        self._update_statistics(len(data_list))
        
        return result
    
    def _update_statistics(self, processed_count: int):
        """Update classification statistics"""
        self.cursor.execute('SELECT total_classified FROM statistics')
        row = self.cursor.fetchone()
        
        if row:
            # Update existing statistics
            self.cursor.execute('''
            UPDATE statistics
            SET total_classified = total_classified + ?,
                last_update = CURRENT_TIMESTAMP
            ''', (processed_count,))
        else:
            # Create initial statistics
            self.cursor.execute('''
            INSERT INTO statistics (total_classified)
            VALUES (?)
            ''', (processed_count,))
            
        self.conn.commit()
    
    def feedback(self, cultivar_name: str, correct_type: str) -> bool:
        """
        Process user feedback to improve classification
        
        Args:
            cultivar_name: Name of the cultivar
            correct_type: Correct type of the cultivar (viniferas, americanas, mesa)
            
        Returns:
            True if feedback was successfully processed
        """
        if not cultivar_name or not correct_type:
            return False
            
        if correct_type not in ["viniferas", "americanas", "mesa"]:
            self.logger.warning(f"Invalid cultivar type in feedback: {correct_type}")
            return False
        
        # Check if we have a previous classification for this cultivar
        self.cursor.execute('SELECT type FROM cultivars WHERE name = ?', (cultivar_name.lower(),))
        row = self.cursor.fetchone()
        
        previous_type = row[0] if row else None
        
        # We assign higher confidence for human feedback (0.9)
        # But not 1.0 to allow for potential future corrections
        self.add_cultivar(cultivar_name, correct_type, confidence=0.9)
        
        # Update accuracy statistics if we had a previous classification
        if previous_type and previous_type != correct_type:
            # We had a misclassification, update accuracy
            self.cursor.execute('''
            UPDATE statistics
            SET accuracy = (accuracy * total_classified - 1) / total_classified,
                last_update = CURRENT_TIMESTAMP
            ''')
        elif previous_type and previous_type == correct_type:
            # We were right, update accuracy positively
            self.cursor.execute('''
            UPDATE statistics
            SET accuracy = (accuracy * total_classified + 1) / total_classified,
                last_update = CURRENT_TIMESTAMP
            ''')
            
        self.conn.commit()
        self.logger.info(f"Feedback processed: {cultivar_name} -> {correct_type}")
        return True
    
    def export_knowledge_base(self, file_path: str) -> bool:
        """
        Export the knowledge base to a JSON file
        
        Args:
            file_path: Path to save the knowledge base
            
        Returns:
            True if the export was successful
        """
        try:
            # Get all cultivars
            self.cursor.execute('SELECT name, type, confidence FROM cultivars')
            cultivars = [{"name": row[0], "type": row[1], "confidence": row[2]} 
                         for row in self.cursor.fetchall()]
            
            # Get all word associations
            self.cursor.execute('''
            SELECT word, viniferas_count, americanas_count, mesa_count 
            FROM word_associations
            ''')
            word_associations = [
                {
                    "word": row[0], 
                    "associations": {
                        "viniferas": row[1],
                        "americanas": row[2],
                        "mesa": row[3]
                    }
                } 
                for row in self.cursor.fetchall()
            ]
            
            # Get statistics
            self.cursor.execute('SELECT total_classified, accuracy FROM statistics')
            row = self.cursor.fetchone()
            stats = {
                "total_classified": row[0] if row else 0,
                "accuracy": row[1] if row else 0.0,
                "last_update": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Create knowledge base
            knowledge_base = {
                "cultivars": cultivars,
                "word_associations": word_associations,
                "statistics": stats,
                "version": "1.0"
            }
            
            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Knowledge base exported to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting knowledge base: {str(e)}")
            return False
    
    def load_knowledge_base(self, file_path: str) -> bool:
        """
        Load a knowledge base from a JSON file
        
        Args:
            file_path: Path to the knowledge base file
            
        Returns:
            True if the import was successful
        """
        try:
            # Read from file
            with open(file_path, 'r', encoding='utf-8') as f:
                knowledge_base = json.load(f)
                
            if not isinstance(knowledge_base, dict):
                self.logger.error("Invalid knowledge base format")
                return False
                
            # Load cultivars
            if "cultivars" in knowledge_base:
                for cultivar in knowledge_base["cultivars"]:
                    self.add_cultivar(
                        cultivar["name"], 
                        cultivar["type"], 
                        cultivar.get("confidence", 1.0)
                    )
            
            # Load word associations (optional, as these will be recalculated)
            if "word_associations" in knowledge_base and False:  # Currently disabled
                # Clear existing associations
                self.cursor.execute('DELETE FROM word_associations')
                
                # Add new associations
                for assoc in knowledge_base["word_associations"]:
                    word = assoc["word"]
                    associations = assoc["associations"]
                    
                    self.cursor.execute('''
                    INSERT INTO word_associations 
                    (word, viniferas_count, americanas_count, mesa_count)
                    VALUES (?, ?, ?, ?)
                    ''', (
                        word, 
                        associations.get("viniferas", 0),
                        associations.get("americanas", 0),
                        associations.get("mesa", 0)
                    ))
            
            # Load statistics
            if "statistics" in knowledge_base:
                stats = knowledge_base["statistics"]
                
                self.cursor.execute('DELETE FROM statistics')
                self.cursor.execute('''
                INSERT INTO statistics (total_classified, accuracy)
                VALUES (?, ?)
                ''', (
                    stats.get("total_classified", 0),
                    stats.get("accuracy", 0.0)
                ))
                
            self.conn.commit()
            self.logger.info(f"Knowledge base loaded from {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading knowledge base: {str(e)}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get classification statistics
        
        Returns:
            Dictionary with classification statistics
        """
        # Get statistics
        self.cursor.execute('SELECT total_classified, accuracy FROM statistics')
        row = self.cursor.fetchone()
        
        # Get counts by type
        self.cursor.execute('''
        SELECT type, COUNT(*) FROM cultivars GROUP BY type
        ''')
        type_counts = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        # Get most recently added cultivars
        self.cursor.execute('''
        SELECT name, type, last_update FROM cultivars
        ORDER BY last_update DESC LIMIT 5
        ''')
        recent_additions = [
            {"name": row[0], "type": row[1], "added": row[2]} 
            for row in self.cursor.fetchall()
        ]
        
        return {
            "total_classified": row[0] if row else 0,
            "accuracy": row[1] if row else 0.0,
            "type_counts": type_counts,
            "recent_additions": recent_additions
        }


class AdaptiveScraper:
    """Adaptive web scraper for VitiViniBrasil data"""
    
    # Base URL for the website
    BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/index.php"
    
    # Category mappings (URL parameters)
    CATEGORY_MAPPING = {
        "producao": "opt_02",
        "processamento": "opt_03",
        "comercializacao": "opt_04",
        "importacao": "opt_05",
        "exportacao": "opt_06",
    }
    
    # Subcategory mappings for each category
    SUBCATEGORY_MAPPING = {
        "processamento": {
            "viniferas": "subopt_01",
            "americanas": "subopt_02",
            "mesa": "subopt_03",
            "semclassificacao": "subopt_04"
        },
        "importacao": {
            "vinhos": "subopt_01",
            "espumantes": "subopt_02",
            "sucos": "subopt_05",
            "passas": "subopt_06",
            "frescas": "subopt_07"
        },
        "exportacao": {
            "vinhos": "subopt_01",
            "espumantes": "subopt_02",
            "sucos": "subopt_04",
            "uvas": "subopt_05"
        },
        "producao": {
            "uva": "subopt_01",
            "vinho": "subopt_02",
            "suco": "subopt_03",
            "derivados": "subopt_04"
        }
    }
    
    # Cultivar type mapping to help auto-detect subcategories for processamento
    CULTIVAR_TYPE_MAPPING = {
        "processamento": {
            "viniferas": [
                "Alicante Bouschet", "Ancelota", "Aramon", "Alfrocheiro", "Ancellotta",
                "Barbera", "Bonarda", "Cabernet Franc", "Cabernet Sauvignon", "Caladoc",
                "Carmenère", "Castelão", "Corvina", "Dornfelder", "Gamay Noir", 
                "Kanthus", "Magliocco", "Malbec", "Marselan", "Merlot", "Moscato Bailey", 
                "Moscato Preto", "Mourvèdre", "Muscat Noir", "Nebbiolo", "Petit Verdot", 
                "Pinot Meunier", "Pinot Noir", "Pinotage", "Primitivo", "Rebo", 
                "Ruby Cabernet", "Sangiovese", "Syrah", "Tannat", "Tempranillo", 
                "Teroldego", "Touriga Franca", "Touriga Nacional", "Trebbiano", "Trincadeira",
                "Viognier", "Cabernet", "Sauvignon", "Moscato", "Chardonnay",
                "Riesling", "Sauvignon Blanc", "Gewürztraminer", "Semillon", "Chenin Blanc"
            ],
            "americanas": [
                "Isabel", "Concord", "Bordô", "Niagara", "Jacquez", "Herbemont", 
                "Seibel", "BRS Magna", "BRS Violeta", "BRS Rúbea", "BRS Cora",
                "Seyve Villard", "Martha", "Cunningham", "Goethe"
            ],
            "mesa": [
                "Italia", "Rubi", "Benitaka", "Red Globe", "Niagara Rosada", 
                "Itália", "Crimson", "Thompson", "Perlette", "BRS Vitória", 
                "BRS Isis", "BRS Nubia", "BRS Morena", "BRS Clara", "BRS Linda"
            ]
        }
    }
    
    # Fallback file mapping for each category and subcategory
    FALLBACK_FILE_MAPPING = {
        "producao": "Producao.csv",
        "comercializacao": "Comercio.csv",
        "processamento": {
            "default": "ProcessaSemclass.csv",
            "viniferas": "ProcessaViniferas.csv",
            "americanas": "ProcessaAmericanas.csv",
            "mesa": "ProcessaMesa.csv",
            "semclassificacao": "ProcessaSemclass.csv"
        },
        "importacao": {
            "default": "ImpVinhos.csv",
            "vinhos": "ImpVinhos.csv",
            "espumantes": "ImpEspumantes.csv",
            "sucos": "ImpSuco.csv",
            "passas": "ImpPassas.csv",
            "frescas": "ImpFrescas.csv"
        },
        "exportacao": {
            "default": "ExpVinho.csv",
            "vinhos": "ExpVinho.csv",
            "espumantes": "ExpEspumantes.csv",
            "sucos": "ExpSuco.csv",
            "uvas": "ExpUva.csv"
        }
    }
    
    # Patterns to detect subcategories based on filename or column names
    FILE_PATTERN_MAPPING = {
        "exportacao": {
            "vinhos": ["vinho", "vinhos", "ExpVinho"],
            "espumantes": ["espumante", "espumantes", "ExpEspumantes"],
            "sucos": ["suco", "sucos", "ExpSuco"],
            "uvas": ["uva", "uvas", "ExpUva", "fresca", "frescas"]
        },
        "importacao": {
            "vinhos": ["vinho", "vinhos", "ImpVinhos"],
            "espumantes": ["espumante", "espumantes", "ImpEspumantes"],
            "sucos": ["suco", "sucos", "ImpSuco"],
            "passas": ["passa", "passas", "ImpPassas"],
            "frescas": ["fresca", "frescas", "ImpFrescas"]
        }
    }
    
    def __init__(self, base_url: str = BASE_URL, use_sqlite: bool = True, db_path: Optional[str] = None):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.last_known_hash: Dict[str, str] = {}
        self.error_count: Dict[str, int] = {}
        self.max_retries = 3
        self.use_sqlite = use_sqlite
        
        # Initialize SQLite classifier if enabled
        if self.use_sqlite:
            self.db_path = db_path or ':memory:'
            self.init_sqlite_classifier()
        
        # Set up session with retry strategy
        self.session = requests.Session()
        self.retry_strategy = Retry(
            total=5,  # Increased from 3 to 5
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST']
        )
        self.session.mount('http://', HTTPAdapter(max_retries=self.retry_strategy))
        self.session.mount('https://', HTTPAdapter(max_retries=self.retry_strategy))
        
        # Set reasonable timeout
        self.timeout = 45  # Increased from default to accommodate slow server responses
        
        # User-agent to mimic normal browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
        })
    
    def init_sqlite_classifier(self):
        """
        Initialize SQLite database for improved classification
        """
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Create tables for classification
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            url_param TEXT
        )''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS subcategories (
            id INTEGER PRIMARY KEY,
            category_id INTEGER,
            name TEXT,
            url_param TEXT,
            FOREIGN KEY (category_id) REFERENCES categories (id),
            UNIQUE (category_id, name)
        )''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS cultivar_types (
            id INTEGER PRIMARY KEY,
            subcategory_id INTEGER,
            name TEXT,
            variants TEXT,
            FOREIGN KEY (subcategory_id) REFERENCES subcategories (id)
        )''')
        
        # Create indexes for faster lookup
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_cultivar_name ON cultivar_types(name)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_category_name ON categories(name)')
        
        # Populate categories
        for category, url_param in self.CATEGORY_MAPPING.items():
            self.cursor.execute(
                'INSERT OR IGNORE INTO categories (name, url_param) VALUES (?, ?)',
                (category, url_param)
            )
        
        # Populate subcategories
        for category, subcategories in self.SUBCATEGORY_MAPPING.items():
            self.cursor.execute('SELECT id FROM categories WHERE name = ?', (category,))
            category_id = self.cursor.fetchone()[0]
            
            for subcategory, url_param in subcategories.items():
                self.cursor.execute(
                    'INSERT OR IGNORE INTO subcategories (category_id, name, url_param) VALUES (?, ?, ?)',
                    (category_id, subcategory, url_param)
                )
        
        # Populate cultivar types
        for category, types in self.CULTIVAR_TYPE_MAPPING.items():
            self.cursor.execute('SELECT id FROM categories WHERE name = ?', (category,))
            category_id = self.cursor.fetchone()[0]
            
            for subcategory, cultivars in types.items():
                self.cursor.execute(
                    'SELECT id FROM subcategories WHERE category_id = ? AND name = ?',
                    (category_id, subcategory)
                )
                result = self.cursor.fetchone()
                if result:
                    subcategory_id = result[0]
                    
                    for cultivar in cultivars:
                        # Store variants (lowercase, without accents, etc.)
                        variants = self._generate_variants(cultivar)
                        self.cursor.execute(
                            'INSERT OR IGNORE INTO cultivar_types (subcategory_id, name, variants) VALUES (?, ?, ?)',
                            (subcategory_id, cultivar, ';'.join(variants))
                        )
        
        self.conn.commit()
    
    def _generate_variants(self, text: str) -> List[str]:
        """
        Generate variants of a text for fuzzy matching
        """
        variants = [text.lower()]
        
        # Remove accents
        no_accents = (
            text.lower()
            .replace('á', 'a').replace('à', 'a').replace('ã', 'a').replace('â', 'a')
            .replace('é', 'e').replace('è', 'e').replace('ê', 'e')
            .replace('í', 'i').replace('ì', 'i').replace('î', 'i')
            .replace('ó', 'o').replace('ò', 'o').replace('õ', 'o').replace('ô', 'o')
            .replace('ú', 'u').replace('ù', 'u').replace('û', 'u')
            .replace('ç', 'c')
        )
        if no_accents != variants[0]:
            variants.append(no_accents)
        
        # Remove spaces
        no_spaces = text.lower().replace(' ', '')
        if no_spaces != variants[0]:
            variants.append(no_spaces)
        
        # Remove other special characters
        clean = re.sub(r'[^a-zA-Z0-9]', '', text.lower())
        if clean != variants[0] and clean not in variants:
            variants.append(clean)
            
        return variants
    
    def classify_cultivar(self, name: str, category: str = 'processamento') -> Optional[str]:
        """
        Classify a cultivar name into the appropriate subcategory using SQLite
        
        Args:
            name: Name of the cultivar to classify
            category: Category to search within
            
        Returns:
            Subcategory name or None if not found
        """
        if not self.use_sqlite:
            # Fallback to the old classification method
            for subcategory, cultivars in self.CULTIVAR_TYPE_MAPPING.get(category, {}).items():
                if any(cultivar.lower() in name.lower() for cultivar in cultivars):
                    return subcategory
            return None
        
        self.cursor.execute('''
            SELECT s.name
            FROM categories c
            JOIN subcategories s ON c.id = s.category_id
            JOIN cultivar_types ct ON s.id = ct.subcategory_id
            WHERE c.name = ? AND (
                ct.name = ? OR 
                ? LIKE '%' || ct.name || '%' OR
                ? IN (SELECT value FROM json_each(json_array(ct.variants)))
                OR EXISTS (
                    SELECT 1 
                    FROM json_each(json_array(ct.variants)) 
                    WHERE ? LIKE '%' || value || '%'
                )
            )
            LIMIT 1
        ''', (category, name, name, name.lower(), name.lower()))
        
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def get_category_params(self, category: str, subcategory: Optional[str] = None) -> Dict[str, str]:
        """
        Get URL parameters for a specific category and subcategory
        
        Args:
            category: Main category name
            subcategory: Optional subcategory name
            
        Returns:
            Dictionary of URL parameters
        """
        if self.use_sqlite:
            # Use SQLite to get parameters
            if subcategory:
                self.cursor.execute('''
                    SELECT c.url_param, s.url_param
                    FROM categories c
                    JOIN subcategories s ON c.id = s.category_id
                    WHERE c.name = ? AND s.name = ?
                ''', (category, subcategory))
                result = self.cursor.fetchone()
                
                if not result:
                    raise ValueError(f'Unknown category/subcategory: {category}/{subcategory}')
                
                return {'opcao': result[0], 'subopcao': result[1]}
            else:
                self.cursor.execute('SELECT url_param FROM categories WHERE name = ?', (category,))
                result = self.cursor.fetchone()
                
                if not result:
                    raise ValueError(f'Unknown category: {category}')
                
                return {'opcao': result[0]}
        else:
            # Original method
            if category not in self.CATEGORY_MAPPING:
                raise ValueError(f'Unknown category: {category}')
                
            params = {'opcao': self.CATEGORY_MAPPING[category]}
            
            if subcategory:
                if subcategory not in self.SUBCATEGORY_MAPPING.get(category, {}):
                    raise ValueError(f'Unknown subcategory {subcategory} for category {category}')
                    
                params['subopcao'] = self.SUBCATEGORY_MAPPING[category][subcategory]
            
            return params
    
    def detect_schema_changes(self, url: str, html_content: str) -> bool:
        """
        Detect changes in HTML structure by comparing hash with last known hash
        
        Args:
            url: URL of the page
            html_content: HTML content to check
            
        Returns:
            bool: True if a change was detected, False otherwise
        """
        # Extract just the main content div to avoid hash changes due to dynamic elements
        soup = BeautifulSoup(html_content, 'html.parser')
        main_content = soup.find('div', {'class': 'main-content'})
        content_to_hash = (main_content.prettify() if main_content else html_content)
        
        current_hash = hashlib.md5(content_to_hash.encode()).hexdigest()
        
        if url not in self.last_known_hash:
            self.last_known_hash[url] = current_hash
            return False
            
        if current_hash != self.last_known_hash[url]:
            self.logger.warning(f'Schema change detected for {url}')
            self.last_known_hash[url] = current_hash
            return True
            
        return False
    
    def update_parsing_strategy(self, url: str, html_content: str) -> None:
        """
        Update the parsing strategy when a schema change is detected
        
        Args:
            url: URL of the page
            html_content: HTML content to analyze
        """
        self.logger.info(f'Analyzing new structure for {url}')
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Attempt to detect table headers and structure
        tables = soup.find_all('table')
        self.logger.info(f'Found {len(tables)} tables on the page')
        
        if not tables:
            # Try finding tables within specific containers
            containers = soup.find_all(['div', 'section'], {'class': ['content', 'main', 'data', 'table-container']})
            for container in containers:
                tables.extend(container.find_all('table'))
            self.logger.info(f'Found {len(tables)} tables after searching within containers')
    
    def extract_table_data(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Extract tabular data from HTML content
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            List of dictionaries containing the extracted data
        """
        results = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Try multiple strategies to find tables
        tables = []
        
        # Strategy 1: Find tables in main content
        main_content = soup.find('div', {'class': 'main-content'})
        if main_content:
            tables.extend(main_content.find_all('table'))
        
        # Strategy 2: Find tables anywhere
        if not tables:
            tables = soup.find_all('table')
        
        # Strategy 3: Look for tables within specific containers
        if not tables:
            containers = soup.find_all(['div', 'section'], {'class': ['content', 'main', 'data', 'table-container']})
            for container in containers:
                tables.extend(container.find_all('table'))
        
        self.logger.info(f'Found {len(tables)} tables to extract')
        
        # Process each table
        for table_index, table in enumerate(tables):
            self.logger.info(f'Processing table {table_index+1}/{len(tables)}')
            table_data = []
            headers = []
            
            # Try to find the header row - sometimes it's marked with th, sometimes with special classes
            header_candidates = [
                table.find('tr', {'class': ['header', 'heading', 'title']}),
                table.find('thead'),
                table.find('tr')  # Fallback to first row
            ]
            
            header_row = next((h for h in header_candidates if h is not None), None)
            
            # Extract headers
            if header_row:
                # Check if within thead
                if header_row.name == 'thead':
                    header_cells = header_row.find_all(['th', 'td'])
                else:
                    header_cells = header_row.find_all(['th', 'td'])
                
                headers = []
                for i, th in enumerate(header_cells):
                    header_text = th.text.strip()
                    if header_text:
                        # Clean up header text - remove line breaks, duplicate spaces
                        header_text = ' '.join(header_text.split())
                        headers.append(header_text)
                    else:
                        headers.append(f'column_{i}')
            
            # If we have <th> elements in the first row but didn't detect headers, use those
            if not headers and table.find_all('tr')[0].find('th'):
                headers = [th.text.strip() or f'column_{i}' for i, th in enumerate(table.find_all('tr')[0].find_all('th'))]
            
            # Extract data rows - if we found headers in the first row, skip it
            start_index = 1 if headers and table.find_all('tr')[0] == header_row else 0
            data_rows = table.find_all('tr')[start_index:]
            
            for row in data_rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                    
                # If no headers were found, create them based on the number of columns in the first row
                if not headers and cells:
                    headers = [f'column_{i}' for i in range(len(cells))]
                
                # Create row data
                row_data = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        header_key = headers[i]
                        # Skip empty header columns
                        if not header_key:
                            continue
                            
                        cell_text = cell.text.strip()
                        # Clean up cell text - remove line breaks, duplicate spaces
                        cell_text = ' '.join(cell_text.split())
                        row_data[header_key] = cell_text
                
                # Only add rows with data
                if row_data:
                    table_data.append(row_data)
            
            if table_data:
                results.extend(table_data)
        
        return results
    
    def scrape_with_pagination(self, url_params: Dict[str, str], start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """
        Scrape data with pagination for multiple years
        
        Args:
            url_params: URL parameters for the request
            start_year: First year to scrape
            end_year: Last year to scrape
            
        Returns:
            Combined list of data from all years
        """
        all_data = []
        raw_html_collection = {}
        
        for year in range(start_year, end_year + 1):
            # Add year to URL parameters
            year_params = {**url_params, 'ano': str(year)}
            
            # Build query string
            query_string = '&'.join([f'{k}={v}' for k, v in year_params.items()])
            year_url = f'{self.base_url}?{query_string}'
            
            self.logger.info(f'Scraping data for year {year} with URL: {year_url}')
            
            # Track retries for this specific URL
            retries = 0
            success = False
            
            while retries < self.max_retries and not success:
                try:
                    response = self.session.get(year_url, timeout=self.timeout)
                    response.raise_for_status()
                    
                    # Store raw HTML for potential recovery later
                    raw_html_collection[year] = response.text
                    
                    self.detect_schema_changes(year_url, response.text)
                    year_data = self.extract_table_data(response.text)
                    
                    # Add year as metadata
                    for item in year_data:
                        item['ano'] = year
                    
                    all_data.extend(year_data)
                    self.error_count[year_url] = 0  # Reset error count on success
                    success = True
                    
                    # Avoid rate limiting
                    time.sleep(1)
                    
                except requests.exceptions.RequestException as e:
                    retries += 1
                    wait_time = retries * 2  # Exponential backoff
                    self.logger.warning(f'Error scraping {year_url}, attempt {retries}/{self.max_retries}: {str(e)}')
                    self.logger.info(f'Waiting {wait_time} seconds before retrying')
                    time.sleep(wait_time)
                    
                    # Update error tracking
                    if year_url not in self.error_count:
                        self.error_count[year_url] = 1
                    else:
                        self.error_count[year_url] += 1
            
            # Log if all retries failed
            if not success:
                self.logger.error(f'Failed to scrape {year_url} after {self.max_retries} attempts')
        
        # Store raw HTML in result only if we have limited data
        if len(all_data) < 10:
            self.raw_html = raw_html_collection
        
        return all_data
    
    def analyze_data_for_classification(self, data: List[Dict[str, Any]], category: str) -> Dict[str, Any]:
        """
        Analyze the scraped data to infer additional classification information
        
        Args:
            data: The scraped data to analyze
            category: The main category of the data
            
        Returns:
            Dictionary with classification insights
        """
        if not data:
            return {}
            
        insights = {
            'likely_subcategory': None,
            'detected_cultivars': [],
            'confidence_score': 0.0,
        }
        
        # Extract all text values into a single string for analysis
        all_text = ' '.join([' '.join(str(val) for val in item.values()) for item in data])
        
        # Try to infer subcategory from content
        if category == 'processamento':
            subcategory_counts = {'viniferas': 0, 'americanas': 0, 'mesa': 0}
            
            for item in data:
                for field, value in item.items():
                    if not value or not isinstance(value, str):
                        continue
                        
                    # Try to classify each text field
                    subcategory = self.classify_cultivar(value, category)
                    if subcategory:
                        subcategory_counts[subcategory] = subcategory_counts.get(subcategory, 0) + 1
            
            # Find the subcategory with the highest count
            if subcategory_counts:
                max_count = max(subcategory_counts.values())
                if max_count > 0:
                    for subcat, count in subcategory_counts.items():
                        if count == max_count:
                            insights['likely_subcategory'] = subcat
                            insights['confidence_score'] = count / len(data)
                            break
        
        return insights
    
    def scrape_category(
        self, 
        category: str, 
        subcategory: Optional[str] = None,
        start_year: int = 1970, 
        end_year: int = 2025,
        region: Optional[str] = None,
        product_type: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None
    ) -> ScrapedData:
        """
        Scrape data for a specific category and subcategory
        
        Args:
            category: Category to scrape ('producao', 'processamento', 'comercializacao', etc.)
            subcategory: Subcategory to scrape (depends on category)
            start_year: First year to scrape
            end_year: Last year to scrape
            region: Optional region filter
            product_type: Optional product type filter
            origin: Optional origin country/region (for imports)
            destination: Optional destination country/region (for exports)
            
        Returns:
            ScrapedData object with the scraped data and metadata
        """
        # Get base URL parameters for the category/subcategory
        url_params = self.get_category_params(category, subcategory)
        
        # Add additional filters if provided
        additional_filters = {}
        if region:
            additional_filters['regiao'] = region
        if product_type:
            additional_filters['tipo'] = product_type
        if origin:
            additional_filters['origem'] = origin
        if destination:
            additional_filters['destino'] = destination
            
        url_params.update(additional_filters)
        
        self.logger.info(f'Scraping category: {category}, subcategory: {subcategory or "all"}')
        
        # Build query string without year (for metadata)
        query_string = '&'.join([f'{k}={v}' for k, v in url_params.items()])
        base_url = f'{self.base_url}?{query_string}'
        
        # Initialize raw_html attribute
        self.raw_html = {}
        
        data = self.scrape_with_pagination(url_params, start_year, end_year)
        
        # If no subcategory was specified, try to infer it from the data
        classification_insights = {}
        if not subcategory and data and category in ('processamento', 'importacao', 'exportacao'):
            classification_insights = self.analyze_data_for_classification(data, category)
            inferred_subcategory = classification_insights.get('likely_subcategory')
            
            if inferred_subcategory:
                self.logger.info(f'Inferred subcategory: {inferred_subcategory} with confidence {classification_insights.get("confidence_score", 0):.2f}')
                if classification_insights.get('confidence_score', 0) > 0.5:  # Only use if reasonably confident
                    subcategory = inferred_subcategory
        
        # Get sample page for recovery if needed
        sample_html = next(iter(self.raw_html.values())) if hasattr(self, 'raw_html') and self.raw_html else None
        
        return ScrapedData(
            source_url=base_url,
            timestamp=time.time(),
            data=data,
            metadata={
                'category': category,
                'subcategory': subcategory,
                'start_year': start_year,
                'end_year': end_year,
                'record_count': len(data),
                'years_with_data': list(set(item.get('ano') for item in data if 'ano' in item)),
                'filters': {
                    'region': region,
                    'product_type': product_type,
                    'origin': origin,
                    'destination': destination
                },
                'classification': classification_insights
            },
            raw_html=sample_html
        )
    
    def close(self):
        """
        Close SQLite connection if it exists
        """
        if self.use_sqlite and hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
                self.logger.info("SQLite connection closed")
            except Exception as e:
                self.logger.error(f"Error closing SQLite connection: {str(e)}")
    
    def __del__(self):
        """Ensure connection is closed on deletion"""
        if hasattr(self, 'use_sqlite') and self.use_sqlite and hasattr(self, 'conn'):
            self.conn.close()