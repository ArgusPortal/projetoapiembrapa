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
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Importar as classes de seus novos locais
from app.services.scraper.models import ScrapedData
from app.services.scraper.cultivar_classifier import CultivarClassifier
from app.services.scraper.html_parser import extract_table_data, parse_html, detect_schema_changes, update_parsing_strategy
from app.services.scraper.constants import (
    BASE_URL, CATEGORY_MAPPING, SUBCATEGORY_MAPPING, 
    SUBCATEGORY_ALIASES, SUBCATEGORY_DISPLAY_NAMES
)


class AdaptiveScraper:
    """Adaptive web scraper for VitiViniBrasil data"""
    
    # Base URL for the website
    BASE_URL = BASE_URL
    
    # Category mappings (URL parameters)
    CATEGORY_MAPPING = CATEGORY_MAPPING
    
    # Subcategory mappings for each category
    SUBCATEGORY_MAPPING = SUBCATEGORY_MAPPING
    
    # Alternative names mapping for subcategories (handles case and format variations)
    SUBCATEGORY_ALIASES = SUBCATEGORY_ALIASES
    
    # Display names for subcategories (for UI presentation)
    SUBCATEGORY_DISPLAY_NAMES = SUBCATEGORY_DISPLAY_NAMES
    
    # Reverse mapping for display names to subcategory keys
    DISPLAY_TO_SUBCATEGORY = {}
    
    def __init__(self, base_url: str = BASE_URL, use_sqlite: bool = True, db_path: Optional[str] = None):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.last_known_hash: Dict[str, str] = {}
        self.error_count: Dict[str, int] = {}
        self.max_retries = 3
        self.use_sqlite = use_sqlite
        self.expected_items_per_page = 50  # Default number of items per page
        self.delay_between_requests = 2  # Seconds between requests to avoid rate limiting
        
        # Headers for HTTP requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        # Build the reverse mapping from display names to subcategory keys
        self._build_display_name_mapping()
        
        # Initialize SQLite classifier if enabled
        if self.use_sqlite:
            self.init_sqlite_classifier(db_path or "app/data/cultivars.db")
        
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
        self.session.headers.update(self.headers)
    
    def init_sqlite_classifier(self, db_path: str = "app/data/cultivars.db"):
        """
        Initialize SQLite classifier for cultivar identification
        
        Args:
            db_path: Path to SQLite database
        """
        try:
            # First check if the database file exists
            if not os.path.exists(db_path):
                # Try to create the database with basic structure
                self._create_sqlite_database(db_path)
            
            self.sqlite_classifier = CultivarClassifier(knowledge_base_path=db_path)
            self.logger.info("SQLite classifier initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize SQLite classifier: {str(e)}")
            self.sqlite_classifier = None
    
    def _create_sqlite_database(self, db_path: str):
        """
        Create SQLite database for cultivar classification if it doesn't exist
        
        Args:
            db_path: Path where the database should be created
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Create the database connection
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create the required tables
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cultivar_types (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY,
                cultivar_type_id INTEGER,
                keyword TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                FOREIGN KEY (cultivar_type_id) REFERENCES cultivar_types(id)
            )
            ''')
            
            # Insert default category types
            category_types = [
                "VINHO DE MESA",
                "VINHO FINO DE MESA (VINIFERA)",
                "SUCO",
                "DERIVADOS"
            ]
            
            for category in category_types:
                cursor.execute('INSERT OR IGNORE INTO cultivar_types (name) VALUES (?)', (category,))
            
            # Add default keywords for each category
            keyword_data = [
                # VINHO DE MESA keywords
                ('VINHO DE MESA', 'vinho de mesa', 1.0),
                ('VINHO DE MESA', 'tinto', 0.8),
                ('VINHO DE MESA', 'branco', 0.8),
                ('VINHO DE MESA', 'rosado', 0.8),
                
                # VINHO FINO DE MESA keywords
                ('VINHO FINO DE MESA (VINIFERA)', 'vinho fino', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'vinífera', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'vinifera', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'cabernet', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'merlot', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'tinto fino', 1.0),
                ('VINHO FINO DE MESA (VINIFERA)', 'branco fino', 1.0),
                
                # SUCO keywords
                ('SUCO', 'suco', 1.0),
                ('SUCO', 'suco de uva', 1.0),
                ('SUCO', 'néctar', 0.9),
                ('SUCO', 'integral', 0.8),
                ('SUCO', 'concentrado', 0.8),
                
                # DERIVADOS keywords
                ('DERIVADOS', 'derivados', 1.0),
                ('DERIVADOS', 'filtrado', 1.0),
                ('DERIVADOS', 'frisante', 1.0),
                ('DERIVADOS', 'vinho leve', 1.0),
                ('DERIVADOS', 'destilado', 1.0),
                ('DERIVADOS', 'bagaceira', 1.0),
                ('DERIVADOS', 'licor', 1.0),
                ('DERIVADOS', 'vinho composto', 1.0),
                ('DERIVADOS', 'pisco', 1.0),
                ('DERIVADOS', 'espumante', 1.0),
                ('DERIVADOS', 'mosto', 1.0),
                ('DERIVADOS', 'vinagre', 1.0),
                ('DERIVADOS', 'brandy', 1.0),
                ('DERIVADOS', 'jeropiga', 1.0),
                ('DERIVADOS', 'borra', 1.0),
                ('DERIVADOS', 'compostos', 1.0),
            ]
            
            for category, keyword, weight in keyword_data:
                # Get the category ID
                cursor.execute('SELECT id FROM cultivar_types WHERE name = ?', (category,))
                category_id = cursor.fetchone()[0]
                
                # Insert the keyword
                cursor.execute(
                    'INSERT OR IGNORE INTO keywords (cultivar_type_id, keyword, weight) VALUES (?, ?, ?)',
                    (category_id, keyword, weight)
                )
            
            # Commit and close
            conn.commit()
            conn.close()
            
            self.logger.info(f"Created SQLite database at {db_path} with default categories and keywords")
        except Exception as e:
            self.logger.error(f"Error creating SQLite database: {str(e)}")
            raise
    
    def classify_cultivar(self, text: str, category: str = None) -> Optional[str]:
        """
        Classify a text string to determine the cultivar type
        
        Args:
            text: Text to classify
            category: Optional category context for classification
            
        Returns:
            Classified cultivar type or None if unclassifiable
        """
        if not self.use_sqlite or not hasattr(self, 'sqlite_classifier') or self.sqlite_classifier is None:
            return None
            
        try:
            cultivar_type, confidence = self.sqlite_classifier.classify(text)
            
            # Only return if above confidence threshold
            if confidence > 0.5:
                return cultivar_type
            return None
        except Exception as e:
            self.logger.error(f"Error classifying cultivar: {str(e)}")
            return None
    
    def _build_display_name_mapping(self):
        """Build a reverse mapping from display names to subcategory keys"""
        for category, subcats in self.SUBCATEGORY_DISPLAY_NAMES.items():
            if category not in self.DISPLAY_TO_SUBCATEGORY:
                self.DISPLAY_TO_SUBCATEGORY[category] = {}
                
            for subcat_key, display_name in subcats.items():
                # Store both the exact match and a normalized version
                self.DISPLAY_TO_SUBCATEGORY[category][display_name] = subcat_key
                self.DISPLAY_TO_SUBCATEGORY[category][display_name.upper()] = subcat_key
                self.DISPLAY_TO_SUBCATEGORY[category][display_name.lower()] = subcat_key
                self.DISPLAY_TO_SUBCATEGORY[category][display_name.lower().replace(' ', '_')] = subcat_key
    
    def normalize_subcategory(self, category: str, subcategory: str) -> Optional[str]:
        """
        Normalize a subcategory name to the internal key format
        
        Args:
            category: The category name
            subcategory: The subcategory name (display format or internal format)
            
        Returns:
            The normalized subcategory key or None if not found
        """
        if not category or not subcategory:
            return None
            
        # If subcategory already matches an internal key, return it
        if category in self.SUBCATEGORY_MAPPING and subcategory in self.SUBCATEGORY_MAPPING[category]:
            return subcategory
            
        # Try to find the subcategory in the display name mapping
        if category in self.DISPLAY_TO_SUBCATEGORY:
            if subcategory in self.DISPLAY_TO_SUBCATEGORY[category]:
                return self.DISPLAY_TO_SUBCATEGORY[category][subcategory]
                
            # Try with normalized versions
            normalized = subcategory.lower()
            if normalized in self.DISPLAY_TO_SUBCATEGORY[category]:
                return self.DISPLAY_TO_SUBCATEGORY[category][normalized]
                
            normalized = subcategory.upper()
            if normalized in self.DISPLAY_TO_SUBCATEGORY[category]:
                return self.DISPLAY_TO_SUBCATEGORY[category][normalized]
                
            normalized = subcategory.lower().replace(' ', '_')
            if normalized in self.DISPLAY_TO_SUBCATEGORY[category]:
                return self.DISPLAY_TO_SUBCATEGORY[category][normalized]
        
        # Check for partial matches
        if category in self.DISPLAY_TO_SUBCATEGORY:
            for display_name, subcat_key in self.DISPLAY_TO_SUBCATEGORY[category].items():
                if subcategory.lower() in display_name.lower() or display_name.lower() in subcategory.lower():
                    return subcat_key
        
        # Check aliases for case-insensitive and format variations
        if category in self.SUBCATEGORY_ALIASES:
            alias_key = subcategory.upper().replace(' ', '_')
            if alias_key in self.SUBCATEGORY_ALIASES[category]:
                return self.SUBCATEGORY_ALIASES[category][alias_key]
        
        return None
    
    def get_category_params(self, category: str, subcategory: Optional[str] = None) -> Dict[str, str]:
        """
        Get the appropriate parameters for a category and subcategory
        
        Args:
            category: Main data category
            subcategory: Optional subcategory
            
        Returns:
            Dictionary with parameters for the HTTP request
        """
        # Inicializa o dicionário de parâmetros
        params = {}
        
        # Adiciona o parâmetro 'ano' por padrão. Este valor será substituído posteriormente 
        # pela data específica da requisição, mas definimos aqui para garantir a ordem correta
        # Importante: o site espera primeiro 'ano' e depois 'opcao'
        params['ano'] = str(datetime.now().year - 1)  # Valor padrão é o ano anterior
        
        # Adiciona o parâmetro 'opcao' baseado na categoria
        if category.lower() in self.CATEGORY_MAPPING:
            params['opcao'] = self.CATEGORY_MAPPING[category.lower()]
        else:
            # Fallback para o comportamento anterior se a categoria não estiver no mapeamento
            params['visao'] = category.lower()
        
        # Adiciona o parâmetro de subcategoria, se fornecido
        if subcategory and category.lower() in self.SUBCATEGORY_MAPPING:
            # Normaliza a subcategoria
            subcategory_key = self.normalize_subcategory(category, subcategory)
            
            if subcategory_key in self.SUBCATEGORY_MAPPING[category.lower()]:
                # Adiciona o parâmetro 'subopcao' baseado na subcategoria
                params['subopcao'] = self.SUBCATEGORY_MAPPING[category.lower()][subcategory_key]
        
        return params
    
    def get_subcategory_key(self, category: str, subcategory: str) -> Optional[str]:
        """
        Get the normalized key for a subcategory, handling aliases and case variations
        
        Args:
            category: Main data category
            subcategory: Subcategory name that might have variations
            
        Returns:
            Normalized subcategory key or None if not found
        """
        # Check if subcategory is None or empty
        if not subcategory:
            return None
            
        # Normalize the input by converting to uppercase and removing extra spaces
        normalized_subcategory = subcategory.upper().strip()
        
        # First, check if the normalized subcategory exists in the aliases dictionary
        if category in self.SUBCATEGORY_ALIASES and normalized_subcategory in self.SUBCATEGORY_ALIASES[category]:
            return self.SUBCATEGORY_ALIASES[category][normalized_subcategory]
        
        # Special case for "VINHO DE MESA" in producao category
        if category.lower() == 'producao' and any(term in normalized_subcategory for term in ['VINHO', 'MESA']):
            if 'VINHO' in normalized_subcategory and 'MESA' in normalized_subcategory:
                return 'vinho_mesa'
            if normalized_subcategory == 'VINHO':
                return 'vinho_mesa'
        
        # Check for direct matches in the subcategory mapping
        if category in self.SUBCATEGORY_MAPPING:
            for key in self.SUBCATEGORY_MAPPING[category].keys():
                if key.lower() == subcategory.lower():
                    return key
        
        # Try to find a partial match
        if category in self.SUBCATEGORY_DISPLAY_NAMES:
            for display_key, display_value in self.SUBCATEGORY_DISPLAY_NAMES[category].items():
                if display_value.upper() == normalized_subcategory:
                    return display_key
        
        # If no match found, return the original with spaces replaced by underscores
        # This helps with new subcategories that might be added in the future
        return normalized_subcategory.lower().replace(' ', '_')
    
    def get_display_name(self, category: str, subcategory: str) -> str:
        """
        Get the display name for a subcategory
        
        Args:
            category: Main category name
            subcategory: Subcategory name
            
        Returns:
            Human-readable display name for the subcategory
        """
        if category in self.SUBCATEGORY_DISPLAY_NAMES and subcategory in self.SUBCATEGORY_DISPLAY_NAMES[category]:
            return self.SUBCATEGORY_DISPLAY_NAMES[category][subcategory]
        return subcategory.replace('_', ' ').title()
    
    def detect_schema_changes(self, url: str, html_content: str) -> bool:
        """
        Detect changes in HTML structure by comparing hash with last known hash
        
        Args:
            url: URL of the page
            html_content: HTML content to check
            
        Returns:
            bool: True if a change was detected, False otherwise
        """
        # Use the imported function from html_parser.py, passing self.last_known_hash
        return detect_schema_changes(url, html_content, self.last_known_hash)
    
    def update_parsing_strategy(self, url: str, html_content: str) -> None:
        """
        Update the parsing strategy when a schema change is detected
        
        Args:
            url: URL of the page
            html_content: HTML content to analyze
        """
        # Use the imported function from html_parser.py
        update_parsing_strategy(url, html_content)
    
    def extract_table_data(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Extract tabular data from HTML content
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            List of dictionaries containing the extracted data
        """
        return extract_table_data(html_content)
    
    def fetch_csv_fallback(self, category: Optional[str], subcategory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch fallback data from CSV files when scraping fails.
        
        Args:
            category: Main data category
            subcategory: Data subcategory (optional)
            
        Returns:
            List of data dictionaries from CSV or empty list if no fallback found
        """
        # Return empty list if category is None - this is an expected case, not an error
        if category is None:
            return []
            
        # Try to find an appropriate CSV file based on category/subcategory
        csv_mapping = self.get_csv_mapping()
        
        # Look for exact match with category and subcategory
        if subcategory:
            key = f"{category}_{subcategory}"
            if key in csv_mapping:
                return self._load_csv_fallback_data(csv_mapping[key])
                
        # If no match with subcategory or subcategory is None, try just the category
        if category in csv_mapping:
            return self._load_csv_fallback_data(csv_mapping[category])
            
        # If nothing matched, check if we have a generic fallback
        if "default" in csv_mapping:
            self.logger.debug(f"Using default fallback for {category}" + 
                            (f" / {subcategory}" if subcategory else ""))
            return self._load_csv_fallback_data(csv_mapping["default"])
            
        # No fallback found - log at debug level since this can be a normal situation
        self.logger.debug(f"No CSV fallback found for {category}" + 
                        (f" / {subcategory}" if subcategory else ""))
        return []
    
    def scrape_with_pagination(self, url: str, category: Optional[str] = None, subcategory: Optional[str] = None, 
                                max_pages: int = 10, parser_config: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Scrape data from a paginated source, with fallback to CSV if needed.
        
        Args:
            url: Base URL to scrape
            category: Data category (for CSV fallback)
            subcategory: Data subcategory (for CSV fallback)
            max_pages: Maximum number of pages to scrape
            parser_config: Configuration for the HTML parser
            
        Returns:
            List of dictionaries containing the scraped data
        """
        all_data = []
        fallback_used = False
        
        try:
            current_page = 1
            while current_page <= max_pages:
                # Format URL for current page if needed
                page_url = url
                if '{page}' in url:
                    page_url = url.format(page=current_page)
                    
                self.logger.info(f"Scraping page {current_page} from: {page_url}")
                
                # Get HTML content
                response = self.session.get(page_url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                
                # Save raw HTML content
                if hasattr(self, 'raw_html'):
                    self.raw_html[f"page_{current_page}"] = response.text
                    
                # Parse the HTML
                page_data = extract_table_data(response.text)
                
                # Process numeric fields
                numeric_fields = [
                    'Quantidade (L.)', 'Quantidade', 'Valor', 
                    'Volume', 'Quantidade (Kg)', 'valor'
                ]
                
                for item in page_data:
                    for field in numeric_fields:
                        if field in item and item[field]:
                            original, numeric = self._extract_single_value(item[field])
                            item[field] = numeric
                            item[f"{field}_original"] = original

                # Add valid data
                if page_data:
                    all_data.extend(page_data)
                    self.logger.debug(f"Added {len(page_data)} records from page {current_page}")
                    
                    # Check item count for pagination
                    if len(page_data) < self.expected_items_per_page:
                        self.logger.info(f"Page {current_page} has fewer items than expected, stopping")
                        break
                        
                else:
                    self.logger.info(f"No data found on page {current_page}, stopping")
                    break
                    
                current_page += 1  # Movido para fora do bloco else
                time.sleep(self.delay_between_requests)

        except Exception as e:
            self.logger.warning(f"Error scraping data: {str(e)}")
            
            if category:
                self.logger.info(f"Attempting CSV fallback for {category}/{subcategory}")
                fallback_data = self.fetch_csv_fallback(category, subcategory)
                
                if fallback_data:
                    self.logger.info(f"Using {len(fallback_data)} fallback records")
                    all_data = fallback_data
                    fallback_used = True
        
        # Apply SQLite classification to improve categorization if available
        if self.use_sqlite and self.sqlite_classifier is not None and all_data:
            self.logger.info(f"Applying SQLite classification to {len(all_data)} records")
            self._apply_sqlite_classification(all_data, category, subcategory)
            
        # Remove duplicate entries from the data
        all_data = self._remove_duplicates(all_data)
        
        return all_data
    
    def _apply_sqlite_classification(self, data: List[Dict[str, Any]], category: str, subcategory: Optional[str] = None) -> None:
        """
        Apply SQLite-based classification to improve categorization of products
        
        Args:
            data: List of data items to classify
            category: Main category 
            subcategory: Optional subcategory
        """
        if not self.sqlite_classifier:
            self.logger.warning("SQLite classifier not available for classification")
            return
            
        try:
            self.logger.info(f"Starting SQLite-based classification for {len(data)} records")
            classified_count = 0
            
            for item in data:
                # Skip if item has no product name
                product_name = None
                for field in ["Produto", "produto", "Descrição", "descricao", "item", "Nome"]:
                    if field in item and item[field]:
                        product_name = str(item[field])
                        break
                        
                if not product_name:
                    continue
                    
                # Skip if it's a category header (all uppercase)
                if product_name.isupper() and len(product_name.split()) > 1:
                    continue
                    
                # Try to classify using SQLite
                classification, confidence = self.sqlite_classifier.classify(product_name)
                
                # Only use classification if confidence is above threshold
                if confidence > 0.5:
                    # Don't overwrite category headers
                    if item.get("categoria_principal") is not True:
                        # Category must be 'producao' for these categorizations to apply
                        if category == 'producao':
                            item['subcategoria'] = classification
                            classified_count += 1
                            
                            # Update the complete product name if needed
                            if product_name.lower() in ["tinto", "branco", "rosado"]:
                                if classification == "VINHO FINO DE MESA (VINIFERA)":
                                    item["produto_completo"] = f"{product_name} (Viníferas)"
                                elif classification == "VINHO DE MESA":
                                    item["produto_completo"] = f"{product_name} (Mesa)"
                                else:
                                    item["produto_completo"] = product_name
            
            self.logger.info(f"SQLite classification applied successfully to {classified_count} records")
        except Exception as e:
            self.logger.error(f"Error during SQLite classification: {str(e)}")
    
    def _remove_duplicates(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate entries from data based on product name and subcategory
        """
        unique_items = {}
        
        for item in data:
            # Cria chave única
            product = item.get('Produto') or item.get('produto') or ''
            subcategory = item.get('subcategoria', '')
            year = item.get('ano', '')
            key = f"{product}_{subcategory}_{year}".lower()

            # Comparação usando valor numérico
            current_quantity = item.get('Quantidade (L.)', 0)
            
            if key in unique_items:
                existing_quantity = unique_items[key].get('Quantidade (L.)', 0)
                
                # Mantém o item com maior quantidade
                if current_quantity > existing_quantity:
                    unique_items[key] = item
            else:
                unique_items[key] = item
                
        return list(unique_items.values())
    
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
        destination: Optional[str] = None,
        normalize_names: bool = True
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
            
        # Add the year parameter - use the end_year as the default year filter
        # This is important to speed up requests
        additional_filters['ano'] = str(end_year)
            
        url_params.update(additional_filters)
        
        self.logger.info(f'Scraping category: {category}, subcategory: {subcategory or "all"}')
        
        # Build query string without year (for metadata)
        query_string = '&'.join([f'{k}={v}' for k, v in url_params.items()])
        base_url = f'{self.base_url}?{query_string}'
        
        # Initialize raw_html attribute
        self.raw_html = {}
        
        # Pass the properly formatted URL string to scrape_with_pagination, not the parameter dictionary
        data = self.scrape_with_pagination(base_url, category, subcategory)
        
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
        
        # Get display name for subcategory if available
        subcategory_display = self.get_display_name(category, subcategory) if subcategory else None
        
        return ScrapedData(
            source_url=base_url,
            timestamp=time.time(),
            data=data,
            metadata={
                'category': category,
                'subcategory': subcategory,
                'subcategory_display': subcategory_display,
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
    
    def fetch_html_data(self, category: str, subcategory: Optional[str] = None, year: Optional[int] = None) -> str:
        """
        Fetch HTML data from the Embrapa data portal
        
        Args:
            category: Main data category
            subcategory: Optional subcategory
            year: Optional year for filtering
            
        Returns:
            HTML content as string
        """
        url = self.base_url
        
        # Get the appropriate parameters based on category and subcategory
        params = self.get_category_params(category, subcategory)
        
        # Add year parameter if provided
        if year:
            params['ano'] = str(year)
            
        # Add a cache key for this request
        cache_key = f"html_{category}_{subcategory or 'all'}_{year or 'all'}"
        
        # Check if we have this data in cache
        # Implementação alternativa sem depender de cache_service
        try:
            self.logger.info(f"Fetching {category}/{subcategory} data from Embrapa portal")
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            html_content = response.text
            return html_content
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            raise Exception(f"Failed to fetch data from Embrapa portal: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise Exception(f"Unexpected error fetching data: {str(e)}")
    
    def scrape_fallback_for_year(self, category: str, year: int, subcategory: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get fallback data for a specific year from CSV files
        
        Args:
            category: Data category
            year: Target year to retrieve
            subcategory: Optional subcategory
            
        Returns:
            List of dictionaries containing the fallback data for the specified year
        """
        # First, check if we already have fallback data loaded
        if not hasattr(self, '_fallback_data') or category not in self._fallback_data:
            self._fallback_data = {} if not hasattr(self, '_fallback_data') else self._fallback_data
            self._fallback_data[category] = self.fetch_csv_fallback(category, subcategory)
        
        # Filter the fallback data for the specified year
        year_data = [
            record for record in self._fallback_data[category] 
            if 'ano' in record and record['ano'] == year
        ]
        
        # Additionally, filter by subcategory if specified
        if subcategory and year_data:
            # Handle different naming conventions in CSV files
            subcategory_keys = ['subcategoria', 'categoria', 'tipo', 'subtipo']
            subcategory_data = []
            
            # Try different possible subcategory field names
            for record in year_data:
                for key in subcategory_keys:
                    if key in record and str(record[key]).lower() == subcategory.lower():
                        subcategory_data.append(record)
                        break
            
            if subcategory_data:
                year_data = subcategory_data
        
        if not year_data:
            self.logger.warning(f"No CSV fallback data available for year {year} and category {category}" + 
                                (f", subcategory {subcategory}" if subcategory else ""))
        else:
            self.logger.info(f"Found {len(year_data)} records in CSV fallback for year {year}, category {category}" +
                             (f", subcategory {subcategory}" if subcategory else ""))
            
        return year_data
    
    def scrape_year(self, category: str, subcategory: Optional[str] = None, year: Optional[int] = None, 
                    tipo: Optional[str] = None, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Scrape data for a specific year
        
        Args:
            category: Main data category (producao, comercializacao, etc.)
            subcategory: Optional subcategory
            year: Specific year to scrape
            tipo: Optional type filter (e.g., "Branco", "Tinto", "Rosado")
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of dictionaries containing the scraped data
        """
        # If no year is specified, use the most recent year
        if year is None:
            year = datetime.now().year - 1  # Previous year's data is usually available
        
        # Construct the URL based on the category, subcategory, and year
        url = self._construct_url(category, subcategory, year, tipo)
        self.logger.info(f"Scraping data for year {year} with URL: {url}")
        
        # Try to scrape the data with retries
        scraped_data = None
        for attempt in range(max_retries):
            try:
                soup = self._fetch_page(url)
                scraped_data = self._parse_table(soup, category)
                if scraped_data and len(scraped_data) > 0:
                    # Add year to each record if it's not already present
                    for record in scraped_data:
                        if 'ano' not in record:
                            record['ano'] = year
                    break
                else:
                    self.logger.warning(f"Received empty data for year {year}, will try fallback")
                    time.sleep(1)  # Wait before retrying
            except Exception as e:
                self.logger.error(f"Error scraping data for year {year}, attempt {attempt+1}: {str(e)}")
                time.sleep(1)  # Wait before retrying
        
        # If scraping failed, try to use fallback data for this specific year
        if not scraped_data or len(scraped_data) == 0:
            self.logger.error(f"Failed to scrape {url} after {max_retries} attempts")
            
            # Create a key for this category/subcategory combination
            cache_key = f"{category}_{subcategory or 'all'}"
            
            # Check if we have fallback data for this specific year
            fallback_for_year = None
            
            # If we have fallback years information stored
            if hasattr(self, '_fallback_years') and cache_key in self._fallback_years:
                # Check if the requested year is in the available fallback years
                if year in self._fallback_years[cache_key]:
                    # Try to get fallback data for this specific year
                    fallback_data = self.fetch_csv_fallback(category, subcategory)
                    if fallback_data:
                        # Filter the fallback data for the specific year
                        fallback_for_year = [record for record in fallback_data if record.get('ano') == year]
                        if fallback_for_year:
                            self.logger.info(f"Using {len(fallback_for_year)} records from CSV fallback for year {year}")
                            return fallback_for_year
            
            # If we don't have specific fallback data for this year
            if not fallback_for_year:
                self.logger.warning(f"No CSV fallback data available for year {year}")
            
            # Return any scraped data we might have (likely empty)
            return scraped_data or []
        
        return scraped_data
    
    def close(self):
        """
        Close all resources (database connections, etc.)
        """
        if hasattr(self, 'sqlite_classifier') and self.sqlite_classifier is not None:
            try:
                self.sqlite_classifier.close()
                self.logger.info("SQLite classifier connection closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing SQLite classifier connection: {str(e)}")
            finally:
                self.sqlite_classifier = None
    
    def __del__(self):
        """
        Destructor to ensure resources are properly released
        """
        # Only call close() if not already closed
        if hasattr(self, 'sqlite_classifier') and self.sqlite_classifier is not None:
            self.close()
    
    def get_csv_mapping(self) -> Dict[str, str]:
        """
        Get mapping of categories and subcategories to CSV fallback files
        
        Returns:
            Dictionary with category/subcategory keys mapped to CSV file paths
        """
        # Create a mapping from category/subcategory to CSV fallback files
        csv_mapping = {
            # Producao category
            "producao": "app/data/Producao.csv",
            
            # Processamento subcategories
            "processamento_viniferas": "app/data/ProcessaViniferas.csv",
            "processamento_americanas": "app/data/ProcessaAmericanas.csv",
            "processamento_mesa": "app/data/ProcessaMesa.csv",
            "processamento_semclassificacao": "app/data/ProcessaSemclass.csv",
            
            # Importacao subcategories
            "importacao_vinhos": "app/data/ImpVinhos.csv",
            "importacao_espumantes": "app/data/ImpEspumantes.csv",
            "importacao_sucos": "app/data/ImpSuco.csv",
            "importacao_passas": "app/data/ImpPassas.csv",
            "importacao_frescas": "app/data/ImpFrescas.csv",
            
            # Exportacao subcategories
            "exportacao_vinhos": "app/data/ExpVinho.csv",
            "exportacao_espumantes": "app/data/ExpEspumantes.csv",
            "exportacao_sucos": "app/data/ExpSuco.csv",
            "exportacao_uvas": "app/data/ExpUva.csv",
            
            # Generic comercializacao category
            "comercializacao": "app/data/Comercio.csv",
            
            # Default fallback (used when no specific match found)
            "default": "app/data/Producao.csv"
        }
        
        return csv_mapping
    
    def _load_csv_fallback_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load data from CSV fallback file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List of dictionaries containing the CSV data
        """
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"Fallback CSV file not found: {file_path}")
                return []
                
            # Read CSV into pandas DataFrame
            df = pd.read_csv(file_path, encoding='utf-8', sep=';')
            
            # Convert to list of dictionaries
            records = df.to_dict(orient='records')
            
            # Process the records to ensure proper data format
            for record in records:
                # Convert any NaN values to None
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                        
                # Convert numeric strings to proper types
                for key, value in record.items():
                    if isinstance(value, str):
                        # Try to convert string to numeric if it looks like a number
                        try:
                            if ',' in value and '.' not in value:
                                value = value.replace(',', '.')
                            float_val = float(value)
                            # If it's a whole number, convert to int
                            if float_val.is_integer():
                                record[key] = int(float_val)
                            else:
                                record[key] = float_val
                        except (ValueError, TypeError):
                            # Keep as string if conversion fails
                            pass
            
            self.logger.info(f"Loaded {len(records)} records from fallback CSV: {file_path}")
            return records
            
        except Exception as e:
            self.logger.error(f"Error loading fallback CSV data from {file_path}: {str(e)}")
            return []
        
    def _extract_single_value(self, value_str: str) -> Tuple[str, float]:
        """
        Extract a numeric value from a string, handling special cases like dashes.
        
        Args:
            value_str: The string value to convert to a number
            
        Returns:
            Tuple containing (original_string, numeric_value)
        """
        # Keep the original value for reference
        original = str(value_str).strip()
        
        # Handle dash character case
        if original == '-' or original == '–' or original == '—':
            return original, 0.0
        
        try:
            # Remove any thousand separators and replace comma with dot for decimal
            cleaned = str(value_str).replace('.', '').replace(',', '.')
            # Convert to float
            return original, float(cleaned)
        except (ValueError, TypeError):
            # If conversion fails, return 0.0 as the numeric value
            self.logger.debug(f"Could not convert value '{value_str}' to float, using 0.0")
            return original, 0.0
