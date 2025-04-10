# -*- coding: utf-8 -*-
import hashlib
import logging
import time
from typing import Dict, List, Optional, Union, Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pydantic import BaseModel


class ScrapedData(BaseModel):
    """Model for scraped data with metadata"""
    source_url: str
    timestamp: float
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]


class AdaptiveScraper:
    """
    Scraper that can adapt to changes in HTML structure
    and extract data from VitiBrasil portal
    """
    
    # VitiBrasil API category mappings
    CATEGORY_MAPPING = {
        'producao': 'opt_02',
        'processamento': 'opt_03',
        'comercializacao': 'opt_04',
        'importacao': 'opt_05',
        'exportacao': 'opt_06',
    }
    
    # Subcategory mappings for each category
    SUBCATEGORY_MAPPING = {
        'producao': {
            'viniferas': 'subopt_01',  # Uvas viníferas processadas
            'americanas_hibridas': 'subopt_02',  # Uvas americanas e híbridas
            'vinhos_mesa': 'subopt_03',  # Produção de vinhos de mesa
            'sucos': 'subopt_04',  # Produção de sucos
        },
        'processamento': {
            'por_tipo': 'subopt_01',  # Quantidade processada por tipo
            'por_regiao': 'subopt_02',  # Processamento por região
            'controle_qualidade': 'subopt_03',  # Controle de qualidade
        },
        'comercializacao': {
            'varejo': 'subopt_01',  # Varejo tradicional
            'grandes_redes': 'subopt_02',  # Grandes redes
            'exportacao_indireta': 'subopt_03',  # Exportação indireta
        },
        'importacao': {
            'vinhos_finos': 'subopt_01',  # Vinhos finos
            'espumantes': 'subopt_02',  # Espumantes
            'uvas_frescas': 'subopt_03',  # Uvas frescas
            'uvas_passas': 'subopt_04',  # Uvas passas
            'sucos': 'subopt_05',  # Sucos
        },
        'exportacao': {
            'vinhos_mesa': 'subopt_01',  # Vinhos de mesa
            'espumantes': 'subopt_02',  # Espumantes
            'uvas_frescas': 'subopt_03',  # Uvas frescas
            'sucos': 'subopt_04',  # Sucos
        }
    }
    
    def __init__(self, base_url: str = 'http://vitibrasil.cnpuv.embrapa.br/index.php'):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.last_known_hash: Dict[str, str] = {}
        
        # Set up session with retry strategy
        self.session = requests.Session()
        self.retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST']
        )
        self.session.mount('http://', HTTPAdapter(max_retries=self.retry_strategy))
        self.session.mount('https://', HTTPAdapter(max_retries=self.retry_strategy))
    
    def detect_schema_changes(self, url: str, html_content: str) -> bool:
        """
        Detect changes in HTML structure by comparing hash with last known hash
        
        Args:
            url: URL of the page
            html_content: HTML content to check
            
        Returns:
            bool: True if a change was detected, False otherwise
        """
        current_hash = hashlib.md5(html_content.encode()).hexdigest()
        
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
        
        # TODO: Implement NLP analysis of column names for dynamic mapping
    
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
        
        # The main content table is usually in a specific div
        main_content = soup.find('div', {'class': 'main-content'}) or soup
        
        tables = main_content.find_all('table')
        for table in tables:
            table_data = []
            headers = []
            
            # Extract headers
            header_row = table.find('tr')
            if header_row:
                headers = [th.text.strip() for th in header_row.find_all(['th', 'td'])]
            
            # Extract data rows
            data_rows = table.find_all('tr')[1:] if headers else table.find_all('tr')
            for row in data_rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = {headers[i] if i < len(headers) else f'column_{i}': 
                                cell.text.strip() for i, cell in enumerate(cells)}
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
        
        for year in range(start_year, end_year + 1):
            # Add year to URL parameters
            year_params = {**url_params, 'ano': str(year)}
            
            # Build query string
            query_string = '&'.join([f'{k}={v}' for k, v in year_params.items()])
            year_url = f'{self.base_url}?{query_string}'
            
            self.logger.info(f'Scraping data for year {year} with URL: {year_url}')
            
            try:
                response = self.session.get(year_url, timeout=30)
                response.raise_for_status()
                
                self.detect_schema_changes(year_url, response.text)
                year_data = self.extract_table_data(response.text)
                
                # Add year as metadata
                for item in year_data:
                    item['ano'] = year
                
                all_data.extend(year_data)
                
                # Avoid rate limiting
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f'Error scraping {year_url}: {str(e)}')
        
        return all_data
    
    def get_category_params(self, category: str, subcategory: Optional[str] = None) -> Dict[str, str]:
        """
        Get URL parameters for a specific category and subcategory
        
        Args:
            category: Main category name
            subcategory: Optional subcategory name
            
        Returns:
            Dictionary of URL parameters
        """
        if category not in self.CATEGORY_MAPPING:
            raise ValueError(f'Unknown category: {category}')
            
        params = {'opcao': self.CATEGORY_MAPPING[category]}
        
        if subcategory:
            if subcategory not in self.SUBCATEGORY_MAPPING.get(category, {}):
                raise ValueError(f'Unknown subcategory {subcategory} for category {category}')
                
            params['subopcao'] = self.SUBCATEGORY_MAPPING[category][subcategory]
        
        return params
    
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
        
        data = self.scrape_with_pagination(url_params, start_year, end_year)
        
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
                'filters': {
                    'region': region,
                    'product_type': product_type,
                    'origin': origin,
                    'destination': destination
                }
            }
        )