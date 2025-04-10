# -*- coding: utf-8 -*-
import hashlib
import logging
import time
from typing import Dict, List, Optional, Union, Any

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
        self.error_count: Dict[str, int] = {}
        self.max_retries = 3
        
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
        
        # Initialize raw_html attribute
        self.raw_html = {}
        
        data = self.scrape_with_pagination(url_params, start_year, end_year)
        
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
                }
            },
            raw_html=sample_html
        )