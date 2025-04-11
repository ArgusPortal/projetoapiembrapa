import logging
from typing import Dict, List, Any, Optional, Union
import math
import numpy as np
import pandas as pd
import re
from bs4 import BeautifulSoup

from app.services.scraper.adaptive_scraper import AdaptiveScraper, ScrapedData
from app.services.cache_service import data_cache
from app.core.config import settings

logger = logging.getLogger(__name__)


class ViniDataService:
    """
    Service for retrieving and transforming data from VitiBrasil
    with caching and fallback mechanisms
    """
    
    def __init__(self):
        self.scraper = AdaptiveScraper(base_url=settings.VITIBRASIL_BASE_URL)
        self.fallback_files = {
            'producao': 'Producao.csv',
            'processamento': {
                'default': 'ProcessaViniferas.csv',
                'viniferas': 'ProcessaViniferas.csv',
                'americanas': 'ProcessaAmericanas.csv',
                'mesa': 'ProcessaMesa.csv',
                'semclassificacao': 'ProcessaSemclass.csv',
            },
            'comercializacao': 'Comercio.csv',
            'importacao': {
                'default': 'ImpVinhos.csv',
                'vinhos': 'ImpVinhos.csv',
                'sucos': 'ImpSuco.csv',
                'espumantes': 'ImpEspumantes.csv',
                'passas': 'ImpPassas.csv',
                'frescas': 'ImpFrescas.csv',
            },
            'exportacao': {
                'default': 'ExpVinho.csv',
                'vinhos': 'ExpVinho.csv',
                'sucos': 'ExpSuco.csv',
                'espumantes': 'ExpEspumantes.csv',
                'uvas': 'ExpUva.csv',
            }
        }
    
    def get_data(
        self,
        category: str,
        start_year: int = 1970,
        end_year: int = 2025,
        region: Optional[str] = None,
        product_type: Optional[str] = None,
        subcategory: Optional[str] = None,
        channel: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get data with resilient caching and filtering
        
        Args:
            category: Data category (producao, processamento, comercializacao, etc.)
            start_year: First year to include
            end_year: Last year to include
            region: Optional region filter
            product_type: Optional product type filter
            subcategory: Optional subcategory name
            channel: Optional channel filter (for comercializacao)
            origin: Optional origin country/region (for imports)
            destination: Optional destination country/region (for exports)
            
        Returns:
            Dictionary with data and metadata
        """
        # Map subcategory based on category and product_type if not explicitly provided
        if not subcategory and product_type:
            subcategory = self._map_product_type_to_subcategory(category, product_type)
        
        # Create a unique cache key based on all filters
        cache_key = f"{category}"
        if subcategory:
            cache_key += f"_{subcategory}"
        cache_key += f"_{start_year}_{end_year}"
        
        if any([region, product_type, channel, origin, destination]):
            filter_key = "_filters_"
            if region:
                filter_key += f"r{region}"
            if product_type:
                filter_key += f"p{product_type}"
            if channel:
                filter_key += f"c{channel}"
            if origin:
                filter_key += f"o{origin}"
            if destination:
                filter_key += f"d{destination}"
            cache_key += filter_key

        # Attempt to get cached data first
        cached_data = data_cache.get(cache_key)
        
        # Define data fetching function for cache
        def fetch_data():
            logger.info(f"Fetching fresh data for category: {category}, subcategory: {subcategory or 'all'}")
            try:
                scraped_data = self.scraper.scrape_category(
                    category=category,
                    subcategory=subcategory,
                    start_year=start_year,
                    end_year=end_year,
                    region=region,
                    product_type=product_type,
                    origin=origin,
                    destination=destination
                )
                
                # Validate data structure
                if not self._validate_scraped_data(scraped_data):
                    logger.warning(f"Invalid data structure detected for {category}, attempting recovery")
                    recovered_data = self._attempt_data_recovery(scraped_data)
                    if recovered_data:
                        return recovered_data.dict()
                    # If recovery fails, raise exception to trigger fallback
                    raise ValueError("Data validation failed and recovery was unsuccessful")
                
                return scraped_data.dict()
            
            except Exception as e:
                logger.error(f"Error scraping data: {str(e)}")
                # Return a failed indicator to trigger fallback
                return None
        
        # Try to get from cache, fallback to fresh data
        result = cached_data or fetch_data()
        
        # All attempts failed, try local CSV fallback
        if not result:
            logger.warning(f"Online data retrieval failed for {category}, trying fallback files")
            result = self._load_fallback_data(category, subcategory)
            
            # If fallback with subcategory failed, try the default fallback
            if not result and subcategory:
                logger.warning(f"Fallback with subcategory {subcategory} failed, trying default fallback")
                result = self._load_fallback_data(category)
        
        # If still no data, return an error
        if not result:
            logger.error(f"All data retrieval methods failed for {category}")
            return {"error": "Data retrieval failed", "data": [], "fallback_used": True}
        
        # Store origin of the data
        data_source = "cache" if cached_data else ("online" if not result.get("fallback_used") else "fallback_file")
        
        # Apply filters if necessary
        filtered_data = self._filter_data(
            result.get("data", []), 
            start_year=start_year,
            end_year=end_year,
            region=region, 
            product_type=product_type,
            channel=channel,
            origin=origin,
            destination=destination
        )
        
        # Tenta detectar a subcategoria automaticamente se ela não foi especificada
        # e os dados não têm uma subcategoria definida
        if not subcategory and not result.get("metadata", {}).get("subcategory"):
            detected_subcategory = self.detect_subcategory_from_data(category, filtered_data)
            
            # Se detectamos uma subcategoria, atualiza os metadados
            if detected_subcategory:
                logger.info(f"Detectada subcategoria automaticamente: {detected_subcategory} para {category}")
                if "metadata" in result:
                    result["metadata"]["subcategory"] = detected_subcategory
                    # Adiciona um campo para indicar que a subcategoria foi detectada automaticamente
                    result["metadata"]["subcategory_detection"] = "automatic"
        
        # Sanitize data for JSON serialization
        sanitized_data = self._sanitize_for_json(filtered_data)
        
        return {
            "metadata": result.get("metadata", {}),
            "data": sanitized_data,
            "from_cache": cached_data is not None,
            "data_source": data_source,
            "total_records": len(sanitized_data)
        }
    
    def _validate_scraped_data(self, scraped_data: ScrapedData) -> bool:
        """
        Validate that the scraped data has the expected structure
        
        Args:
            scraped_data: The scraped data to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check if data is a list as expected
        if not isinstance(scraped_data.data, list):
            return False
            
        # Empty list is valid but suspicious for most queries
        if len(scraped_data.data) == 0:
            logger.warning("Scraped data returned an empty list")
            return True
            
        # Check that each item is a dictionary
        if not all(isinstance(item, dict) for item in scraped_data.data):
            return False
            
        # Check that we have reasonable keys in each dictionary
        # At minimum each record should have some key besides 'ano'
        has_valid_keys = False
        for item in scraped_data.data[:10]:  # Check first 10 items
            if len(item.keys()) > 1:
                has_valid_keys = True
                break
                
        return has_valid_keys
    
    def _attempt_data_recovery(self, scraped_data: ScrapedData) -> Optional[ScrapedData]:
        """
        Attempt to recover data from potentially malformed scraped content
        
        Args:
            scraped_data: The scraped data to recover
            
        Returns:
            Recovered ScrapedData or None if recovery failed
        """
        # First, check if this is raw HTML rather than parsed data
        if hasattr(scraped_data, 'raw_html'):
            logger.info("Attempting to parse raw HTML directly")
            try:
                html_content = scraped_data.raw_html
                recovered_data = self._parse_raw_html(html_content, scraped_data.source_url)
                
                if recovered_data and len(recovered_data) > 0:
                    # Return recovered data in the expected format
                    return ScrapedData(
                        source_url=scraped_data.source_url,
                        timestamp=scraped_data.timestamp,
                        data=recovered_data,
                        metadata=scraped_data.metadata
                    )
            except Exception as e:
                logger.error(f"Recovery parsing failed: {str(e)}")
        
        return None
    
    def _parse_raw_html(self, html_content: str, source_url: str) -> List[Dict[str, Any]]:
        """
        Parse raw HTML from failed scraping attempts
        
        Args:
            html_content: Raw HTML content
            source_url: The source URL
            
        Returns:
            List of dictionaries extracted from the HTML
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        # Extract year from URL if present
        year_match = re.search(r'ano=(\d{4})', source_url)
        year = int(year_match.group(1)) if year_match else None
        
        # Find tables
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables in HTML")
        
        for table in tables:
            # Extract headers
            header_row = table.find('tr')
            if not header_row:
                continue
                
            headers = []
            for th in header_row.find_all(['th', 'td']):
                header_text = th.text.strip()
                if header_text:
                    headers.append(header_text)
                else:
                    headers.append(f"column_{len(headers)}")
            
            if not headers:
                continue
                
            # Extract data rows
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['td', 'th'])
                
                if len(cells) == 0:
                    continue
                    
                # Create record
                record = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        key = headers[i]
                        value = cell.text.strip()
                        record[key] = value
                
                # Add year if we found it
                if year and 'ano' not in record:
                    record['ano'] = year
                    
                if record:
                    results.append(record)
        
        return results
    
    def _map_product_type_to_subcategory(self, category: str, product_type: str) -> Optional[str]:
        """
        Map product type to appropriate subcategory based on the category
        
        Args:
            category: The data category
            product_type: The product type
            
        Returns:
            Mapped subcategory name or None
        """
        mapping = {
            'processamento': {
                'vinifera': 'viniferas',
                'viniferas': 'viniferas',
                'americana': 'americanas',
                'americanas': 'americanas',
                'mesa': 'mesa',
            },
            'importacao': {
                'vinho': 'vinhos',
                'vinhos': 'vinhos',
                'suco': 'sucos',
                'sucos': 'sucos',
                'espumante': 'espumantes',
                'espumantes': 'espumantes',
                'passa': 'passas',
                'passas': 'passas',
                'fresca': 'frescas',
                'frescas': 'frescas',
                'uvas frescas': 'frescas',
            },
            'exportacao': {
                'vinho': 'vinhos',
                'vinhos': 'vinhos',
                'suco': 'sucos',
                'sucos': 'sucos',
                'espumante': 'espumantes',
                'espumantes': 'espumantes',
                'uva': 'uvas',
                'uvas': 'uvas',
            }
        }
        
        if category in mapping and product_type.lower() in mapping[category]:
            return mapping[category][product_type.lower()]
        
        return None
    
    def detect_subcategory_from_data(self, category: str, data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Detecta automaticamente a subcategoria com base no conteúdo dos dados.
        
        Args:
            category: Categoria principal dos dados ('processamento', 'importacao', 'exportacao', etc.)
            data: Lista de registros para análise
            
        Returns:
            Subcategoria detectada ou None se não foi possível detectar
        """
        if not data or len(data) < 2:
            return None
            
        # Para processamento, verificamos o tipo de cultivar
        if category == "processamento" and category in self.scraper.CULTIVAR_TYPE_MAPPING:
            # Obtenha todas as cultivares nos dados
            cultivares = []
            for item in data:
                if "Cultivar" in item and item["Cultivar"]:
                    cultivares.append(item["Cultivar"])
            
            if not cultivares:
                return None
                
            # Contadores por tipo
            counts = {subcategory: 0 for subcategory in self.scraper.CULTIVAR_TYPE_MAPPING[category].keys()}
            
            # Conta as ocorrências de cada tipo
            for cultivar in cultivares:
                for subcategory, cultivar_list in self.scraper.CULTIVAR_TYPE_MAPPING[category].items():
                    if any(c.lower() in cultivar.lower() for c in cultivar_list):
                        counts[subcategory] += 1
            
            # Retorna o tipo com mais ocorrências, se houver algum
            if counts:
                max_count = max(counts.values())
                if max_count > 0:
                    return max(counts.items(), key=lambda x: x[1])[0]
        
        # Para exportação e importação, verificamos com base no nome do arquivo de fallback
        elif category in ["exportacao", "importacao"]:
            # Vamos verificar pelo padrão de colunas nos dados
            column_names = []
            for item in data:
                column_names.extend(item.keys())
            
            column_names = list(set(column_names))  # Remove duplicatas
            
            # Verificamos países/destinos que indicam exportação
            if "Países" in column_names and category == "exportacao":
                # Tenta identificar por caracteres distintos dos valores
                # Verifica tipos específicos de produtos nos dados
                all_text = ""
                for item in data:
                    all_text += str(item)
                
                # Verifica palavras-chave para vinhos
                if any(keyword in all_text.lower() for keyword in ["vinho", "vinhos", "cabernet", "merlot", "chardonnay"]):
                    return "vinhos"
                
                # Verifica palavras-chave para espumantes
                if any(keyword in all_text.lower() for keyword in ["espumante", "espumantes", "champagne", "moscatel"]):
                    return "espumantes"
                
                # Verifica palavras-chave para sucos
                if any(keyword in all_text.lower() for keyword in ["suco", "sucos", "concentrado"]):
                    return "sucos"
                
                # Verifica palavras-chave para uvas
                if any(keyword in all_text.lower() for keyword in ["uva", "uvas", "fresca", "frescas", "mesa"]):
                    return "uvas"
                
                # Se não encontrou nenhum padrão específico, verifica o padrão de export. mais comum
                return "vinhos"  # Fallback para a subcategoria mais comum
        
        # Para outros tipos de categoria, podemos implementar lógicas específicas no futuro
        return None
    
    def clean_unnecessary_headers(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove cabeçalhos desnecessários e redundantes dos dados.
        
        Args:
            data: Lista de registros para limpar
            
        Returns:
            Lista de registros limpa
        """
        if not data:
            return data
        
        cleaned_data = []
        
        # Chaves a serem filtradas
        keys_to_filter = [
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado",
            "Dados da Vitivinicultura",
        ]
        
        # Identificadores de linhas de cabeçalho
        header_identifiers = [
            {"Cultivar": "Cultivar", "Quantidade (Kg)": "Quantidade (Kg)"},
            {"Países": "Países", "Quantidade (Kg)": "Quantidade (Kg)"},
        ]
        
        # Filtra os registros
        for item in data:
            # Pula itens que são cabeçalhos redundantes
            if any(all(item.get(k) == v for k, v in header_id.items()) for header_id in header_identifiers):
                continue
            
            # Remove chaves desnecessárias
            cleaned_item = {k: v for k, v in item.items() if k not in keys_to_filter}
            
            # Adiciona apenas se o item tiver algum conteúdo após a limpeza
            if cleaned_item:
                cleaned_data.append(cleaned_item)
        
        return cleaned_data
    
    def _load_fallback_data(self, category: str, subcategory: Optional[str] = None) -> Dict[str, Any]:
        """
        Load fallback data from local CSV files with improved handling of different
        CSV formats and subcategories
        
        Args:
            category: Category to load data for
            subcategory: Optional subcategory
            
        Returns:
            Dictionary with data and metadata, or None if fallback fails
        """
        if category not in self.fallback_files:
            logger.error(f"No fallback files defined for category: {category}")
            return None
            
        # Determine the appropriate file path based on category and subcategory
        if isinstance(self.fallback_files[category], dict):
            # If we have subcategories
            file_path = None
            
            if subcategory and subcategory in self.fallback_files[category]:
                file_path = self.fallback_files[category][subcategory]
            else:
                # Use default file if specified subcategory doesn't exist
                file_path = self.fallback_files[category].get('default')
                
            if not file_path:
                logger.error(f"No fallback file found for {category}/{subcategory}")
                return None
        else:
            # Simple string file path
            file_path = self.fallback_files[category]
        
        logger.info(f"Loading fallback data from {file_path}")
        
        try:
            # Try multiple parsing approaches to handle different CSV formats
            df = None
            parsing_errors = []
            
            # Attempt 1: Standard CSV loading with auto delimiter detection
            try:
                df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
            except Exception as e:
                parsing_errors.append(f"Standard parsing failed: {str(e)}")
            
            # Attempt 2: Try with semicolon delimiter if first attempt failed
            if df is None:
                try:
                    df = pd.read_csv(file_path, sep=';', encoding='utf-8')
                except Exception as e:
                    parsing_errors.append(f"Semicolon delimiter failed: {str(e)}")
            
            # Attempt 3: Try with specific encoding if previous attempts failed
            if df is None:
                try:
                    df = pd.read_csv(file_path, sep=';', encoding='latin1')
                except Exception as e:
                    parsing_errors.append(f"Latin1 encoding failed: {str(e)}")
            
            # Handle error if all attempts failed
            if df is None:
                logger.error(f"All parsing attempts failed for {file_path}: {parsing_errors}")
                return None
            
            # Post-process the data: convert from wide to long format if needed
            # If we have years as column names (wide format), transform to long format
            year_columns = [col for col in df.columns if str(col).isdigit() or 
                           (isinstance(col, str) and re.match(r'^(19|20)\d{2}$', col))]
            
            # If data is in wide format (years as columns)
            if len(year_columns) > 0:
                logger.info(f"Converting wide format data to long format ({len(year_columns)} year columns)")
                # Identify ID and metadata columns
                id_cols = [col for col in df.columns if col.lower() in 
                          ['id', 'control', 'produto', 'cultivar', 'país', 'pais']]
                
                # If no ID columns found, use first non-year column
                if not id_cols and len(df.columns) > len(year_columns):
                    id_cols = [col for col in df.columns if col not in year_columns][:1]
                
                # Convert to long format
                if id_cols:
                    try:
                        # Melt the DataFrame to convert wide to long
                        melted_df = pd.melt(
                            df,
                            id_vars=id_cols,
                            value_vars=year_columns,
                            var_name='ano',
                            value_name='valor'
                        )
                        
                        # Clean up the melted DataFrame
                        melted_df['ano'] = pd.to_numeric(melted_df['ano'], errors='coerce')
                        df = melted_df
                        
                        logger.info(f"Successfully converted to long format: {len(df)} rows")
                    except Exception as e:
                        logger.warning(f"Error converting to long format: {str(e)}")
            
            # Handle special values like 'nd' and '*'
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].replace(['nd', '*', '-'], np.nan)
            
            # Convert numeric columns
            for col in df.columns:
                if col != 'ano' and df[col].dtype == object:  # Skip already numeric columns
                    # Try to convert column to numeric if it contains numbers
                    try:
                        # Handle European number format (comma as decimal separator)
                        numeric_col = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                        numeric_col = pd.to_numeric(numeric_col, errors='coerce')
                        
                        # If most values converted successfully, apply the conversion
                        if numeric_col.notna().sum() > 0.5 * len(numeric_col):
                            df[col] = numeric_col
                    except:
                        pass
            
            # Convert to records
            data = df.replace({np.nan: None}).to_dict('records')
            
            return {
                "data": data,
                "metadata": {
                    "category": category,
                    "subcategory": subcategory,
                    "source": "fallback_file",
                    "file": file_path,
                    "record_count": len(data)
                },
                "fallback_used": True
            }
        except Exception as e:
            logger.error(f"Error loading fallback file {file_path}: {str(e)}")
            return None
    
    def _sanitize_for_json(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sanitize data to ensure it can be JSON serialized by replacing non-JSON-compliant values
        and cleaning up the data structure
        
        Args:
            data: Data to sanitize
            
        Returns:
            Sanitized data safe for JSON serialization
        """
        if not data:
            return []
        
        # First, clean up the structure using the export cleaning logic
        cleaned_data = self._clean_data_for_export(data)
        sanitized_data = []
        
        def clean_value(value):
            # Handle special string values
            if isinstance(value, str):
                # Replace empty strings or dash-only strings with None
                if value.strip() in ('', '-'):
                    return None
                # Try to convert numerical strings to proper numbers
                try:
                    if ',' in value and '.' not in value:
                        # Handle European number format
                        cleaned_val = value.replace('.', '').replace(',', '.')
                        if cleaned_val.replace('.', '').isdigit():
                            if '.' in cleaned_val:
                                return float(cleaned_val)
                            else:
                                return int(cleaned_val)
                except ValueError:
                    pass
            
            # Handle float special values
            if isinstance(value, float):
                if math.isnan(value) or value != value:  # Another way to check NaN
                    return None
                if math.isinf(value):
                    if value > 0:
                        return 1.7976931348623157e+308  # Max JSON float
                    else:
                        return -1.7976931348623157e+308  # Min JSON float
            
            # Handle numpy types
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                float_val = float(value)
                if math.isnan(float_val) or math.isinf(float_val):
                    return None
                return float_val
            if isinstance(value, np.ndarray):
                return value.tolist()
            if isinstance(value, np.bool_):
                return bool(value)
            
            # Handle nested dictionaries
            if isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
                
            # Handle lists
            if isinstance(value, list):
                return [clean_value(item) for item in value]
                
            return value
        
        # Clean all values in the dataset after structure cleanup
        for item in cleaned_data:
            sanitized_item = {k: clean_value(v) for k, v in item.items()}
            sanitized_data.append(sanitized_item)
            
        return sanitized_data
    
    def export_to_parquet(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """
        Export data to Parquet format for ML pipeline integration with improved structure
        
        Args:
            data: Data to export
            file_path: Path to save the Parquet file
            
        Returns:
            Success status
        """
        try:
            if not data:
                logger.warning("No data to export to Parquet")
                return False
                
            # Clean and normalize the data before export
            cleaned_data = self._clean_data_for_export(data)
            
            # Convert to DataFrame
            df = pd.DataFrame(cleaned_data)
            
            # Remove rows that are just navigation elements or metadata
            if 'Produto' in df.columns:
                df = df[~df['Produto'].isin(['DOWNLOAD', 'TOPO', '« ‹ › »'])]
                
            # Remove duplicate rows
            if len(df) > 0:
                content_cols = [col for col in df.columns if col not in ['source', 'timestamp', 'metadata']]
                if content_cols:
                    df = df.drop_duplicates(subset=content_cols, keep='first')
            
            # Convert string numbers to actual numbers for better storage efficiency
            for col in df.columns:
                if df[col].dtype == 'object':  # If column is string/object type
                    # Try to convert to numeric, but only if the majority can be converted
                    try:
                        # Replace commas with dots for decimal conversion
                        temp_col = df[col].str.replace(',', '.', regex=False)
                        converted = pd.to_numeric(temp_col, errors='coerce')
                        # If most values convert successfully, apply the conversion
                        if converted.notna().sum() > 0.5 * df.shape[0]:
                            df[col] = converted
                    except:
                        pass
            
            # Export to Parquet with compression
            df.to_parquet(file_path, index=False, compression='snappy')
            logger.info(f"Successfully exported {len(df)} rows to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to Parquet: {str(e)}")
            return False
    
    def export_to_csv(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """
        Export data to CSV format with improved structure and cleaning
        
        Args:
            data: Data to export
            file_path: Path to save the CSV file
            
        Returns:
            Success status
        """
        try:
            if not data:
                logger.warning("No data to export to CSV")
                return False
                
            # Clean and normalize the data before export
            cleaned_data = self._clean_data_for_export(data)
            
            # Convert to DataFrame and export
            df = pd.DataFrame(cleaned_data)
            
            # Remove rows that are just navigation elements or metadata
            if 'Produto' in df.columns:
                df = df[~df['Produto'].isin(['DOWNLOAD', 'TOPO', '« ‹ › »'])]
                
            # Remove duplicate rows by checking all values except metadata columns
            if len(df) > 0:
                content_cols = [col for col in df.columns if col not in ['source', 'timestamp', 'metadata']]
                if content_cols:
                    df = df.drop_duplicates(subset=content_cols, keep='first')
            
            # Export to CSV
            df.to_csv(file_path, index=False, encoding='utf-8-sig')  # Use utf-8-sig to ensure proper encoding with BOM
            logger.info(f"Successfully exported {len(df)} rows to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return False
            
    def _clean_data_for_export(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and structure data for export formats
        
        Args:
            data: Original data to clean
            
        Returns:
            Cleaned data ready for export
        """
        cleaned_data = []
        
        # First pass: identify important columns and normalize data
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
            
        # Remove unnecessary columns
        columns_to_remove = [
            col for col in all_keys 
            if col.startswith('column_') or 
               'copyright' in col.lower() or 
               'livramento' in col.lower() or
               'embrapa' in col.lower()
        ]
        
        # Process each data item
        for item in data:
            # Skip items that appear to be metadata or navigation
            if any(nav in str(item.values()) for nav in ['DOWNLOAD', 'TOPO', '« ‹ › »']):
                continue
                
            # Skip mostly empty rows
            if sum(1 for v in item.values() if v and str(v).strip()) <= 1:
                continue
                
            # Create a new clean item
            clean_item = {}
            
            # Copy values, except those in columns_to_remove
            for k, v in item.items():
                if k not in columns_to_remove:
                    # Clean string values
                    if isinstance(v, str):
                        # Remove long descriptive texts that appear to be metadata
                        if len(v) > 200 and ('banco de dados' in v.lower() or 'download' in v.lower()):
                            continue
                        # Clean up the value
                        clean_v = v.strip()
                    else:
                        clean_v = v
                        
                    clean_item[k] = clean_v
            
            # Only add items that have useful data
            if clean_item and len(clean_item) > 1:
                cleaned_data.append(clean_item)
                
        return cleaned_data
    
    def _filter_data(
        self,
        data: List[Dict[str, Any]],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        region: Optional[str] = None,
        product_type: Optional[str] = None,
        channel: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Filter data based on various criteria
        
        Args:
            data: The data list to filter
            start_year: Starting year for filtering
            end_year: End year for filtering
            region: Region to filter by
            product_type: Product type to filter by
            channel: Channel to filter by (for comercializacao)
            origin: Origin country/region to filter by (for imports)
            destination: Destination country/region to filter by (for exports)
            
        Returns:
            Filtered data list
        """
        if not data:
            return []
            
        filtered_data = data
        
        # Filter by year if present
        if start_year is not None or end_year is not None:
            year_filtered = []
            for item in filtered_data:
                year_value = None
                
                # Try to find the year value
                if 'ano' in item:
                    year_value = item['ano']
                elif 'Ano' in item:
                    year_value = item['Ano']
                
                # Try to convert year to integer if it's a string
                if isinstance(year_value, str):
                    try:
                        year_value = int(year_value)
                    except (ValueError, TypeError):
                        # If conversion fails, skip this record
                        continue
                        
                # Apply year filter
                if year_value is not None:
                    if start_year is not None and year_value < start_year:
                        continue
                    if end_year is not None and year_value > end_year:
                        continue
                    year_filtered.append(item)
                else:
                    # If no year field found, include it (might be cross-year data)
                    year_filtered.append(item)
                    
            filtered_data = year_filtered
            
        # Filter by region if specified
        if region:
            region_filtered = []
            region_lower = region.lower()
            
            for item in filtered_data:
                # Check all possible region fields
                for field in ['Regiao', 'regiao', 'Região', 'região', 'Region', 'region']:
                    if field in item and item[field] and region_lower in str(item[field]).lower():
                        region_filtered.append(item)
                        break
            filtered_data = region_filtered
            
        # Filter by product type if specified
        if product_type:
            product_filtered = []
            product_lower = product_type.lower()
            
            for item in filtered_data:
                # Check all possible product fields
                for field in ['Produto', 'produto', 'tipo', 'Tipo', 'cultivar', 'Cultivar']:
                    if field in item and item[field] and product_lower in str(item[field]).lower():
                        product_filtered.append(item)
                        break
            filtered_data = product_filtered
            
        # Filter by channel (for comercializacao)
        if channel:
            channel_filtered = []
            channel_lower = channel.lower()
            
            for item in filtered_data:
                # Check all possible channel fields
                for field in ['Canal', 'canal', 'Canais', 'canais']:
                    if field in item and item[field] and channel_lower in str(item[field]).lower():
                        channel_filtered.append(item)
                        break
            filtered_data = channel_filtered
            
        # Filter by origin (for imports)
        if origin:
            origin_filtered = []
            origin_lower = origin.lower()
            
            for item in filtered_data:
                # Check all possible origin fields
                for field in ['Origem', 'origem', 'País', 'país', 'Pais', 'pais', 'Origin', 'origin']:
                    if field in item and item[field] and origin_lower in str(item[field]).lower():
                        origin_filtered.append(item)
                        break
            filtered_data = origin_filtered
            
        # Filter by destination (for exports)
        if destination:
            dest_filtered = []
            dest_lower = destination.lower()
            
            for item in filtered_data:
                # Check all possible destination fields
                for field in ['Destino', 'destino', 'País', 'país', 'Pais', 'pais', 'Destination']:
                    if field in item and item[field] and dest_lower in str(item[field]).lower():
                        dest_filtered.append(item)
                        break
            filtered_data = dest_filtered
            
        return filtered_data


# Create a global instance of the service
vini_data_service = ViniDataService()