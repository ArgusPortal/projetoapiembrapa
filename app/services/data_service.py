import logging
from typing import Dict, List, Any, Optional
import math
import numpy as np
import pandas as pd

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
        
        # Define data fetching function for cache
        def fetch_data():
            logger.info(f"Fetching fresh data for category: {category}, subcategory: {subcategory or 'all'}")
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
            return scraped_data.dict()
        
        # Try to get from cache, fallback to fresh data
        result = data_cache.get(cache_key, fetch_data)
        
        if not result:
            logger.error(f"Failed to retrieve data for {category}")
            return {"error": "Data retrieval failed", "data": []}
        
        # Apply filters if necessary
        filtered_data = self._filter_data(
            result["data"], 
            region=region, 
            product_type=product_type,
            channel=channel,
            origin=origin,
            destination=destination
        )
        
        # Sanitize data for JSON serialization
        sanitized_data = self._sanitize_for_json(filtered_data)
        
        return {
            "metadata": result["metadata"],
            "data": sanitized_data,
            "from_cache": True if cache_key in data_cache.cache else False,
        }
    
    def _sanitize_for_json(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sanitize data to ensure it can be JSON serialized by replacing non-JSON-compliant values
        
        Args:
            data: Data to sanitize
            
        Returns:
            Sanitized data safe for JSON serialization
        """
        if not data:
            return []
        
        def clean_value(value):
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
        
        # Clean all values in the dataset
        sanitized_data = []
        for item in data:
            sanitized_item = {k: clean_value(v) for k, v in item.items()}
            sanitized_data.append(sanitized_item)
            
        return sanitized_data
    
    def _map_product_type_to_subcategory(self, category: str, product_type: str) -> Optional[str]:
        """
        Maps product types to appropriate subcategories for each category
        
        Args:
            category: Main category
            product_type: Product type string
            
        Returns:
            Mapped subcategory or None
        """
        mapping = {
            'producao': {
                'vinifera': 'viniferas',
                'americana': 'americanas_hibridas',
                'hibrida': 'americanas_hibridas',
                'vinho': 'vinhos_mesa',
                'suco': 'sucos',
            },
            'processamento': {
                'tipo': 'por_tipo',
                'regiao': 'por_regiao',
                'qualidade': 'controle_qualidade',
            },
            'comercializacao': {
                'varejo': 'varejo',
                'grande': 'grandes_redes',
                'rede': 'grandes_redes',
                'indireta': 'exportacao_indireta',
            },
            'importacao': {
                'fino': 'vinhos_finos',
                'espumante': 'espumantes',
                'fresca': 'uvas_frescas',
                'passa': 'uvas_passas',
                'suco': 'sucos',
            },
            'exportacao': {
                'mesa': 'vinhos_mesa',
                'espumante': 'espumantes',
                'fresca': 'uvas_frescas',
                'suco': 'sucos',
            }
        }
        
        if category in mapping:
            for key, value in mapping[category].items():
                if key.lower() in product_type.lower():
                    return value
                    
        return None
    
    def _filter_data(
        self, 
        data: List[Dict[str, Any]], 
        region: Optional[str] = None, 
        product_type: Optional[str] = None,
        channel: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Apply filters to the data
        
        Args:
            data: Data to filter
            region: Region filter
            product_type: Product type filter
            channel: Commercialization channel
            origin: Import origin
            destination: Export destination
            
        Returns:
            Filtered data
        """
        if not data:
            return []
            
        # Convert to pandas for easier filtering
        df = pd.DataFrame(data)
        
        # Apply region filter if provided
        if region:
            region_columns = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["regiao", "estado", "uf", "municipio", "local"]
            )]
            
            if region_columns:
                # Filter by any column that might contain region info
                mask = False
                for col in region_columns:
                    mask |= df[col].str.contains(region, case=False, na=False)
                df = df[mask]
        
        # Apply product type filter if provided
        if product_type:
            product_columns = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["produto", "tipo", "variedade", "uva", "vinho", "suco", "cultivar"]
            )]
            
            if product_columns:
                # Filter by any column that might contain product info
                mask = False
                for col in product_columns:
                    mask |= df[col].str.contains(product_type, case=False, na=False)
                df = df[mask]

        # Apply channel filter if provided
        if channel:
            channel_columns = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["canal", "comercializacao", "venda", "varejo", "rede"]
            )]
            
            if channel_columns:
                # Filter by any column that might contain channel info
                mask = False
                for col in channel_columns:
                    mask |= df[col].str.contains(channel, case=False, na=False)
                df = df[mask]
                
        # Apply origin filter if provided
        if origin:
            origin_columns = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["origem", "procedencia", "pais", "fornecedor"]
            )]
            
            if origin_columns:
                # Filter by any column that might contain origin info
                mask = False
                for col in origin_columns:
                    mask |= df[col].str.contains(origin, case=False, na=False)
                df = df[mask]
                
        # Apply destination filter if provided
        if destination:
            destination_columns = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["destino", "pais", "mercado", "comprador"]
            )]
            
            if destination_columns:
                # Filter by any column that might contain destination info
                mask = False
                for col in destination_columns:
                    mask |= df[col].str.contains(destination, case=False, na=False)
                df = df[mask]
        
        # Convert back to list of dicts
        return df.to_dict("records")
    
    def export_to_parquet(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """
        Export data to Parquet format for ML pipeline integration
        
        Args:
            data: Data to export
            file_path: Path to save the Parquet file
            
        Returns:
            Success status
        """
        try:
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            return True
        except Exception as e:
            logger.error(f"Error exporting to Parquet: {str(e)}")
            return False
    
    def export_to_csv(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """
        Export data to CSV format
        
        Args:
            data: Data to export
            file_path: Path to save the CSV file
            
        Returns:
            Success status
        """
        try:
            df = pd.DataFrame(data)
            df.to_csv(file_path, index=False)
            return True
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return False


# Create a global instance of the service
vini_data_service = ViniDataService()