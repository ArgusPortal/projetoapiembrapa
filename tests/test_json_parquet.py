#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to validate the improved JSON and Parquet output functionality
"""
import sys
import os
import logging
import json
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Add the project root to the path so we can import our modules
project_root = str(Path(__file__).parent.parent.absolute())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.services.data_service import vini_data_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def create_test_data():
    """Create test data that simulates the problematic structure"""
    return [
        {
            "column_0": "Dados da Vitivinicultura",
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": "Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado",
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": None,
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": "Banco de dados de uva, vinho e derivados Comercialização de vinhos e derivados",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": "« ‹ › »",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "Produto",
            "Quantidade (L.)": "Quantidade (L.)",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "VINHO DE MESA",
            "Quantidade (L.)": "215.557.931",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "Tinto",
            "Quantidade (L.)": "189.573.423",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "Rosado",
            "Quantidade (L.)": "1.394.901",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "DOWNLOAD",
            "Quantidade (L.)": "TOPO",
            "Copyright © Embrapa Uva e Vinho. Todos os direitos reservados.": None
        }
    ]

def test_json_output():
    """Test the improved JSON output functionality"""
    logger.info("Testing JSON output cleaning...")
    
    # Get test data
    test_data = create_test_data()
    
    # Before processing - original data
    logger.info(f"Original data length: {len(test_data)}")
    
    # Count metadata keys in original data
    metadata_keys_count = 0
    for item in test_data:
        for key in item:
            if any(marker in key for marker in ['column_', 'copyright', 'embrapa']):
                metadata_keys_count += 1
    logger.info(f"Original metadata keys count: {metadata_keys_count}")
    
    # After processing - data through sanitize_for_json
    cleaned_data = vini_data_service._sanitize_for_json(test_data)
    logger.info(f"Cleaned data length: {len(cleaned_data)}")
    
    # Count metadata keys in cleaned data
    cleaned_metadata_keys_count = 0
    for item in cleaned_data:
        for key in item:
            if any(marker in key for marker in ['column_', 'copyright', 'embrapa']):
                cleaned_metadata_keys_count += 1
    logger.info(f"Cleaned metadata keys count: {cleaned_metadata_keys_count}")
    
    # Write to file for manual inspection
    with open("tests/json_output_test.json", "w", encoding="utf-8") as f:
        json.dump({"data": cleaned_data}, f, ensure_ascii=False, indent=2)
    
    # Check that data rows are properly identified and preserved
    wine_products = [item for item in cleaned_data if item.get('Produto') == 'VINHO DE MESA']
    logger.info(f"Wine products found: {len(wine_products)}")
    
    # Verify numbers were properly converted from strings
    for item in cleaned_data:
        if isinstance(item.get('Quantidade (L.)'), (int, float)):
            logger.info(f"Number conversion successful: {item.get('Quantidade (L.)')}")
            break
    
    return {
        'original_length': len(test_data),
        'cleaned_length': len(cleaned_data),
        'metadata_keys_reduced': metadata_keys_count > cleaned_metadata_keys_count
    }

def test_parquet_output():
    """Test the improved Parquet output functionality"""
    logger.info("Testing Parquet output...")
    
    # Get test data
    test_data = create_test_data()
    
    # Write to parquet
    parquet_path = "tests/parquet_output_test.parquet"
    success = vini_data_service.export_to_parquet(test_data, parquet_path)
    
    if not success:
        logger.error("Failed to export to Parquet")
        return False
    
    # Read back the parquet file to analyze
    if os.path.exists(parquet_path):
        # Use PyArrow to inspect the file
        table = pq.read_table(parquet_path)
        logger.info(f"Parquet file schema: {table.schema}")
        logger.info(f"Parquet file rows: {table.num_rows}")
        
        # Check numeric column types
        df = table.to_pandas()
        numeric_columns = 0
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                numeric_columns += 1
                logger.info(f"Column {col} correctly stored as {df[col].dtype}")
                
        logger.info(f"Total numeric columns: {numeric_columns}")
        
        # Check file size optimization
        file_size = os.path.getsize(parquet_path)
        logger.info(f"Parquet file size: {file_size} bytes")
        
        # For comparison, save as CSV as well
        csv_path = "tests/parquet_comparison.csv"
        df.to_csv(csv_path, index=False)
        csv_size = os.path.getsize(csv_path)
        logger.info(f"CSV file size: {csv_size} bytes")
        logger.info(f"Compression ratio: {csv_size/file_size:.2f}x")
        
        return {
            'success': True,
            'row_count': table.num_rows,
            'numeric_columns': numeric_columns,
            'compression_ratio': csv_size/file_size
        }
    else:
        logger.error(f"Parquet file not found at {parquet_path}")
        return {'success': False}

def run_tests():
    """Run all tests and report results"""
    logger.info("Starting JSON and Parquet export tests")
    
    # Test JSON output
    json_results = test_json_output()
    
    # Test Parquet output
    parquet_results = test_parquet_output()
    
    # Report results
    logger.info("\n--- TEST RESULTS SUMMARY ---")
    
    logger.info("\nJSON Export Test:")
    logger.info(f"Original data rows: {json_results['original_length']}")
    logger.info(f"Cleaned data rows: {json_results['cleaned_length']}")
    logger.info(f"Metadata keys reduced: {'YES' if json_results['metadata_keys_reduced'] else 'NO'}")
    
    logger.info("\nParquet Export Test:")
    if isinstance(parquet_results, dict) and parquet_results.get('success'):
        logger.info(f"Export successful: YES")
        logger.info(f"Rows in parquet: {parquet_results['row_count']}")
        logger.info(f"Numeric columns: {parquet_results['numeric_columns']}")
        logger.info(f"Compression ratio: {parquet_results['compression_ratio']:.2f}x")
    else:
        logger.info("Export successful: NO")
    
    return json_results, parquet_results

if __name__ == "__main__":
    run_tests()