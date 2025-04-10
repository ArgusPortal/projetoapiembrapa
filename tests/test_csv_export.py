#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to validate the improved CSV export functionality
"""
import sys
import os
import logging
import json
import pandas as pd
from pathlib import Path

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

def test_csv_export():
    """Test the improved CSV export functionality"""
    # Load sample problematic data
    sample_data = []
    
    # Create test data that simulates the problematic structure
    test_data = [
        {
            "column_0": "Dados da Vitivinicultura",
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": "Loiva Maria Ribeiro de Mello",
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": None,
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": "Banco de dados de uva, vinho e derivados Comercialização de vinhos e derivados",
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": None,
            "Quantidade (L.)": "« ‹ › »",
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "VINHO DE MESA",
            "Quantidade (L.)": "215.557.931",
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "Tinto",
            "Quantidade (L.)": "189.573.423",
        },
        # Duplicated data (simulating the issue in the sample CSV)
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "VINHO DE MESA",
            "Quantidade (L.)": "215.557.931",
        },
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "Tinto",
            "Quantidade (L.)": "189.573.423",
        },
        # Navigation elements
        {
            "column_0": None,
            "Dados da Vitivinicultura Loiva Maria Ribeiro de Mello Carlos Alberto Ely Machado": None,
            "ano": 2020,
            "Produto": "DOWNLOAD",
            "Quantidade (L.)": "TOPO",
        }
    ]

    # Export data using both the old and new methods for comparison
    
    # Original method (direct pandas export)
    df_original = pd.DataFrame(test_data)
    original_csv_path = "tests/csv_export_original.csv"
    df_original.to_csv(original_csv_path, index=False)
    logger.info(f"Original CSV exported to {original_csv_path}")
    
    # New improved method
    improved_csv_path = "tests/csv_export_improved.csv"
    success = vini_data_service.export_to_csv(test_data, improved_csv_path)
    logger.info(f"Improved CSV export {'succeeded' if success else 'failed'}")
    
    # Compare the files
    if os.path.exists(improved_csv_path):
        df_improved = pd.read_csv(improved_csv_path)
        
        logger.info("\n--- COMPARISON ---")
        logger.info(f"Original rows: {len(df_original)}")
        logger.info(f"Improved rows: {len(df_improved)}")
        
        logger.info("\nOriginal columns:")
        for col in df_original.columns:
            logger.info(f"  - {col}")
            
        logger.info("\nImproved columns:")
        for col in df_improved.columns:
            logger.info(f"  - {col}")
        
        # Check for duplicates in original
        orig_dupes = df_original.duplicated().sum()
        logger.info(f"\nDuplicates in original: {orig_dupes}")
        
        # Check for duplicates in improved
        imp_dupes = df_improved.duplicated().sum()
        logger.info(f"Duplicates in improved: {imp_dupes}")
        
        # Show sample data from improved CSV
        logger.info("\nSample data from improved CSV:")
        logger.info(df_improved.head())
        
        return True
    else:
        logger.error(f"Failed to find improved CSV at {improved_csv_path}")
        return False

if __name__ == "__main__":
    test_csv_export()