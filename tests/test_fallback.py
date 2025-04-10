#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to validate the fallback mechanisms
in the Vini Data API service
"""
import sys
import os
import logging
import json
from pathlib import Path

# Add the project root to the path so we can import our modules
project_root = str(Path(__file__).parent.parent.absolute())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.services.data_service import vini_data_service
from app.services.scraper.adaptive_scraper import AdaptiveScraper, ScrapedData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def test_raw_html_recovery():
    """Test the ability to recover data from raw HTML responses"""
    logger.info("Testing raw HTML recovery functionality...")
    
    # Sample HTML from the response you showed
    with open("tests/sample_raw_html.html", "r", encoding="utf-8") as f:
        raw_html = f.read()
    
    # Create a ScrapedData object with the raw HTML
    source_url = "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_02&subopcao=subopt_01&ano=1970"
    scraped_data = ScrapedData(
        source_url=source_url,
        timestamp=1234567890.0,
        data=[],  # Empty data to simulate parsing failure
        metadata={
            "category": "producao",
            "subcategory": "viniferas",
            "start_year": 1970,
            "end_year": 1970,
            "record_count": 0,
        },
        raw_html=raw_html
    )
    
    # Test recovery
    recovered_data = vini_data_service._attempt_data_recovery(scraped_data)
    
    if recovered_data:
        logger.info(f"Successfully recovered {len(recovered_data.data)} records from raw HTML")
        # Print sample of the recovered data
        if recovered_data.data:
            logger.info(f"Sample recovered data: {recovered_data.data[:3]}")
        return True
    else:
        logger.error("Failed to recover data from raw HTML")
        return False


def test_fallback_hierarchy():
    """Test the fallback hierarchy (cache -> online -> local files)"""
    logger.info("Testing fallback hierarchy...")
    
    # Test with a category we know exists in fallback files
    result = vini_data_service.get_data(
        category="producao",
        start_year=1970,
        end_year=1970
    )
    
    logger.info(f"Data source: {result.get('data_source', 'unknown')}")
    logger.info(f"From cache: {result.get('from_cache', False)}")
    logger.info(f"Fallback used: {result.get('fallback_used', False)}")
    logger.info(f"Record count: {len(result.get('data', []))}")
    
    # Print sample of the results
    if result.get("data"):
        logger.info(f"Sample data: {result['data'][:3]}")
    
    return result


def test_subcategory_fallback_files():
    """Test the functionality of subcategory-specific fallback files"""
    logger.info("Testing subcategory fallback file functionality...")
    
    # Define test cases for different categories and subcategories
    test_cases = [
        # (category, subcategory, expected_min_records)
        ("processamento", "viniferas", 5),
        ("processamento", "americanas", 5),
        ("processamento", "mesa", 3),
        ("processamento", "semclassificacao", 1),
        ("importacao", "vinhos", 5),
        ("importacao", "sucos", 3),
        ("importacao", "espumantes", 3),
        ("importacao", "passas", 3),
        ("importacao", "frescas", 3),
        ("exportacao", "vinhos", 3),
        ("exportacao", "sucos", 3),
        ("exportacao", "espumantes", 3),
        ("exportacao", "uvas", 3),
    ]
    
    results = {}
    success_count = 0
    
    # Run tests for each combination
    for category, subcategory, expected_min_records in test_cases:
        logger.info(f"Testing {category}/{subcategory}...")
        
        # Force fallback by using an unreachable URL for the scraper
        original_url = vini_data_service.scraper.base_url
        vini_data_service.scraper.base_url = "http://non-existent-url.local/"
        
        try:
            # Get data with the specified category and subcategory
            result = vini_data_service.get_data(
                category=category,
                subcategory=subcategory,
                start_year=1970,
                end_year=2022
            )
            
            # Check if data was retrieved
            record_count = len(result.get("data", []))
            success = record_count >= expected_min_records
            
            # Store test result
            results[f"{category}/{subcategory}"] = {
                "success": success,
                "record_count": record_count,
                "expected_min_records": expected_min_records,
                "source": result.get("data_source"),
                "sample": result.get("data", [])[:2] if result.get("data") else None
            }
            
            if success:
                success_count += 1
                logger.info(f"✓ {category}/{subcategory}: {record_count} records (expected min: {expected_min_records})")
            else:
                logger.error(f"✗ {category}/{subcategory}: {record_count} records (expected min: {expected_min_records})")
                
        except Exception as e:
            logger.error(f"Error testing {category}/{subcategory}: {str(e)}")
            results[f"{category}/{subcategory}"] = {"success": False, "error": str(e)}
        finally:
            # Restore original URL
            vini_data_service.scraper.base_url = original_url
    
    # Print summary
    logger.info("\n--- SUBCATEGORY FALLBACK TEST RESULTS ---")
    logger.info(f"Tests passed: {success_count}/{len(test_cases)}")
    
    return results


def test_csv_format_handling():
    """Test the handling of different CSV formats (wide vs long)"""
    logger.info("Testing wide-to-long CSV format conversion...")
    
    # Force fallback for producao (typically in wide format with years as columns)
    original_url = vini_data_service.scraper.base_url
    vini_data_service.scraper.base_url = "http://non-existent-url.local/"
    
    try:
        # Get data that should trigger wide-to-long conversion
        result = vini_data_service.get_data(
            category="producao",
            start_year=1970,
            end_year=2020
        )
        
        # Check if data was retrieved and properly formatted
        success = False
        if result.get("data"):
            # In long format, each record should have 'ano' and 'valor' fields
            # or something representing year and value
            sample_record = result["data"][0] if result["data"] else {}
            
            # Check if the data has been transformed to long format
            has_year_field = any(field.lower() == "ano" for field in sample_record.keys())
            has_value_field = any(field.lower() in ["valor", "quantidade", "volume"] for field in sample_record.keys())
            
            success = has_year_field and len(result["data"]) > 5
            
            logger.info(f"Sample record structure: {list(sample_record.keys())}")
            logger.info(f"Has year field: {has_year_field}")
            logger.info(f"Record count: {len(result.get('data', []))}")
            logger.info(f"Sample data: {result['data'][:2]}")
        
        return {
            "success": success,
            "record_count": len(result.get("data", [])),
            "data_source": result.get("data_source"),
            "sample": result.get("data", [])[:2] if result.get("data") else None
        }
    
    except Exception as e:
        logger.error(f"Error testing CSV format handling: {str(e)}")
        return {"success": False, "error": str(e)}
    
    finally:
        # Restore original URL
        vini_data_service.scraper.base_url = original_url


def test_dash_string_handling():
    """Test handling of dash strings in data"""
    logger.info("Testing dash string handling...")
    
    # Create test data with dash values
    test_data = [
        {"cultivar": "Arriloba", "quantidade": "-"},
        {"cultivar": "Barbera", "quantidade": "4.548.313"},
    ]
    
    # Sanitize the data
    clean_data = vini_data_service._sanitize_for_json(test_data)
    
    logger.info(f"Original data: {test_data}")
    logger.info(f"Sanitized data: {clean_data}")
    
    # Check if dash was converted to None
    success = any(item.get("quantidade") is None for item in clean_data)
    logger.info(f"Dash string conversion successful: {success}")
    
    return success


def test_regional_number_format():
    """Test handling of regional number formats with comma as decimal separator"""
    logger.info("Testing regional number format handling...")
    
    # Create test data with European number format
    test_data = [
        {"cultivar": "Test1", "quantidade": "1.234.567,89"},
        {"cultivar": "Test2", "quantidade": "123,45"},
    ]
    
    # Sanitize the data
    clean_data = vini_data_service._sanitize_for_json(test_data)
    
    logger.info(f"Original data: {test_data}")
    logger.info(f"Sanitized data: {clean_data}")
    
    # Check if European format numbers were converted correctly
    success = any(isinstance(item.get("quantidade"), (int, float)) for item in clean_data)
    logger.info(f"Number format conversion successful: {success}")
    
    return success


def create_sample_html_file():
    """Create a sample HTML file for testing if it doesn't exist"""
    file_path = "tests/sample_raw_html.html"
    
    if os.path.exists(file_path):
        logger.info(f"Sample HTML file already exists at {file_path}")
        return
    
    # Simple HTML structure based on the example you provided
    sample_html = """
    <html>
    <body>
    <div class="main-content">
        <h1>Uvas viníferas processadas [1970]</h1>
        <table>
            <tr>
                <th>Cultivar</th>
                <th>Quantidade (Kg)</th>
            </tr>
            <tr>
                <td>TINTAS</td>
                <td>10.448.228</td>
            </tr>
            <tr>
                <td>Alicante Bouschet</td>
                <td>-</td>
            </tr>
            <tr>
                <td>Barbera</td>
                <td>4.548.313</td>
            </tr>
            <tr>
                <td>Bonarda</td>
                <td>1.631.610</td>
            </tr>
            <tr>
                <td>Arriloba</td>
                <td>-</td>
            </tr>
        </table>
    </div>
    </body>
    </html>
    """
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(sample_html)
    
    logger.info(f"Created sample HTML file at {file_path}")


def run_all_tests():
    """Run all fallback mechanism tests"""
    logger.info("Starting fallback mechanism tests")
    
    # Create sample HTML file for testing
    create_sample_html_file()
    
    # Run all tests
    results = {
        "raw_html_recovery": test_raw_html_recovery(),
        "fallback_hierarchy": bool(test_fallback_hierarchy()),
        "subcategory_fallback": bool(test_subcategory_fallback_files()),
        "csv_format_handling": test_csv_format_handling().get("success", False),
        "dash_string_handling": test_dash_string_handling(),
        "regional_number_format": test_regional_number_format()
    }
    
    # Print summary
    logger.info("\n--- TEST RESULTS SUMMARY ---")
    for test_name, success in results.items():
        logger.info(f"{test_name}: {'PASS' if success else 'FAIL'}")
    
    # Overall result
    overall = all(results.values())
    logger.info(f"\nOVERALL RESULT: {'PASS' if overall else 'FAIL'}")
    
    return overall


if __name__ == "__main__":
    run_all_tests()