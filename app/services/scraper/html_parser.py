# -*- coding: utf-8 -*-
"""
HTML Parser module for AdaptiveScraper
Contains functions for parsing and extracting data from HTML content
"""

import hashlib
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def parse_html(html_content: str) -> BeautifulSoup:
    """
    Parse HTML content into a BeautifulSoup object
    
    Args:
        html_content: Raw HTML string
        
    Returns:
        BeautifulSoup object
    """
    return BeautifulSoup(html_content, 'html.parser')

def find_table_elements(soup: BeautifulSoup) -> List[Any]:
    """
    Find all table elements in a BeautifulSoup object
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        List of table elements
    """
    tables = soup.find_all('table')
    
    if not tables:
        # Try finding tables within specific containers
        containers = soup.find_all(['div', 'section'], {'class': ['content', 'main', 'data', 'table-container']})
        tables = []
        for container in containers:
            tables.extend(container.find_all('table'))
    
    return tables

def extract_links(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Extract all links from a BeautifulSoup object
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        List of dictionaries containing href and text for each link
    """
    links = []
    for a in soup.find_all('a', href=True):
        links.append({
            'href': a['href'],
            'text': a.get_text(strip=True)
        })
    return links

def extract_table_data(html_content: str) -> List[Dict[str, Any]]:
    """
    Extract tabular data from HTML content
    
    Args:
        html_content: HTML content to parse
        
    Returns:
        List of dictionaries containing the extracted data
    """
    soup = parse_html(html_content)
    tables = find_table_elements(soup)
    
    if not tables:
        logger.warning("No tables found in HTML content")
        return []
    
    all_data = []
    
    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue
            
        # Try to extract headers from the first row
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # Process data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) != len(headers) and len(headers) > 0:
                # Skip rows with a different number of columns
                continue
                
            row_data = {}
            for i, cell in enumerate(cells):
                # Use header if available, otherwise use column index
                col_name = headers[i] if i < len(headers) and headers[i] else f"col_{i}"
                row_data[col_name] = cell.get_text(strip=True)
                
            if row_data:
                all_data.append(row_data)
    
    return all_data

def detect_schema_changes(url: str, html_content: str, last_known_hash: Dict[str, str]) -> bool:
    """
    Detect changes in HTML structure by comparing hash with last known hash
    
    Args:
        url: URL of the page
        html_content: HTML content to check
        last_known_hash: Dictionary of previous hashes by URL
        
    Returns:
        bool: True if a change was detected, False otherwise
    """
    # Extract just the main content div to avoid hash changes due to dynamic elements
    soup = parse_html(html_content)
    main_content = soup.find('div', {'class': 'main-content'})
    content_to_hash = (main_content.prettify() if main_content else html_content)
    
    current_hash = hashlib.md5(content_to_hash.encode()).hexdigest()
    
    if url not in last_known_hash:
        last_known_hash[url] = current_hash
        return False
        
    if current_hash != last_known_hash[url]:
        logger.warning(f'Schema change detected for {url}')
        last_known_hash[url] = current_hash
        return True
        
    return False

def update_parsing_strategy(url: str, html_content: str) -> None:
    """
    Update the parsing strategy when a schema change is detected
    
    Args:
        url: URL of the page
        html_content: HTML content to analyze
    """
    logger.info(f'Analyzing new structure for {url}')
    soup = parse_html(html_content)
    
    # Attempt to detect table headers and structure
    tables = soup.find_all('table')
    logger.info(f'Found {len(tables)} tables on the page')
    
    if not tables:
        # Try finding tables within specific containers
        containers = soup.find_all(['div', 'section'], {'class': ['content', 'main', 'data', 'table-container']})
        for container in containers:
            tables.extend(container.find_all('table'))
        logger.info(f'Found {len(tables)} tables after searching within containers')