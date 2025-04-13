# -*- coding: utf-8 -*-
import logging
import time
import sqlite3
import os
import json
from typing import Dict, List, Optional, Any, Tuple

from .constants import DEFAULT_CULTIVAR_DATA

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
        # Add each cultivar to the database from constants
        for cultivar_type, cultivars in DEFAULT_CULTIVAR_DATA.items():
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
            # Try multiple encodings to handle potential encoding issues
            encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
            knowledge_base = None
            
            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        knowledge_base = json.load(f)
                    self.logger.info(f"Successfully loaded knowledge base with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    self.logger.warning(f"Failed to decode with {encoding}, trying next encoding")
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse JSON with {encoding} encoding")
                except Exception as e:
                    self.logger.warning(f"Error with {encoding} encoding: {str(e)}")
            
            # If all encodings failed, try binary mode as a last resort
            if knowledge_base is None:
                try:
                    with open(file_path, 'rb') as f:
                        # Skip any potential BOM markers
                        content = f.read()
                        if content.startswith(b'\xef\xbb\xbf'):  # UTF-8 BOM
                            content = content[3:]
                        # Try to decode after removing BOM and replace invalid chars
                        text = content.decode('utf-8', errors='replace')
                        knowledge_base = json.loads(text)
                    self.logger.info("Successfully loaded knowledge base using binary mode with BOM handling")
                except Exception as e:
                    self.logger.error(f"All encoding attempts failed: {str(e)}")
                    
                    # If the file is corrupted, try to create a new one
                    self.logger.warning("Knowledge base appears corrupted, creating a new one")
                    # Create a backup of the corrupted file
                    backup_path = file_path + '.corrupted'
                    try:
                        import shutil
                        shutil.copy2(file_path, backup_path)
                        self.logger.info(f"Backed up corrupted file to {backup_path}")
                        
                        # Export current in-memory database to create a new knowledge base
                        self.export_knowledge_base(file_path)
                        self.logger.info(f"Created new knowledge base at {file_path}")
                        
                        # Try to load the newly created file
                        with open(file_path, 'r', encoding='utf-8') as f:
                            knowledge_base = json.load(f)
                    except Exception as backup_error:
                        self.logger.error(f"Failed to create new knowledge base: {str(backup_error)}")
                        return False
                
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
    
    def close(self):
        """
        Close the SQLite database connection to prevent resource leaks
        """
        try:
            if hasattr(self, 'conn') and self.conn is not None:
                self.conn.close()
                self.logger.info("SQLite connection closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing SQLite connection: {str(e)}")
            
    def __del__(self):
        """
        Destructor to ensure connection is closed when object is garbage collected
        """
        try:
            self.close()
        except:
            pass  # Silently fail in destructor