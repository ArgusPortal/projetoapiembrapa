import time
import threading
import logging
from typing import Dict, Any, Optional, Callable, TypeVar, Generic

from cachetools import LRUCache, TTLCache
from app.core.config import settings

T = TypeVar('T')
logger = logging.getLogger(__name__)

class ResilientCache(Generic[T]):
    """
    A resilient caching system with TTL and LRU eviction policy.
    Also implements fallback mechanism for failed data retrieval.
    """
    
    def __init__(self, max_size: int = None, ttl: int = None):
        """
        Initialize the cache with specified size and TTL
        
        Args:
            max_size: Maximum number of items in the cache
            ttl: Time to live for cache items in seconds
        """
        self.max_size = max_size or settings.CACHE_MAX_SIZE
        self.ttl = ttl or settings.CACHE_TTL
        
        # Main cache with TTL and LRU eviction
        self.cache = TTLCache(maxsize=self.max_size, ttl=self.ttl)
        
        # Historical cache for fallback (no TTL)
        self.historical_cache = LRUCache(maxsize=self.max_size * 2)
        
        # Lock for thread safety
        self.lock = threading.RLock()
    
    def get(self, key: str, fetch_func: Callable[[], T] = None) -> Optional[T]:
        """
        Get an item from cache, with optional fetching function
        
        Args:
            key: Cache key
            fetch_func: Optional function to fetch data if not in cache
            
        Returns:
            Cached value or value from fetch_func
        """
        # Try to get from main cache first
        try:
            with self.lock:
                if key in self.cache:
                    logger.debug(f"Cache hit for key: {key}")
                    return self.cache[key]
        except Exception as e:
            logger.warning(f"Error accessing cache for key {key}: {str(e)}")
        
        # If fetch_func is provided, try to fetch fresh data
        if fetch_func:
            try:
                logger.debug(f"Cache miss for key: {key}, fetching fresh data")
                value = fetch_func()
                self.set(key, value)
                return value
            except Exception as e:
                logger.error(f"Error fetching fresh data for key {key}: {str(e)}")
        
        # If main cache failed and fetch also failed, try historical cache
        try:
            with self.lock:
                if key in self.historical_cache:
                    logger.warning(f"Using historical data for key: {key}")
                    return self.historical_cache[key]
        except Exception:
            pass
        
        # Nothing worked
        return None
    
    def set(self, key: str, value: T) -> None:
        """
        Set a value in the cache
        
        Args:
            key: Cache key
            value: Value to cache
        """
        try:
            with self.lock:
                # Store in main cache
                self.cache[key] = value
                # Also store in historical cache for fallback
                self.historical_cache[key] = value
                logger.debug(f"Cached value for key: {key}")
        except Exception as e:
            logger.error(f"Error setting cache for key {key}: {str(e)}")
    
    def invalidate(self, key: str) -> None:
        """
        Invalidate a specific cache entry
        
        Args:
            key: Cache key to invalidate
        """
        try:
            with self.lock:
                if key in self.cache:
                    del self.cache[key]
                    logger.debug(f"Invalidated cache for key: {key}")
        except Exception as e:
            logger.error(f"Error invalidating cache for key {key}: {str(e)}")
    
    def clear(self) -> None:
        """Clear the entire cache"""
        try:
            with self.lock:
                self.cache.clear()
                logger.debug("Cache cleared")
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")


# Global cache instance
data_cache = ResilientCache[Dict[str, Any]]()