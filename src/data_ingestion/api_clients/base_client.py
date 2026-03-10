"""
Base API Client - Abstract class for all data source clients
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import time

from src.utils.decorators import retry, timeit

logger = logging.getLogger(__name__)


class BaseAPIClient(ABC):
    """
    Abstract base class for all API clients.
    
    All data source implementations should inherit from this class.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize API client.
        
        Args:
            config: Configuration dictionary for this API source
        """
        self.config = config
        self.name = config.get('name', 'Unknown API')
        self.enabled = config.get('enabled', True)
        self.rate_limit = config.get('rate_limit', 60)
        self.timeout = config.get('timeout', 30)
        self.last_request_time = 0
        
        logger.info(f"Initialized {self.name} client")
    
    def _enforce_rate_limit(self):
        """Enforce rate limiting between API calls."""
        if self.rate_limit <= 0:
            return
        
        min_interval = 60.0 / self.rate_limit  # seconds between requests
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @abstractmethod
    def fetch_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical market data.
        
        Args:
            symbol: Stock ticker symbol
            start_date: Start date for historical data
            end_date: End date for historical data
            
        Returns:
            List of dictionaries containing OHLCV data
        """
        pass
    
    @abstractmethod
    def fetch_latest_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch latest market data (most recent bar).
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Dictionary containing latest OHLCV data or None
        """
        pass
    
    def get_source_name(self) -> str:
        """Get the name of this data source."""
        return self.name.lower().replace(' ', '_')