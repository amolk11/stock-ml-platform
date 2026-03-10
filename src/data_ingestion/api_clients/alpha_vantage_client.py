"""
Alpha Vantage API Client
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import requests
import logging
import os

from src.data_ingestion.api_clients.base_client import BaseAPIClient
from src.utils.decorators import retry

logger = logging.getLogger(__name__)


class AlphaVantageClient(BaseAPIClient):
    """Alpha Vantage API client."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url', 'https://www.alphavantage.co/query')
        self.api_key = config.get('api_key') or os.getenv('ALPHA_VANTAGE_API_KEY')
        
        if not self.api_key:
            logger.warning("Alpha Vantage API key not configured")
            self.enabled = False
    
    @retry(max_attempts=3, delay=5.0)
    def fetch_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical daily data from Alpha Vantage.
        
        Args:
            symbol: Stock ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            List of market data dictionaries
        """
        if not self.enabled:
            logger.warning("Alpha Vantage client is disabled")
            return []
        
        self._enforce_rate_limit()
        
        try:
            params = {
                'function': 'TIME_SERIES_DAILY_ADJUSTED',
                'symbol': symbol,
                'outputsize': 'full',  # Get full historical data
                'apikey': self.api_key
            }
            
            logger.info(f"Fetching {symbol} from Alpha Vantage")
            
            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            if 'Note' in data:
                logger.warning(f"API Note: {data['Note']}")
                return []
            
            time_series = data.get('Time Series (Daily)', {})
            
            if not time_series:
                logger.warning(f"No time series data for {symbol}")
                return []
            
            # Convert to list of dictionaries
            records = []
            for date_str, values in time_series.items():
                timestamp = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Filter by date range
                if not (start_date <= timestamp <= end_date):
                    continue
                
                record = {
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'adjusted_close': float(values['5. adjusted close']),
                    'volume': int(values['6. volume']),
                    'source': self.get_source_name()
                }
                records.append(record)
            
            logger.info(f"Fetched {len(records)} records for {symbol}")
            return sorted(records, key=lambda x: x['timestamp'])
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            raise
    
    def fetch_latest_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch latest market data.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Latest market data or None
        """
        try:
            # Alpha Vantage doesn't have a specific "latest" endpoint
            # So we fetch recent data and take the most recent
            end_date = datetime.now()
            start_date = end_date.replace(day=1)  # Current month
            
            data = self.fetch_historical_data(symbol, start_date, end_date)
            
            if data:
                return data[-1]
            return None
            
        except Exception as e:
            logger.error(f"Error fetching latest data for {symbol}: {e}")
            return None