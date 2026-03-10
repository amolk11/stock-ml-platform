"""
Yahoo Finance API Client
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging

from src.data_ingestion.api_clients.base_client import BaseAPIClient
from src.utils.decorators import retry

logger = logging.getLogger(__name__)


class YahooFinanceClient(BaseAPIClient):
    """Yahoo Finance data client using yfinance library."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
    
    @retry(max_attempts=3, delay=2.0)
    def fetch_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical data from Yahoo Finance.
        
        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            start_date: Start date
            end_date: End date
            
        Returns:
            List of market data dictionaries
        """
        self._enforce_rate_limit()
        
        try:
            logger.info(
                f"Fetching {symbol} data from {start_date.date()} to {end_date.date()}"
            )
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date,
                end=end_date + timedelta(days=1),  # Include end_date
                interval='1d'
            )
            
            if df.empty:
                logger.warning(f"No data returned for {symbol}")
                return []
            
            # Convert to list of dictionaries
            records = []
            for timestamp, row in df.iterrows():
                record = {
                    'symbol': symbol,
                    'timestamp': timestamp.to_pydatetime(),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']),
                    'adjusted_close': float(row['Close']),  # Yahoo adjusts automatically
                    'source': self.get_source_name()
                }
                records.append(record)
            
            logger.info(f"Fetched {len(records)} records for {symbol}")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            raise
    
    @retry(max_attempts=3, delay=2.0)
    def fetch_latest_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the most recent trading data.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Latest market data or None
        """
        self._enforce_rate_limit()
        
        try:
            # Get last 2 days to ensure we have the latest complete bar
            end_date = datetime.now()
            start_date = end_date - timedelta(days=2)
            
            data = self.fetch_historical_data(symbol, start_date, end_date)
            
            if data:
                return data[-1]  # Return most recent record
            return None
            
        except Exception as e:
            logger.error(f"Error fetching latest data for {symbol}: {e}")
            return None