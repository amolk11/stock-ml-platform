"""
Market Data Repository - Specialized queries for market data
"""

from typing import List, Optional
from datetime import datetime
from sqlalchemy import and_, desc
from sqlalchemy.orm import Session

from src.database.models import RawMarketData, Ticker
from src.database.repositories.base_repository import BaseRepository


class MarketDataRepository(BaseRepository[RawMarketData]):
    """Repository for raw market data operations."""
    
    def __init__(self, session: Session):
        super().__init__(session, RawMarketData)
    
    def get_by_ticker_and_date_range(
        self,
        ticker_symbol: str,
        start_date: datetime,
        end_date: datetime,
        source: Optional[str] = None
    ) -> List[RawMarketData]:
        """
        Get market data for a ticker within date range.
        
        Args:
            ticker_symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start datetime
            end_date: End datetime
            source: Optional data source filter
            
        Returns:
            List of market data records
        """
        query = self.session.query(RawMarketData).join(Ticker).filter(
            and_(
                Ticker.symbol == ticker_symbol,
                RawMarketData.timestamp >= start_date,
                RawMarketData.timestamp <= end_date
            )
        )
        
        if source:
            query = query.filter(RawMarketData.source == source)
        
        return query.order_by(RawMarketData.timestamp).all()
    
    def get_latest_timestamp_for_ticker(
        self,
        ticker_symbol: str,
        source: Optional[str] = None
    ) -> Optional[datetime]:
        """
        Get the most recent timestamp for a ticker.
        
        Args:
            ticker_symbol: Stock symbol
            source: Optional data source filter
            
        Returns:
            Latest timestamp or None
        """
        query = self.session.query(RawMarketData.timestamp).join(Ticker).filter(
            Ticker.symbol == ticker_symbol
        )
        
        if source:
            query = query.filter(RawMarketData.source == source)
        
        result = query.order_by(desc(RawMarketData.timestamp)).first()
        return result[0] if result else None
    
    def check_data_exists(
        self,
        ticker_symbol: str,
        timestamp: datetime,
        source: str
    ) -> bool:
        """
        Check if data already exists for ticker/timestamp/source.
        
        Args:
            ticker_symbol: Stock symbol
            timestamp: Data timestamp
            source: Data source
            
        Returns:
            True if exists, False otherwise
        """
        count = self.session.query(RawMarketData).join(Ticker).filter(
            and_(
                Ticker.symbol == ticker_symbol,
                RawMarketData.timestamp == timestamp,
                RawMarketData.source == source
            )
        ).count()
        
        return count > 0