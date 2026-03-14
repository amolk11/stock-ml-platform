"""
Feature Store - Manages feature storage in database
"""

from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.orm import Session
import logging

from src.database.models import TechnicalFeature, Ticker
from src.database.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class FeatureStore:
    """Manages storing and retrieving features from database."""
    
    def __init__(self, session: Session):
        """
        Initialize feature store.
        
        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.repo = BaseRepository(session, TechnicalFeature)
    
    def save_features(
        self,
        ticker_symbol: str,
        features_df: pd.DataFrame,
        deduplicate: bool = True
    ) -> int:
        """
        Save computed features to database.
        
        Args:
            ticker_symbol: Stock ticker symbol
            features_df: DataFrame with computed features
            deduplicate: Whether to check for existing features
            
        Returns:
            Number of features saved
        """
        # Get ticker_id
        ticker = self.session.query(Ticker).filter(
            Ticker.symbol == ticker_symbol
        ).first()
        
        if not ticker:
            raise ValueError(f"Ticker {ticker_symbol} not found in database")
        
        ticker_id = ticker.ticker_id
        
        # Prepare records for insertion
        records = []
        
        for _, row in features_df.iterrows():
            # Check if already exists (if deduplicate enabled)
            if deduplicate:
                exists = self.session.query(TechnicalFeature).filter(
                    TechnicalFeature.ticker_id == ticker_id,
                    TechnicalFeature.timestamp == row['timestamp'],
                    TechnicalFeature.feature_version == row.get('feature_version', 'v1.0')
                ).first()
                
                if exists:
                    continue
            
            # Build record
            record = {
                'ticker_id': ticker_id,
                'timestamp': row['timestamp'],
                'feature_version': row.get('feature_version', 'v1.0'),
            }
            
            # Add all feature columns (handle None/NaN)
            feature_columns = [
                'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_200',
                'ema_12', 'ema_26',
                'rsi_14',
                'macd', 'macd_signal', 'macd_histogram',
                'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                'atr_14',
                'volume_sma_20', 'obv',
                'returns_1d', 'returns_5d', 'returns_20d',
                'volatility_20d',
            ]
            
            for col in feature_columns:
                if col in row:
                    value = row[col]
                    # Convert NaN to None for database
                    record[col] = None if pd.isna(value) else float(value)
            
            records.append(record)
        
        # Bulk insert
        if records:
            count = self.repo.bulk_insert(records)
            logger.info(f"Saved {count} feature records for {ticker_symbol}")
            return count
        else:
            logger.info(f"No new features to save for {ticker_symbol}")
            return 0
    
    def load_features(
        self,
        ticker_symbol: str,
        feature_version: str = None
    ) -> pd.DataFrame:
        """
        Load features from database.
        
        Args:
            ticker_symbol: Stock ticker symbol
            feature_version: Optional feature version filter
            
        Returns:
            DataFrame with features
        """
        query = self.session.query(TechnicalFeature).join(Ticker).filter(
            Ticker.symbol == ticker_symbol
        )
        
        if feature_version:
            query = query.filter(TechnicalFeature.feature_version == feature_version)
        
        query = query.order_by(TechnicalFeature.timestamp)
        
        features = query.all()
        
        if not features:
            logger.warning(f"No features found for {ticker_symbol}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        data = []
        for feature in features:
            record = {
                'timestamp': feature.timestamp,
                'sma_5': feature.sma_5,
                'sma_10': feature.sma_10,
                'sma_20': feature.sma_20,
                'sma_50': feature.sma_50,
                'sma_200': feature.sma_200,
                'ema_12': feature.ema_12,
                'ema_26': feature.ema_26,
                'rsi_14': feature.rsi_14,
                'macd': feature.macd,
                'macd_signal': feature.macd_signal,
                'macd_histogram': feature.macd_histogram,
                'bollinger_upper': feature.bollinger_upper,
                'bollinger_middle': feature.bollinger_middle,
                'bollinger_lower': feature.bollinger_lower,
                'atr_14': feature.atr_14,
                'volume_sma_20': feature.volume_sma_20,
                'obv': feature.obv,
                'returns_1d': feature.returns_1d,
                'returns_5d': feature.returns_5d,
                'returns_20d': feature.returns_20d,
                'volatility_20d': feature.volatility_20d,
                'feature_version': feature.feature_version,
            }
            data.append(record)
        
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} feature records for {ticker_symbol}")
        
        return df