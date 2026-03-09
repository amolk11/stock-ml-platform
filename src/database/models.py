"""
SQLAlchemy ORM Models - Database table definitions
"""

from sqlalchemy import (
    Column, Integer, String, DECIMAL, BigInteger, DateTime, 
    Boolean, Text, JSON, Enum, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()


class Ticker(Base):
    """Stock ticker master table."""
    
    __tablename__ = 'tickers'
    
    ticker_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, unique=True, index=True)
    company_name = Column(String(255))
    sector = Column(String(100), index=True)
    industry = Column(String(100))
    exchange = Column(String(50))
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Ticker(symbol='{self.symbol}', company='{self.company_name}')>"


class RawMarketData(Base):
    """Raw OHLCV market data from APIs."""
    
    __tablename__ = 'raw_market_data'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey('tickers.ticker_id'), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    open = Column(DECIMAL(12, 4), nullable=False)
    high = Column(DECIMAL(12, 4), nullable=False)
    low = Column(DECIMAL(12, 4), nullable=False)
    close = Column(DECIMAL(12, 4), nullable=False)
    volume = Column(BigInteger, nullable=False)
    adjusted_close = Column(DECIMAL(12, 4))
    source = Column(String(50), nullable=False, index=True)
    data_quality_score = Column(DECIMAL(3, 2))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('ticker_id', 'timestamp', 'source', 
                        name='unique_ticker_timestamp_source'),
        Index('idx_ticker_timestamp', 'ticker_id', 'timestamp'),
    )
    
    def __repr__(self):
        return f"<RawMarketData(ticker_id={self.ticker_id}, timestamp='{self.timestamp}')>"


class TechnicalFeature(Base):
    """Engineered technical indicator features."""
    
    __tablename__ = 'technical_features'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey('tickers.ticker_id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    
    # Moving Averages
    sma_5 = Column(DECIMAL(12, 4))
    sma_10 = Column(DECIMAL(12, 4))
    sma_20 = Column(DECIMAL(12, 4))
    sma_50 = Column(DECIMAL(12, 4))
    sma_200 = Column(DECIMAL(12, 4))
    ema_12 = Column(DECIMAL(12, 4))
    ema_26 = Column(DECIMAL(12, 4))
    
    # Momentum Indicators
    rsi_14 = Column(DECIMAL(5, 2))
    macd = Column(DECIMAL(12, 4))
    macd_signal = Column(DECIMAL(12, 4))
    macd_histogram = Column(DECIMAL(12, 4))
    
    # Volatility Indicators
    bollinger_upper = Column(DECIMAL(12, 4))
    bollinger_middle = Column(DECIMAL(12, 4))
    bollinger_lower = Column(DECIMAL(12, 4))
    atr_14 = Column(DECIMAL(12, 4))
    
    # Volume Indicators
    volume_sma_20 = Column(BigInteger)
    obv = Column(BigInteger)
    
    # Price Action
    returns_1d = Column(DECIMAL(8, 6))
    returns_5d = Column(DECIMAL(8, 6))
    returns_20d = Column(DECIMAL(8, 6))
    volatility_20d = Column(DECIMAL(8, 6))
    
    feature_version = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('ticker_id', 'timestamp', 'feature_version',
                        name='unique_ticker_timestamp_version'),
        Index('idx_ticker_timestamp', 'ticker_id', 'timestamp'),
    )


class DataQualityMetrics(Base):
    """Track data quality over time."""
    
    __tablename__ = 'data_quality_metrics'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    check_timestamp = Column(DateTime, nullable=False)
    table_name = Column(String(100), nullable=False)
    total_records = Column(BigInteger)
    null_count = Column(JSON)
    duplicate_count = Column(Integer)
    anomaly_count = Column(Integer)
    quality_score = Column(DECIMAL(5, 4))
    issues = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_table_timestamp', 'table_name', 'check_timestamp'),
    )