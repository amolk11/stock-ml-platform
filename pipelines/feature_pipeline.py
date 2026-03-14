"""
Feature Engineering Pipeline - Compute and store features
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
from datetime import datetime
import logging

from src.utils.config_loader import load_config
from src.utils.logger import LoggerFactory
from src.database.connection import db_manager
from src.database.models import Ticker, RawMarketData
from src.features.feature_engineer import FeatureEngineer
from src.features.feature_store import FeatureStore

logger = logging.getLogger(__name__)


def load_raw_data_for_ticker(session, ticker_symbol: str) -> pd.DataFrame:
    """
    Load raw market data for a ticker from database.
    
    Args:
        session: Database session
        ticker_symbol: Stock ticker symbol
        
    Returns:
        DataFrame with raw market data
    """
    # Query raw market data
    query = session.query(RawMarketData).join(Ticker).filter(
        Ticker.symbol == ticker_symbol
    ).order_by(RawMarketData.timestamp)
    
    data = query.all()
    
    if not data:
        logger.warning(f"No raw data found for {ticker_symbol}")
        return pd.DataFrame()
    
    # Convert to DataFrame
    records = []
    for record in data:
        records.append({
            'timestamp': record.timestamp,
            'open': float(record.open),
            'high': float(record.high),
            'low': float(record.low),
            'close': float(record.close),
            'volume': int(record.volume),
            'adjusted_close': float(record.adjusted_close) if record.adjusted_close else float(record.close),
        })
    
    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df)} raw records for {ticker_symbol}")
    
    return df


def run_feature_pipeline(symbols: List[str], config: Dict):
    """
    Run feature engineering for multiple symbols.
    
    Args:
        symbols: List of ticker symbols
        config: Configuration dictionary
    """
    logger.info(f"Running feature pipeline for {len(symbols)} symbols")
    
    # Initialize feature engineer
    engineer = FeatureEngineer()
    min_history = engineer.get_required_history_length()
    
    logger.info(f"Minimum history required: {min_history} records")
    
    total_features_saved = 0
    
    with db_manager.get_session() as session:
        feature_store = FeatureStore(session)
        
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol}...")
                
                # Load raw data
                raw_data = load_raw_data_for_ticker(session, symbol)
                
                if raw_data.empty:
                    logger.warning(f"Skipping {symbol} - no data")
                    continue
                
                if len(raw_data) < min_history:
                    logger.warning(
                        f"Skipping {symbol} - insufficient history "
                        f"({len(raw_data)} < {min_history})"
                    )
                    continue
                
                # Compute features
                features = engineer.compute_features(raw_data)
                
                # Save to database
                saved_count = feature_store.save_features(
                    ticker_symbol=symbol,
                    features_df=features,
                    deduplicate=True
                )
                
                total_features_saved += saved_count
                logger.info(f"Completed {symbol}: {saved_count} features saved")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}", exc_info=True)
    
    logger.info(f"Feature pipeline complete. Total features saved: {total_features_saved}")
    return total_features_saved


def main():
    """Main pipeline execution."""
    parser = argparse.ArgumentParser(description="Feature Engineering Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/feature_engineering.yaml",
        help="Path to feature engineering configuration"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    LoggerFactory.setup_logging(
        log_level="INFO",
        log_file="logs/feature_engineering.log"
    )
    
    try:
        logger.info("=" * 80)
        logger.info("FEATURE ENGINEERING PIPELINE STARTED")
        logger.info("=" * 80)
        
        # Load configuration
        config = load_config(args.config)
        
        # Initialize database
        db_manager.initialize()
        
        # Get symbols from data ingestion config
        ingestion_config = load_config("configs/data_ingestion.yaml")
        symbols = ingestion_config.get('data_ingestion', {}).get('tickers', [])
        
        if not symbols:
            raise ValueError("No tickers configured")
        
        # Run feature pipeline
        total_saved = run_feature_pipeline(symbols, config)
        
        logger.info("=" * 80)
        logger.info("FEATURE ENGINEERING PIPELINE COMPLETED")
        logger.info(f"Total features saved: {total_saved}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db_manager.dispose()


if __name__ == "__main__":
    main()