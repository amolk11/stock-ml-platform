"""
Data Ingestion Pipeline - Executable pipeline for data collection
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
from datetime import datetime, timedelta
import logging

from src.utils.config_loader import load_config
from src.utils.logger import LoggerFactory
from src.database.connection import db_manager
from src.data_ingestion.orchestrator import DataIngestionOrchestrator

logger = logging.getLogger(__name__)


def run_historical_collection(
    symbols: list,
    lookback_days: int,
    sources: list = None
):
    """
    Run historical data collection.
    
    Args:
        symbols: List of ticker symbols
        lookback_days: Number of days to look back
        sources: List of sources to use
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    logger.info(f"Collecting {lookback_days} days of historical data")
    
    orchestrator = DataIngestionOrchestrator()
    summary = orchestrator.collect_historical_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        sources=sources
    )
    
    logger.info(f"Collection Summary: {summary}")
    return summary


def run_latest_collection(symbols: list):
    """
    Run latest data collection.
    
    Args:
        symbols: List of ticker symbols
    """
    logger.info("Collecting latest market data")
    
    orchestrator = DataIngestionOrchestrator()
    summary = orchestrator.collect_latest_data(symbols)
    
    logger.info(f"Collection Summary: {summary}")
    return summary


def main():
    """Main pipeline execution."""
    parser = argparse.ArgumentParser(
        description="Data Ingestion Pipeline"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=['historical', 'latest'],
        default='historical',
        help="Collection mode"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="configs/data_ingestion.yaml",
        help="Path to ingestion configuration"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Days of historical data (overrides config)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    LoggerFactory.setup_logging(
        log_level="INFO",
        log_file="logs/data_ingestion.log"
    )
    
    try:
        logger.info("=" * 80)
        logger.info("DATA INGESTION PIPELINE STARTED")
        logger.info("=" * 80)
        
        # Load configuration
        config = load_config(args.config)
        ingestion_config = config.get('data_ingestion', {})
        
        # Initialize database
        db_manager.initialize()
        
        # Get symbols from config
        symbols = ingestion_config.get('tickers', [])
        if not symbols:
            raise ValueError("No tickers configured in data_ingestion.yaml")
        
        # Get sources
        sources = ingestion_config.get('sources', None)
        
        # Run collection based on mode
        if args.mode == 'historical':
            lookback_days = args.lookback_days or ingestion_config['collection'].get('lookback_days', 30)
            summary = run_historical_collection(symbols, lookback_days, sources)
        else:
            summary = run_latest_collection(symbols)
        
        logger.info("=" * 80)
        logger.info("DATA INGESTION PIPELINE COMPLETED")
        logger.info(f"Records inserted: {summary['total_records_inserted']}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db_manager.dispose()


if __name__ == "__main__":
    main()