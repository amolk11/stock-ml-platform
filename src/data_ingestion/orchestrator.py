"""
Data Ingestion Orchestrator - Coordinates data collection from multiple sources
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
import logging

from src.data_ingestion.api_clients.base_client import BaseAPIClient
from src.data_ingestion.api_clients.yahoo_client import YahooFinanceClient
from src.data_ingestion.api_clients.alpha_vantage_client import AlphaVantageClient
from src.data_ingestion.validators.schema_validator import SchemaValidator
from src.data_ingestion.validators.quality_checker import DataQualityChecker
from src.database.connection import db_manager
from src.database.models import Ticker, RawMarketData
from src.database.repositories.market_data_repository import MarketDataRepository
from src.utils.config_loader import load_config

logger = logging.getLogger(__name__)


class DataIngestionOrchestrator:
    """Orchestrates data collection from multiple API sources."""
    
    def __init__(
        self,
        ingestion_config_path: str = "configs/data_ingestion.yaml",
        api_config_path: str = "configs/api_sources.yaml"
    ):
        """
        Initialize the orchestrator.
        
        Args:
            ingestion_config_path: Path to ingestion configuration
            api_config_path: Path to API sources configuration
        """
        self.ingestion_config = load_config(ingestion_config_path)
        self.api_config = load_config(api_config_path)
        
        # Initialize API clients
        self.clients: Dict[str, BaseAPIClient] = {}
        self._initialize_clients()
        
        logger.info("Data Ingestion Orchestrator initialized")
    
    def _initialize_clients(self):
        """Initialize all enabled API clients."""
        api_sources = self.api_config.get('api_sources', {})
        
        # Map source names to client classes
        client_mapping = {
            'yahoo_finance': YahooFinanceClient,
            'alpha_vantage': AlphaVantageClient,
        }
        
        for source_name, client_class in client_mapping.items():
            if source_name in api_sources:
                config = api_sources[source_name]
                if config.get('enabled', False):
                    try:
                        client = client_class(config)
                        self.clients[source_name] = client
                        logger.info(f"Initialized {source_name} client")
                    except Exception as e:
                        logger.error(f"Failed to initialize {source_name}: {e}")
    
    def _ensure_ticker_exists(self, session, symbol: str) -> int:
        """
        Ensure ticker exists in database, create if not.
        
        Args:
            session: Database session
            symbol: Stock ticker symbol
            
        Returns:
            ticker_id
        """
        ticker = session.query(Ticker).filter(Ticker.symbol == symbol).first()
        
        if not ticker:
            ticker = Ticker(
                symbol=symbol,
                company_name=symbol,  # Will be updated later with real name
                is_active=True
            )
            session.add(ticker)
            session.flush()
            logger.info(f"Created ticker: {symbol}")
        
        return ticker.ticker_id
    
    def collect_historical_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        sources: List[str] = None
    ) -> Dict[str, Any]:
        """
        Collect historical data for multiple symbols.
        
        Args:
            symbols: List of stock ticker symbols
            start_date: Start date for historical data
            end_date: End date for historical data
            sources: List of source names to use (None = use all)
            
        Returns:
            Collection summary
        """
        if sources is None:
            sources = list(self.clients.keys())
        
        logger.info(
            f"Starting historical data collection for {len(symbols)} symbols "
            f"from {start_date.date()} to {end_date.date()}"
        )
        
        total_records = 0
        total_inserted = 0
        errors = []
        
        with db_manager.get_session() as session:
            repo = MarketDataRepository(session)
            
            for symbol in symbols:
                # Ensure ticker exists
                ticker_id = self._ensure_ticker_exists(session, symbol)
                
                for source_name in sources:
                    if source_name not in self.clients:
                        logger.warning(f"Client {source_name} not available")
                        continue
                    
                    client = self.clients[source_name]
                    
                    try:
                        # Fetch data
                        records = client.fetch_historical_data(
                            symbol, start_date, end_date
                        )
                        
                        if not records:
                            logger.warning(
                                f"No data for {symbol} from {source_name}"
                            )
                            continue
                        
                        # Validate schema
                        validation = SchemaValidator.validate_batch(records)
                        if validation['invalid_records'] > 0:
                            logger.warning(
                                f"Schema validation: {validation['invalid_records']} "
                                f"invalid records for {symbol}"
                            )
                        
                        # Check quality
                        quality_config = self.ingestion_config.get('data_ingestion', {}).get('quality', {})
                        quality_check = DataQualityChecker.check_batch_quality(
                            records,
                            min_quality_score=quality_config.get('min_quality_score', 0.8)
                        )
                        
                        # Filter out invalid records
                        valid_records = [
                            r for r in records
                            if SchemaValidator.validate_record(r)[0]
                        ]
                        
                        # Prepare for insertion
                        insert_records = []
                        for record in valid_records:
                            # Check if already exists
                            exists = repo.check_data_exists(
                                symbol,
                                record['timestamp'],
                                record['source']
                            )
                            
                            if not exists:
                                insert_records.append({
                                    'ticker_id': ticker_id,
                                    'timestamp': record['timestamp'],
                                    'open': record['open'],
                                    'high': record['high'],
                                    'low': record['low'],
                                    'close': record['close'],
                                    'volume': record['volume'],
                                    'adjusted_close': record.get('adjusted_close', record['close']),
                                    'source': record['source'],
                                    'data_quality_score': record.get('data_quality_score', 1.0)
                                })
                        
                        # Bulk insert
                        if insert_records:
                            inserted = repo.bulk_insert(insert_records)
                            total_inserted += inserted
                            logger.info(
                                f"Inserted {inserted} records for {symbol} "
                                f"from {source_name}"
                            )
                        
                        total_records += len(records)
                        
                    except Exception as e:
                        error_msg = f"Error collecting {symbol} from {source_name}: {e}"
                        logger.error(error_msg)
                        errors.append(error_msg)
        
        summary = {
            'total_records_fetched': total_records,
            'total_records_inserted': total_inserted,
            'symbols_processed': len(symbols),
            'sources_used': sources,
            'errors': errors
        }
        
        logger.info(
            f"Collection complete: {total_inserted} records inserted "
            f"from {total_records} fetched"
        )
        
        return summary
    
    def collect_latest_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Collect latest market data for symbols.
        
        Args:
            symbols: List of stock ticker symbols
            
        Returns:
            Collection summary
        """
        logger.info(f"Collecting latest data for {len(symbols)} symbols")
        
        # Use today and yesterday as date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        return self.collect_historical_data(symbols, start_date, end_date)