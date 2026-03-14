"""
Feature Engineer - Orchestrates feature computation
"""

from typing import Dict, Any, List
import pandas as pd
import logging

from src.features.transformers.technical_transformer import TechnicalIndicatorsTransformer
from src.features.transformers.price_transformer import PriceFeaturesTransformer
from src.features.transformers.temporal_transformer import TemporalFeaturesTransformer
from src.utils.config_loader import load_config

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Orchestrates feature engineering across multiple transformers."""
    
    def __init__(self, config_path: str = "configs/feature_engineering.yaml"):
        """
        Initialize feature engineer.
        
        Args:
            config_path: Path to feature engineering configuration
        """
        self.config = load_config(config_path)
        self.feature_config = self.config.get('feature_engineering', {})
        self.version = self.feature_config.get('version', 'v1.0')
        
        # Initialize transformers
        self.transformers = {}
        self._initialize_transformers()
        
        logger.info(f"Feature Engineer initialized (version: {self.version})")
    
    def _initialize_transformers(self):
        """Initialize all enabled feature transformers."""
        feature_groups = self.feature_config.get('feature_groups', [])
        
        transformer_mapping = {
            'technical_indicators': TechnicalIndicatorsTransformer,
            'price_features': PriceFeaturesTransformer,
            'temporal_features': TemporalFeaturesTransformer,
        }
        
        for group in feature_groups:
            if group in transformer_mapping:
                transformer_class = transformer_mapping[group]
                self.transformers[group] = transformer_class(self.feature_config)
                logger.info(f"Initialized {group} transformer")
    
    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute all features for input data.
        
        Args:
            df: DataFrame with raw market data
                Must have: timestamp, open, high, low, close, volume
            
        Returns:
            DataFrame with all computed features
        """
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return df
        
        logger.info(f"Computing features for {len(df)} records")
        
        # Start with copy of original data
        result = df.copy()
        
        # Ensure sorted by timestamp
        result = result.sort_values('timestamp').reset_index(drop=True)
        
        # Apply each transformer
        for name, transformer in self.transformers.items():
            try:
                logger.info(f"Applying {name} transformer...")
                result = transformer.transform(result)
            except Exception as e:
                logger.error(f"Error in {name} transformer: {e}")
                raise
        
        # Add feature version
        result['feature_version'] = self.version
        
        logger.info(f"Feature computation complete. Columns: {len(result.columns)}")
        
        return result
    
    def get_required_history_length(self) -> int:
        """Get minimum historical records needed for all features."""
        max_length = 0
        
        for transformer in self.transformers.values():
            length = transformer.get_required_history_length()
            max_length = max(max_length, length)
        
        return max_length
    
    def get_feature_columns(self) -> List[str]:
        """
        Get list of all feature column names that will be generated.
        
        Returns:
            List of feature column names
        """
        # This is a helper for documentation/testing
        # In practice, run on sample data to get actual columns
        feature_columns = [
            # Technical indicators
            'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_200',
            'ema_12', 'ema_26',
            'rsi_14',
            'macd', 'macd_signal', 'macd_histogram',
            'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
            'atr_14',
            'volume_sma_20', 'obv',
            
            # Price features
            'returns_1d', 'returns_5d', 'returns_20d',
            'volatility_20d', 'volatility_60d',
            
            # Temporal features
            'day_of_week', 'day_of_month', 'week_of_year',
            'month', 'quarter', 'year',
            'is_month_start', 'is_month_end',
            'is_quarter_start', 'is_quarter_end',
            
            # Metadata
            'feature_version'
        ]
        
        return feature_columns