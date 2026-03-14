"""
Price Features Transformer - Compute price-based features
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
import logging

from src.features.transformers.base_transformer import BaseFeatureTransformer

logger = logging.getLogger(__name__)


class PriceFeaturesTransformer(BaseFeatureTransformer):
    """Compute price-based features like returns and volatility."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.price_config = config.get('price_features', {})
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute price features.
        
        Args:
            df: DataFrame with price data
            
        Returns:
            DataFrame with price features
        """
        self.validate_input(df, ['close'])
        
        result = df.copy()
        result = result.sort_values('timestamp')
        
        logger.info(f"Computing price features for {len(result)} records")
        
        # Returns
        result = self._compute_returns(result)
        
        # Volatility
        result = self._compute_volatility(result)
        
        return result
    
    def _compute_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute returns over various periods."""
        periods = self.price_config.get('returns_periods', [1, 5, 20])
        
        for period in periods:
            # Percentage returns
            df[f'returns_{period}d'] = df['close'].pct_change(periods=period)
        
        return df
    
    def _compute_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute rolling volatility."""
        periods = self.price_config.get('volatility_periods', [20, 60])
        
        # First compute daily returns if not already computed
        if 'returns_1d' not in df.columns:
            df['returns_1d'] = df['close'].pct_change()
        
        for period in periods:
            # Standard deviation of returns
            df[f'volatility_{period}d'] = df['returns_1d'].rolling(window=period).std()
        
        return df
    
    def get_required_history_length(self) -> int:
        """Get minimum history needed."""
        volatility_periods = self.price_config.get('volatility_periods', [60])
        returns_periods = self.price_config.get('returns_periods', [20])
        return max(max(volatility_periods), max(returns_periods))