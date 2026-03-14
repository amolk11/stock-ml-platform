"""
Technical Indicators Transformer - Compute technical analysis features
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
import logging

from src.features.transformers.base_transformer import BaseFeatureTransformer

logger = logging.getLogger(__name__)


class TechnicalIndicatorsTransformer(BaseFeatureTransformer):
    """Compute technical indicator features."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.tech_config = config.get('technical_indicators', {})
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute all technical indicators.
        
        Args:
            df: DataFrame with OHLCV data (must have: open, high, low, close, volume)
            
        Returns:
            DataFrame with technical indicator features
        """
        self.validate_input(df, ['open', 'high', 'low', 'close', 'volume'])
        
        # Make a copy to avoid modifying original
        result = df.copy()
        
        # Sort by timestamp to ensure correct calculation
        result = result.sort_values('timestamp')
        
        logger.info(f"Computing technical indicators for {len(result)} records")
        
        # Moving Averages
        result = self._compute_sma(result)
        result = self._compute_ema(result)
        
        # Momentum Indicators
        result = self._compute_rsi(result)
        result = self._compute_macd(result)
        
        # Volatility Indicators
        result = self._compute_bollinger_bands(result)
        result = self._compute_atr(result)
        
        # Volume Indicators
        result = self._compute_volume_sma(result)
        result = self._compute_obv(result)
        
        return result
    
    def _compute_sma(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Simple Moving Averages."""
        periods = self.tech_config.get('sma_periods', [5, 10, 20, 50, 200])
        
        for period in periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
        
        return df
    
    def _compute_ema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Exponential Moving Averages."""
        periods = self.tech_config.get('ema_periods', [12, 26])
        
        for period in periods:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        
        return df
    
    def _compute_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Relative Strength Index."""
        rsi_config = self.tech_config.get('rsi', {})
        period = rsi_config.get('period', 14)
        
        # Calculate price changes
        delta = df['close'].diff()
        
        # Separate gains and losses
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # Calculate average gain and loss
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        return df
    
    def _compute_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute MACD (Moving Average Convergence Divergence)."""
        macd_config = self.tech_config.get('macd', {})
        fast_period = macd_config.get('fast_period', 12)
        slow_period = macd_config.get('slow_period', 26)
        signal_period = macd_config.get('signal_period', 9)
        
        # Calculate MACD line
        ema_fast = df['close'].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow_period, adjust=False).mean()
        df['macd'] = ema_fast - ema_slow
        
        # Calculate signal line
        df['macd_signal'] = df['macd'].ewm(span=signal_period, adjust=False).mean()
        
        # Calculate histogram
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        return df
    
    def _compute_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Bollinger Bands."""
        bb_config = self.tech_config.get('bollinger_bands', {})
        period = bb_config.get('period', 20)
        std_dev = bb_config.get('std_dev', 2)
        
        # Middle band (SMA)
        df['bollinger_middle'] = df['close'].rolling(window=period).mean()
        
        # Standard deviation
        rolling_std = df['close'].rolling(window=period).std()
        
        # Upper and lower bands
        df['bollinger_upper'] = df['bollinger_middle'] + (rolling_std * std_dev)
        df['bollinger_lower'] = df['bollinger_middle'] - (rolling_std * std_dev)
        
        return df
    
    def _compute_atr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Average True Range."""
        atr_config = self.tech_config.get('atr', {})
        period = atr_config.get('period', 14)
        
        # True Range calculation
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        # ATR is the moving average of True Range
        df['atr_14'] = true_range.rolling(window=period).mean()
        
        return df
    
    def _compute_volume_sma(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Volume Simple Moving Average."""
        periods = self.tech_config.get('volume_sma_periods', [20])
        
        for period in periods:
            df[f'volume_sma_{period}'] = df['volume'].rolling(window=period).mean()
        
        return df
    
    def _compute_obv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute On-Balance Volume."""
        # OBV calculation: cumulative sum of volume when price increases
        obv = []
        obv_value = 0
        
        for i in range(len(df)):
            if i == 0:
                obv.append(df.iloc[i]['volume'])
            else:
                if df.iloc[i]['close'] > df.iloc[i-1]['close']:
                    obv_value += df.iloc[i]['volume']
                elif df.iloc[i]['close'] < df.iloc[i-1]['close']:
                    obv_value -= df.iloc[i]['volume']
                obv.append(obv_value)
        
        df['obv'] = obv
        
        return df
    
    def get_required_history_length(self) -> int:
        """Get minimum history needed for all indicators."""
        sma_periods = self.tech_config.get('sma_periods', [200])
        return max(sma_periods) if sma_periods else 200