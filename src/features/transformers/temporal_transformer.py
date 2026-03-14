"""
Temporal Features Transformer - Extract time-based features
"""

import pandas as pd
from typing import Dict, Any
import logging

from src.features.transformers.base_transformer import BaseFeatureTransformer

logger = logging.getLogger(__name__)


class TemporalFeaturesTransformer(BaseFeatureTransformer):
    """Extract temporal features from timestamps."""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract temporal features.
        
        Args:
            df: DataFrame with timestamp column
            
        Returns:
            DataFrame with temporal features
        """
        self.validate_input(df, ['timestamp'])
        
        result = df.copy()
        
        logger.info(f"Computing temporal features for {len(result)} records")
        
        # Extract components
        result['day_of_week'] = pd.to_datetime(result['timestamp']).dt.dayofweek
        result['day_of_month'] = pd.to_datetime(result['timestamp']).dt.day
        result['week_of_year'] = pd.to_datetime(result['timestamp']).dt.isocalendar().week
        result['month'] = pd.to_datetime(result['timestamp']).dt.month
        result['quarter'] = pd.to_datetime(result['timestamp']).dt.quarter
        result['year'] = pd.to_datetime(result['timestamp']).dt.year
        
        # Is month start/end
        result['is_month_start'] = pd.to_datetime(result['timestamp']).dt.is_month_start.astype(int)
        result['is_month_end'] = pd.to_datetime(result['timestamp']).dt.is_month_end.astype(int)
        result['is_quarter_start'] = pd.to_datetime(result['timestamp']).dt.is_quarter_start.astype(int)
        result['is_quarter_end'] = pd.to_datetime(result['timestamp']).dt.is_quarter_end.astype(int)
        
        return result