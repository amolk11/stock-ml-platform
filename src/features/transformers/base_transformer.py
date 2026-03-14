"""
Base Feature Transformer - Abstract class for all feature transformers
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseFeatureTransformer(ABC):
    """
    Abstract base class for feature transformers.
    
    All feature engineering components should inherit from this.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.name = self.__class__.__name__
        logger.info(f"Initialized {self.name}")
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data into features.
        
        Args:
            df: DataFrame with raw market data
            
        Returns:
            DataFrame with computed features
        """
        pass
    
    def validate_input(self, df: pd.DataFrame, required_columns: list):
        """
        Validate input DataFrame has required columns.
        
        Args:
            df: Input DataFrame
            required_columns: List of required column names
            
        Raises:
            ValueError: If required columns are missing
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
    
    def get_required_history_length(self) -> int:
        """
        Get minimum number of historical records needed.
        
        Returns:
            Minimum history length
        """
        return 1  # Override in subclasses if needed