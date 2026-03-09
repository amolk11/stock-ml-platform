"""
Configuration Loader - Load YAML configs with validation
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from functools import lru_cache


class ConfigLoader:
    """Load and validate YAML configuration files."""
    
    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        """
        Load a YAML configuration file.
        
        Args:
            config_path: Path to YAML file
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML is malformed
        """
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, 'r') as f:
            try:
                config = yaml.safe_load(f)
                return config
            except yaml.YAMLError as e:
                raise ValueError(f"Error parsing YAML file {config_path}: {e}")
    
    @staticmethod
    @lru_cache(maxsize=32)
    def load_cached(config_path: str) -> Dict[str, Any]:
        """
        Load config with caching (useful for frequently accessed configs).
        
        Args:
            config_path: Path to YAML file
            
        Returns:
            Cached configuration dictionary
        """
        return ConfigLoader.load(config_path)


# Convenience function
def load_config(config_path: str, cached: bool = False) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        cached: Whether to use cached version
        
    Returns:
        Configuration dictionary
    """
    if cached:
        return ConfigLoader.load_cached(config_path)
    return ConfigLoader.load(config_path)