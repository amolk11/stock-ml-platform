"""
Configuration Loader - Load YAML configs with environment variable support
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any
from functools import lru_cache
from dotenv import load_dotenv


class ConfigLoader:
    """Load and validate YAML configuration files."""

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        """
        Load a YAML configuration file with environment variable expansion.

        Args:
            config_path: Path to YAML file

        Returns:
            Dictionary containing configuration
        """

        # Load environment variables from .env
        load_dotenv()

        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, "r") as f:
            try:
                # Read file
                content = f.read()

                # Replace ${ENV_VAR} with actual environment variables
                content = os.path.expandvars(content)

                # Parse YAML
                config = yaml.safe_load(content)

                return config

            except yaml.YAMLError as e:
                raise ValueError(f"Error parsing YAML file {config_path}: {e}")

    @staticmethod
    @lru_cache(maxsize=32)
    def load_cached(config_path: str) -> Dict[str, Any]:
        """Load config with caching."""
        return ConfigLoader.load(config_path)


# Convenience function
def load_config(config_path: str, cached: bool = False) -> Dict[str, Any]:
    if cached:
        return ConfigLoader.load_cached(config_path)
    return ConfigLoader.load(config_path)