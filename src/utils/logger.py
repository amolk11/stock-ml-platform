"""
Centralized Logging Configuration
"""

import logging
import sys
from pathlib import Path
from typing import Optional


class LoggerFactory:
    """Factory for creating standardized loggers."""
    
    _initialized = False
    
    @classmethod
    def setup_logging(
        cls,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        log_format: Optional[str] = None
    ):
        """
        Setup global logging configuration.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional file path for logging
            log_format: Custom log format string
        """
        if cls._initialized:
            return
        
        if log_format is None:
            log_format = (
                '%(asctime)s - %(name)s - %(levelname)s - '
                '%(filename)s:%(lineno)d - %(message)s'
            )
        
        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Add file handler if specified
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
        
        cls._initialized = True
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get a logger instance.
        
        Args:
            name: Logger name (typically __name__)
            
        Returns:
            Logger instance
        """
        return logging.getLogger(name)


# Convenience function
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (use __name__ from calling module)
        
    Returns:
        Configured logger
    """
    return LoggerFactory.get_logger(name)