"""Logging utilities for Excel to Bronze application."""
import logging
import sys
from typing import Optional

from excel_to_bronze.config import config


def setup_logging(name: Optional[str] = None, level: Optional[str] = None) -> logging.Logger:
    """Set up logging with consistent format.
    
    Args:
        name: Logger name (defaults to module name)
        level: Logging level (defaults to config setting)
        
    Returns:
        Configured logger instance
    """
    # Get name from caller if not provided
    if name is None:
        name = sys._getframe(1).f_globals.get('__name__')
    
    # Get level from config if not provided
    if level is None:
        level = config.get_application_config()["log_level"]
    
    # Convert level string to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Configure logger
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    
    # Only add handler if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger