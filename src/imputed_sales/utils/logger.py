"""
Logging configuration for the Imputed Sales Analytics Platform.

This module provides a centralized logging configuration with support for
console and file logging, log rotation, and structured logging.
"""

import os
import sys
import logging
import logging.config
import logging.handlers
from pathlib import Path
from typing import Dict, Any, Optional
import json
import time
from datetime import datetime

# Default log format
DEFAULT_LOG_FORMAT = (
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
    '[%(filename)s:%(lineno)d]'
)

# Default log file path
DEFAULT_LOG_DIR = 'logs'
DEFAULT_LOG_FILE = 'imputed_sales.log'

# Log level mapping
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Structured log formatter
class StructuredFormatter(logging.Formatter):
    """Log formatter that outputs structured JSON logs."""
    
    def format(self, record):
        """Format the log record as JSON."""
        log_record = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'process': record.process,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        
        # Add any extra attributes
        if hasattr(record, 'extra') and record.extra:
            log_record.update(record.extra)
        
        return json.dumps(log_record, ensure_ascii=False)


def setup_logging(
    log_level: str = 'INFO',
    log_file: Optional[str] = None,
    log_dir: str = DEFAULT_LOG_DIR,
    log_format: str = DEFAULT_LOG_FORMAT,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    enable_console: bool = True,
    enable_file: bool = True,
    json_format: bool = False,
    log_config: Optional[Dict[str, Any]] = None
) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Log file name (will be placed in log_dir)
        log_dir: Directory to store log files
        log_format: Log message format
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup log files to keep
        enable_console: Whether to enable console logging
        enable_file: Whether to enable file logging
        json_format: Whether to use JSON format for logs
        log_config: Optional dictionary with logging configuration
    """
    # Use provided config if available
    if log_config:
        logging.config.dictConfig(log_config)
        return
    
    # Normalize log level
    log_level = log_level.upper()
    if log_level not in LOG_LEVELS:
        log_level = 'INFO'
    
    # Set up log directory
    if enable_file and log_file:
        log_path = Path(log_dir) / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create formatters
    if json_format:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(log_format)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVELS[log_level])
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if enable_file and log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Configure third-party loggers
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    
    # Log configuration
    logger.info("Logging configured (level=%s, file=%s)", log_level, log_file or 'disabled')


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def log_execution_time(logger: logging.Logger):
    """
    Decorator to log the execution time of a function.
    
    Args:
        logger: Logger instance to use for logging
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = None
            
            try:
                logger.debug("Starting %s", func.__name__)
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.error("Error in %s: %s", func.__name__, str(e), exc_info=True)
                raise
            finally:
                duration = time.time() - start_time
                logger.info(
                    "Completed %s in %.2f seconds", 
                    func.__name__, 
                    duration
                )
        return wrapper
    return decorator


def log_dataframe_info(logger: logging.Logger, df_name: str):
    """
    Decorator to log DataFrame information before and after a function.
    
    Args:
        logger: Logger instance to use for logging
        df_name: Name of the DataFrame parameter
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Log input DataFrame info
            if df_name in kwargs:
                df = kwargs[df_name]
            else:
                # Find DataFrame in args
                df = next((arg for arg in args if hasattr(arg, 'count')), None)
            
            if df is not None:
                logger.debug(
                    "%s - Input DataFrame: %d rows, %d columns", 
                    func.__name__, 
                    df.count(),
                    len(df.columns)
                )
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Log output DataFrame info
            if result is not None and hasattr(result, 'count'):
                logger.debug(
                    "%s - Output DataFrame: %d rows, %d columns", 
                    func.__name__,
                    result.count(),
                    len(result.columns)
                )
            
            return result
        return wrapper
    return decorator


# Default logger for the module
logger = get_logger(__name__)

# Initialize logging with default configuration
setup_logging()

# Export commonly used functions
__all__ = [
    'setup_logging',
    'get_logger',
    'log_execution_time',
    'log_dataframe_info',
    'logger'
]
