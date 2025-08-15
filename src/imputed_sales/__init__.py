"""
Imputed Sales Analytics Platform

A comprehensive PySpark-based solution for processing and analyzing
pharmaceutical sales data with advanced imputation techniques.
"""

__version__ = "1.0.0"
__author__ = "Your Name <your.email@example.com>"
__license__ = "MIT"

# Import key components for easier access
from .core.spark_manager import SparkManager
from .core.data_loader import DataLoader
from .core.data_processor import DataProcessor
from .core.data_validator import DataValidator
from .etl.base_pipeline import BasePipeline
from .etl.sales_pipeline import SalesPipeline
from .etl.inventory_pipeline import InventoryPipeline

# Define public API
__all__ = [
    'SparkManager',
    'DataLoader',
    'DataProcessor',
    'DataValidator',
    'BasePipeline',
    'SalesPipeline',
    'InventoryPipeline',
]
