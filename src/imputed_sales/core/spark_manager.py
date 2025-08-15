"""
Spark session management for the Imputed Sales Analytics Platform.

This module provides a singleton SparkSession manager that handles the creation,
configuration, and teardown of Spark sessions with appropriate optimizations.
"""

import os
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf

logger = logging.getLogger(__name__)

class SparkManager:
    """
    Manages the SparkSession lifecycle and configuration.
    
    Implements the singleton pattern to ensure only one SparkSession is active.
    """
    _instance = None
    _spark_session = None
    
    def __new__(cls, config: Optional[Dict[str, Any]] = None):
        """Create or get the singleton instance."""
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
            cls._instance._initialize(config)
        return cls._instance
    
    def _initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the Spark manager with configuration."""
        self.config = config or {}
        self._initialize_logging()
        self._create_spark_session()
    
    def _initialize_logging(self) -> None:
        """Initialize logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def _create_spark_session(self) -> None:
        """Create and configure the SparkSession."""
        if self._spark_session is not None:
            logger.warning("SparkSession already exists. Reusing existing session.")
            return
            
        logger.info("Creating new SparkSession...")
        
        # Create Spark configuration
        conf = SparkConf()
        
        # Set default configurations
        default_config = {
            'spark.app.name': 'ImputedSalesAnalytics',
            'spark.master': 'local[*]',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '4g',
            'spark.sql.shuffle.partitions': '8',
            'spark.default.parallelism': '8',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.skewJoin.enabled': 'true',
            'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
            'spark.speculation': 'true',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.shuffle.service.enabled': 'true',
            'spark.sql.legacy.timeParserPolicy': 'LEGACY',
            'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY',
            'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY',
            'spark.sql.legacy.json.allowEmptyString.enabled': 'true',
        }
        
        # Update with user-provided configurations
        spark_config = self.config.get('spark', {})
        
        # Apply configurations
        for key, value in {**default_config, **spark_config}.items():
            if value is not None:
                conf.set(key, str(value))
        
        # Create the SparkSession
        self._spark_session = (
            SparkSession.builder
            .config(conf=conf)
            .enableHiveSupport()
            .getOrCreate()
        )
        
        # Set log level
        log_level = self.config.get('app', {}).get('log_level', 'WARN').upper()
        self._spark_session.sparkContext.setLogLevel(log_level)
        
        logger.info(f"SparkSession created successfully with app name: {conf.get('spark.app.name')}")
    
    @property
    def spark(self) -> SparkSession:
        """Get the active SparkSession."""
        if self._spark_session is None:
            raise RuntimeError("SparkSession has not been initialized.")
        return self._spark_session
    
    def get_session(self) -> SparkSession:
        """Get the active SparkSession (alias for spark property)."""
        return self.spark
    
    def stop(self) -> None:
        """Stop the active SparkSession."""
        if self._spark_session is not None:
            logger.info("Stopping SparkSession...")
            self._spark_session.stop()
            self._spark_session = None
            logger.info("SparkSession stopped.")
    
    def __del__(self) -> None:
        """Ensure SparkSession is stopped when the manager is garbage collected."""
        self.stop()
    
    @classmethod
    def get_instance(cls) -> 'SparkManager':
        """Get the singleton instance of SparkManager."""
        if cls._instance is None:
            raise RuntimeError("SparkManager has not been initialized. Call SparkManager(config) first.")
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (for testing purposes)."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None


def get_spark_session() -> SparkSession:
    """Convenience function to get the active SparkSession."""
    return SparkManager.get_instance().spark
