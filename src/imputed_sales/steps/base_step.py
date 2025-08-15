"""
Base class for all ETL steps in the Imputed Sales pipeline.

This module provides a base class that all step implementations should inherit from.
It includes common functionality for logging, configuration, and data processing.
"""

import os
import sys
import logging
import yaml
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class BaseStep(ABC):
    """Base class for all ETL steps in the pipeline."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the base step.
        
        Args:
            config: Optional configuration dictionary. If not provided, will be loaded from config file.
        """
        self.config = config or self._load_config()
        self.spark = self._init_spark_session()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Set up paths
        self.base_path = self.config.get('base_path', '.')
        self.source_data_path = self.config.get('source_data_path', os.path.join(self.base_path, 'source_data'))
        self.processed_data_path = self.config.get('processed_data_path', os.path.join(self.base_path, 'processed_data'))
        self.checkpoint_path = self.config.get('checkpoint_path', os.path.join(self.base_path, 'checkpoints'))
        
        # Ensure output directories exist
        os.makedirs(self.processed_data_path, exist_ok=True)
        os.makedirs(self.checkpoint_path, exist_ok=True)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file.
        
        Returns:
            Dictionary containing the configuration.
        """
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'config',
            'app',
            'base.yaml'
        )
        
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load config file: {e}. Using defaults.")
            return {}
    
    def _init_spark_session(self) -> SparkSession:
        """Initialize and return a Spark session.
        
        Returns:
            Configured SparkSession instance.
        """
        return SparkSession.builder \
            .appName(self.__class__.__name__) \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    @abstractmethod
    def extract(self) -> Dict[str, DataFrame]:
        """Extract data from source systems.
        
        Returns:
            Dictionary of DataFrames containing the extracted data.
        """
        pass
    
    @abstractmethod
    def transform(self, data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Transform the extracted data.
        
        Args:
            data: Dictionary of DataFrames from the extract step.
                
        Returns:
            Dictionary of transformed DataFrames.
        """
        pass
    
    @abstractmethod
    def load(self, data: Dict[str, DataFrame]) -> None:
        """Load the transformed data to the target system.
        
        Args:
            data: Dictionary of DataFrames from the transform step.
        """
        pass
    
    def run(self) -> bool:
        """Run the ETL step.
        
        Returns:
            True if the step completed successfully, False otherwise.
        """
        try:
            self.logger.info(f"Starting {self.__class__.__name__}")
            
            # Execute ETL steps
            extracted_data = self.extract()
            transformed_data = self.transform(extracted_data)
            self.load(transformed_data)
            
            self.logger.info(f"Completed {self.__class__.__name__} successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}: {str(e)}", exc_info=True)
            return False
        finally:
            # Clean up Spark resources
            if hasattr(self, 'spark'):
                self.spark.stop()
    
    def read_csv(self, filename: str, **kwargs) -> DataFrame:
        """Helper method to read a CSV file with the appropriate schema.
        
        Args:
            filename: Name of the CSV file to read.
            **kwargs: Additional arguments to pass to spark.read.csv()
            
        Returns:
            DataFrame containing the CSV data.
        """
        filepath = os.path.join(self.source_data_path, filename)
        schema = self._get_schema(filename)
        
        return self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(filepath, **kwargs)
    
    def _get_schema(self, filename: str) -> 'pyspark.sql.types.StructType':
        """Get the schema for a given data file.
        
        Args:
            filename: Name of the data file.
                
        Returns:
            Spark schema for the specified file.
        """
        # Import here to avoid circular imports
        from imputed_sales.utils.schema_utils import get_schema
        return get_schema(filename)
    
    def write_output(self, df: DataFrame, output_name: str, mode: str = "overwrite") -> None:
        """Write the output DataFrame to the processed data directory.
        
        Args:
            df: DataFrame to write.
            output_name: Name of the output file (without extension).
            mode: Write mode (default: "overwrite").
        """
        output_path = os.path.join(self.processed_data_path, f"{output_name}.parquet")
        df.write.mode(mode).parquet(output_path)
        self.logger.info(f"Wrote output to {output_path}")


class GlueCompatibleStep(BaseStep):
    """Base class for steps that need to be compatible with AWS Glue."""
    
    def __init__(self, args: Optional[Dict[str, Any]] = None, **kwargs):
        """Initialize the Glue-compatible step.
        
        Args:
            args: Command line arguments (from Glue).
            **kwargs: Additional keyword arguments for the base class.
        """
        self.args = args or {}
        super().__init__(**kwargs)
        
        # Override paths with Glue-specific paths if provided
        if 'S3_SOURCE_PATH' in self.args:
            self.source_data_path = self.args['S3_SOURCE_PATH']
        if 'S3_OUTPUT_PATH' in self.args:
            self.processed_data_path = self.args['S3_OUTPUT_PATH']
        if 'S3_CHECKPOINT_PATH' in self.args:
            self.checkpoint_path = self.args['S3_CHECKPOINT_PATH']
    
    def _init_spark_session(self) -> SparkSession:
        """Initialize and return a Spark session for Glue."""
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        
        # Apply any Glue-specific configurations
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
        
        return spark
