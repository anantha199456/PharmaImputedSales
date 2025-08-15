"""
Data loading utilities for the Imputed Sales Analytics Platform.

This module provides functionality to load data from various sources
(CSV, Parquet, JDBC, etc.) with support for schema inference and validation.
"""

import os
import logging
from typing import Dict, Any, Optional, Union, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Handles loading data from various sources with support for schema validation
    and data quality checks.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataLoader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config or {}
        self.base_path = self.config.get('data', {}).get('base_dir', 'data')
        
    def load_csv(
        self,
        path: str,
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Load data from a CSV file.
        
        Args:
            path: Path to the CSV file or directory
            schema: Optional schema to enforce
            options: Additional options for CSV reader
            **kwargs: Additional arguments to pass to spark.read.csv()
            
        Returns:
            DataFrame containing the loaded data
        """
        full_path = self._get_full_path(path)
        logger.info(f"Loading CSV data from: {full_path}")
        
        # Default options
        default_options = {
            'header': 'true',
            'inferSchema': 'true',
            'nullValue': '',
            'emptyValue': '',
            'mode': 'PERMISSIVE',
            'encoding': 'UTF-8',
            'quote': '"',
            'escape': '\\',
            'multiLine': 'true',
            'ignoreLeadingWhiteSpace': 'true',
            'ignoreTrailingWhiteSpace': 'true',
            'dateFormat': 'yyyy-MM-dd',
            'timestampFormat': 'yyyy-MM-dd HH:mm:ss'
        }
        
        # Merge with user-provided options
        read_options = {**default_options, **(options or {})}
        
        try:
            reader = self.spark.read.options(**read_options)
            
            if schema:
                reader = reader.schema(schema)
                
            return reader.csv(full_path, **kwargs)
            
        except Exception as e:
            logger.error(f"Failed to load CSV from {full_path}: {str(e)}")
            raise
    
    def load_parquet(
        self,
        path: str,
        options: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Load data from a Parquet file or directory.
        
        Args:
            path: Path to the Parquet file or directory
            options: Additional options for Parquet reader
            **kwargs: Additional arguments to pass to spark.read.parquet()
            
        Returns:
            DataFrame containing the loaded data
        """
        full_path = self._get_full_path(path)
        logger.info(f"Loading Parquet data from: {full_path}")
        
        try:
            reader = self.spark.read
            
            if options:
                reader = reader.options(**options)
                
            return reader.parquet(full_path, **kwargs)
            
        except Exception as e:
            logger.error(f"Failed to load Parquet from {full_path}: {str(e)}")
            raise
    
    def load_table(
        self,
        table_name: str,
        database: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Load data from a Hive table.
        
        Args:
            table_name: Name of the table to load
            database: Optional database name
            options: Additional options for table reader
            
        Returns:
            DataFrame containing the table data
        """
        full_table_name = f"{database}.{table_name}" if database else table_name
        logger.info(f"Loading table: {full_table_name}")
        
        try:
            reader = self.spark.read
            
            if options:
                reader = reader.options(**options)
                
            return reader.table(full_table_name)
            
        except Exception as e:
            logger.error(f"Failed to load table {full_table_name}: {str(e)}")
            raise
    
    def load_jdbc(
        self,
        url: str,
        table: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Load data from a JDBC source.
        
        Args:
            url: JDBC URL
            table: Table name or query
            user: Database username
            password: Database password
            properties: Additional JDBC properties
            **kwargs: Additional arguments to pass to spark.read.jdbc()
            
        Returns:
            DataFrame containing the loaded data
        """
        logger.info(f"Loading data from JDBC: {url}, table: {table}")
        
        try:
            # Create properties
            jdbc_properties = {
                'user': user or '',
                'password': password or '',
                'driver': 'org.postgresql.Driver',  # Default, can be overridden
                **(properties or {})
            }
            
            return self.spark.read.jdbc(
                url=url,
                table=table,
                properties=jdbc_properties,
                **kwargs
            )
            
        except Exception as e:
            logger.error(f"Failed to load data from JDBC {url}, table {table}: {str(e)}")
            raise
    
    def load_data(
        self,
        source_type: str,
        path: str,
        **kwargs
    ) -> DataFrame:
        """
        Generic method to load data based on source type.
        
        Args:
            source_type: Type of source ('csv', 'parquet', 'table', 'jdbc')
            path: Path, table name, or JDBC URL
            **kwargs: Additional arguments specific to the source type
            
        Returns:
            DataFrame containing the loaded data
        """
        source_type = source_type.lower()
        
        if source_type == 'csv':
            return self.load_csv(path, **kwargs)
        elif source_type == 'parquet':
            return self.load_parquet(path, **kwargs)
        elif source_type == 'table':
            return self.load_table(path, **kwargs)
        elif source_type == 'jdbc':
            return self.load_jdbc(path, **kwargs)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _get_full_path(self, path: str) -> str:
        """
        Convert a relative path to a full path using the base directory.
        
        Args:
            path: Relative or absolute path
            
        Returns:
            Full path
        """
        if os.path.isabs(path):
            return path
        return os.path.join(self.base_path, path)
    
    def load_source_data(self, source_config: Dict[str, Any]) -> DataFrame:
        """
        Load data based on a source configuration dictionary.
        
        Args:
            source_config: Dictionary containing source configuration
                - type: Source type ('csv', 'parquet', 'table', 'jdbc')
                - path: Path, table name, or JDBC URL
                - options: Additional options for the reader
                - schema: Optional schema definition
                - **: Additional source-specific parameters
                
        Returns:
            DataFrame containing the loaded data
        """
        source_type = source_config.get('type', 'csv').lower()
        path = source_config.get('path', '')
        options = source_config.get('options', {})
        schema = source_config.get('schema')
        
        # Remove processed keys to avoid duplication
        source_params = {k: v for k, v in source_config.items() 
                        if k not in ['type', 'path', 'options', 'schema']}
        
        # Add schema to options if provided
        if schema and source_type in ['csv', 'json']:
            options['schema'] = schema
        
        # Call the appropriate load method
        if source_type == 'csv':
            return self.load_csv(path, schema=schema, options=options, **source_params)
        elif source_type == 'parquet':
            return self.load_parquet(path, options=options, **source_params)
        elif source_type == 'table':
            return self.load_table(path, options=options, **source_params)
        elif source_type == 'jdbc':
            return self.load_jdbc(url=path, **{**options, **source_params})
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
