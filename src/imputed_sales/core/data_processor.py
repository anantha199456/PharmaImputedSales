"""
Data processing utilities for the Imputed Sales Analytics Platform.

This module provides functionality for transforming, cleaning, and processing
data using PySpark operations.
"""

import logging
from typing import Dict, Any, List, Optional, Union, Callable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DataType

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Handles data transformation and processing operations.
    """
    
    def __init__(self, spark):
        """
        Initialize the DataProcessor.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def transform(self, df: DataFrame, transformations: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply a series of transformations to a DataFrame.
        
        Args:
            df: Input DataFrame
            transformations: List of transformation definitions
                [
                    {
                        'type': 'select',
                        'columns': ['col1', 'col2', ...]
                    },
                    {
                        'type': 'filter',
                        'condition': 'col1 > 10'
                    },
                    ...
                ]
                
        Returns:
            Transformed DataFrame
        """
        result_df = df
        
        for transform in transformations:
            transform_type = transform.get('type', '').lower()
            
            if transform_type == 'select':
                columns = transform.get('columns', [])
                if columns:
                    result_df = result_df.select(*columns)
                    
            elif transform_type == 'filter':
                condition = transform.get('condition')
                if condition:
                    result_df = result_df.filter(condition)
                    
            elif transform_type == 'with_column':
                column_name = transform.get('name')
                expression = transform.get('expression')
                if column_name and expression:
                    result_df = result_df.withColumn(column_name, F.expr(expression))
                    
            elif transform_type == 'drop':
                columns = transform.get('columns', [])
                if columns:
                    result_df = result_df.drop(*columns)
                    
            elif transform_type == 'rename':
                columns = transform.get('columns', {})
                if columns:
                    for old_name, new_name in columns.items():
                        result_df = result_df.withColumnRenamed(old_name, new_name)
                        
            elif transform_type == 'fill_na':
                value = transform.get('value')
                subset = transform.get('subset')
                if value is not None:
                    result_df = result_df.fillna(value, subset=subset)
                    
            elif transform_type == 'drop_duplicates':
                subset = transform.get('subset')
                result_df = result_df.dropDuplicates(subset=subset)
                
            elif transform_type == 'join':
                other_df = transform.get('other_df')
                on = transform.get('on')
                how = transform.get('how', 'inner')
                if other_df and on:
                    result_df = result_df.join(other_df, on=on, how=how)
                    
            elif transform_type == 'group_by':
                group_cols = transform.get('group_cols', [])
                aggs = transform.get('aggs', {})
                if group_cols and aggs:
                    result_df = result_df.groupBy(*group_cols).agg(**aggs)
                    
            elif transform_type == 'window':
                window_spec = transform.get('window_spec')
                aggs = transform.get('aggs', {})
                if window_spec and aggs:
                    window = Window.partitionBy(window_spec.get('partitionBy', [])) \
                                 .orderBy(window_spec.get('orderBy', [])) \
                                 .rowsBetween(window_spec.get('start', -1), 
                                           window_spec.get('end', 0))
                    
                    for col_name, (func, *args) in aggs.items():
                        result_df = result_df.withColumn(col_name, func().over(window))
                        
            elif transform_type == 'custom':
                func = transform.get('function')
                if callable(func):
                    result_df = func(result_df)
                    
        return result_df
    
    def clean_data(self, df: DataFrame, cleaning_rules: Dict[str, Any]) -> DataFrame:
        """
        Clean data based on cleaning rules.
        
        Args:
            df: Input DataFrame
            cleaning_rules: Dictionary of cleaning rules
                {
                    'drop_duplicates': {
                        'subset': ['col1', 'col2']
                    },
                    'fill_na': {
                        'columns': {
                            'col1': 0,
                            'col2': 'unknown'
                        }
                    },
                    'cast_types': {
                        'col1': 'int',
                        'col2': 'string'
                    },
                    'rename_columns': {
                        'old_name': 'new_name'
                    },
                    'drop_columns': ['col1', 'col2']
                }
                
        Returns:
            Cleaned DataFrame
        """
        result_df = df
        
        # Drop duplicates
        if 'drop_duplicates' in cleaning_rules:
            subset = cleaning_rules['drop_duplicates'].get('subset')
            result_df = result_df.dropDuplicates(subset=subset)
        
        # Fill NA values
        if 'fill_na' in cleaning_rules:
            fill_values = cleaning_rules['fill_na'].get('columns', {})
            for col_name, value in fill_values.items():
                result_df = result_df.fillna(value, subset=[col_name])
        
        # Cast column types
        if 'cast_types' in cleaning_rules:
            type_mapping = {
                'int': 'integer',
                'integer': 'integer',
                'string': 'string',
                'double': 'double',
                'float': 'float',
                'boolean': 'boolean',
                'date': 'date',
                'timestamp': 'timestamp'
            }
            
            for col_name, type_name in cleaning_rules['cast_types'].items():
                spark_type = type_mapping.get(type_name.lower(), 'string')
                result_df = result_df.withColumn(
                    col_name, 
                    F.col(col_name).cast(spark_type)
                )
        
        # Rename columns
        if 'rename_columns' in cleaning_rules:
            for old_name, new_name in cleaning_rules['rename_columns'].items():
                result_df = result_df.withColumnRenamed(old_name, new_name)
        
        # Drop columns
        if 'drop_columns' in cleaning_rules:
            columns_to_drop = [col for col in cleaning_rules['drop_columns'] 
                             if col in result_df.columns]
            if columns_to_drop:
                result_df = result_df.drop(*columns_to_drop)
        
        return result_df
    
    def apply_business_rules(self, df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply business rules to the DataFrame.
        
        Args:
            df: Input DataFrame
            rules: List of business rules to apply
                [
                    {
                        'name': 'rule_name',
                        'condition': 'col1 > 10',  # Condition to check
                        'action': 'set_flag',      # Action to take
                        'params': {                # Action parameters
                            'column': 'flag_col',
                            'value': True
                        }
                    },
                    ...
                ]
                
        Returns:
            DataFrame with business rules applied
        """
        result_df = df
        
        for rule in rules:
            rule_name = rule.get('name', 'unnamed_rule')
            condition = rule.get('condition')
            action = rule.get('action', '').lower()
            params = rule.get('params', {})
            
            logger.info(f"Applying business rule: {rule_name}")
            
            if not condition:
                logger.warning(f"No condition specified for rule: {rule_name}")
                continue
                
            if action == 'set_flag':
                column = params.get('column')
                value = params.get('value')
                
                if not column:
                    logger.warning(f"No column specified for set_flag action in rule: {rule_name}")
                    continue
                    
                result_df = result_df.withColumn(
                    column,
                    F.when(F.expr(condition), value)
                     .otherwise(F.col(column) if column in result_df.columns else F.lit(None))
                )
                
            elif action == 'transform':
                column = params.get('column')
                value_expr = params.get('value')
                
                if not column or not value_expr:
                    logger.warning(f"Missing column or value expression in transform action for rule: {rule_name}")
                    continue
                    
                result_df = result_df.withColumn(
                    column,
                    F.when(F.expr(condition), F.expr(value_expr))
                     .otherwise(F.col(column))
                )
                
            elif action == 'filter':
                keep = params.get('keep', True)
                
                if keep:
                    result_df = result_df.filter(F.expr(condition))
                else:
                    result_df = result_df.filter(~F.expr(condition))
            
            elif action == 'custom' and 'function' in params:
                custom_func = params['function']
                if callable(custom_func):
                    result_df = custom_func(result_df, condition, params)
            
        return result_df
