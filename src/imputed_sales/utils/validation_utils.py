"""
Validation utilities for the Imputed Sales Analytics Platform.

This module provides functions for data validation and quality checks.
"""

import re
import logging
from typing import Dict, List, Optional, Union, Tuple, Any, Callable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, DataType, StringType
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Common validation patterns
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_PATTERN = r'^\+?[1-9]\d{1,14}$'  # E.164 format
ZIP_CODE_PATTERN = r'^\d{5}(-\d{4})?$'  # US ZIP codes
SSN_PATTERN = r'^\d{3}-\d{2}-\d{4}$'  # Social Security Number

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass

def validate_dataframe(
    df: DataFrame,
    schema: Optional[Union[StructType, Dict[str, str]]] = None,
    required_columns: Optional[List[str]] = None,
    not_null_columns: Optional[List[str]] = None,
    unique_columns: Optional[List[str]] = None,
    value_constraints: Optional[Dict[str, Dict[str, Any]]] = None,
    custom_validations: Optional[List[Tuple[str, Callable[[DataFrame], bool], str]]] = None,
    raise_on_error: bool = True
) -> Dict[str, Any]:
    """
    Validate a DataFrame against various constraints.
    
    Args:
        df: Input DataFrame to validate
        schema: Expected schema (StructType or dict of column: type)
        required_columns: List of columns that must be present
        not_null_columns: List of columns that cannot contain nulls
        unique_columns: List of columns that should have unique values
        value_constraints: Dict of {column: {constraint: value, ...}}
        custom_validations: List of (name, validation_function, error_message) tuples
        raise_on_error: Whether to raise an exception on validation failure
        
    Returns:
        Dict containing validation results
        
    Raises:
        ValidationError: If validation fails and raise_on_error is True
    """
    results = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'summary': {}
    }
    
    # Check required columns
    if required_columns:
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            error_msg = f"Missing required columns: {', '.join(missing)}"
            results['errors'].append(error_msg)
            results['summary']['missing_columns'] = missing
    
    # Check schema
    if schema:
        schema_errors = []
        
        if isinstance(schema, dict):
            for col, expected_type in schema.items():
                if col in df.columns:
                    actual_type = df.schema[col].dataType.simpleString()
                    if actual_type.lower() != expected_type.lower():
                        schema_errors.append(
                            f"Column '{col}': expected {expected_type}, got {actual_type}"
                        )
        
        elif isinstance(schema, StructType):
            for field in schema.fields:
                if field.name in df.columns:
                    actual_type = df.schema[field.name].dataType.simpleString()
                    expected_type = field.dataType.simpleString()
                    if actual_type.lower() != expected_type.lower():
                        schema_errors.append(
                            f"Column '{field.name}': expected {expected_type}, got {actual_type}"
                        )
        
        if schema_errors:
            results['errors'].extend(schema_errors)
            results['summary']['schema_errors'] = schema_errors
    
    # Check for null values
    if not_null_columns:
        null_columns = []
        for col in not_null_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    null_columns.append((col, null_count))
        
        if null_columns:
            error_msg = "Found null values in non-nullable columns: " + ", ".join([f"{col} ({count} nulls)" for col, count in null_columns])
            results['errors'].append(error_msg)
            results['summary']['null_violations'] = dict(null_columns)
    
    # Check for duplicate values
    if unique_columns:
        duplicate_columns = []
        for col in unique_columns:
            if col in df.columns:
                total_count = df.count()
                distinct_count = df.select(col).distinct().count()
                if total_count != distinct_count:
                    duplicate_columns.append((col, total_count - distinct_count))
        
        if duplicate_columns:
            error_msg = "Found duplicate values in unique columns: " + ", ".join([f"{col} ({count} duplicates)" for col, count in duplicate_columns])
            results['errors'].append(error_msg)
            results['summary']['duplicate_violations'] = dict(duplicate_columns)
    
    # Check value constraints
    if value_constraints:
        constraint_errors = []
        
        for col, constraints in value_constraints.items():
            if col not in df.columns:
                continue
                
            for constraint, value in constraints.items():
                if constraint == 'min':
                    invalid_count = df.filter(F.col(col) < value).count()
                    if invalid_count > 0:
                        constraint_errors.append(
                            f"Column '{col}': {invalid_count} values below minimum {value}"
                        )
                
                elif constraint == 'max':
                    invalid_count = df.filter(F.col(col) > value).count()
                    if invalid_count > 0:
                        constraint_errors.append(
                            f"Column '{col}': {invalid_count} values above maximum {value}"
                        )
                
                elif constraint == 'values':
                    if not isinstance(value, (list, set, tuple)):
                        value = [value]
                    invalid_count = df.filter(~F.col(col).isin(value)).count()
                    if invalid_count > 0:
                        constraint_errors.append(
                            f"Column '{col}': {invalid_count} values not in {value}"
                        )
                
                elif constraint == 'pattern':
                    if not isinstance(value, str):
                        raise ValueError("Pattern must be a string")
                    invalid_count = df.filter(
                        ~F.col(col).rlike(value)
                    ).count()
                    if invalid_count > 0:
                        constraint_errors.append(
                            f"Column '{col}': {invalid_count} values don't match pattern '{value}'"
                        )
        
        if constraint_errors:
            results['errors'].extend(constraint_errors)
            results['summary']['constraint_errors'] = constraint_errors
    
    # Run custom validations
    if custom_validations:
        for name, validation_func, error_msg in custom_validations:
            try:
                if not validation_func(df):
                    results['errors'].append(f"Custom validation failed: {name} - {error_msg}")
            except Exception as e:
                results['errors'].append(
                    f"Error running custom validation '{name}': {str(e)}"
                )
    
    # Update overall validation status
    results['is_valid'] = len(results['errors']) == 0
    
    # Raise exception if validation failed and raise_on_error is True
    if not results['is_valid'] and raise_on_error:
        raise ValidationError("\n".join(results['errors']))
    
    return results


def validate_email(email_col: Union[str, F.Column]) -> F.Column:
    """
    Validate email addresses in a column.
    
    Args:
        email_col: Column containing email addresses
        
    Returns:
        Boolean column indicating valid emails
    """
    if isinstance(email_col, str):
        email_col = F.col(email_col)
    return email_col.rlike(EMAIL_PATTERN)


def validate_phone(phone_col: Union[str, F.Column]) -> F.Column:
    """
    Validate phone numbers in a column (E.164 format).
    
    Args:
        phone_col: Column containing phone numbers
        
    Returns:
        Boolean column indicating valid phone numbers
    """
    if isinstance(phone_col, str):
        phone_col = F.col(phone_col)
    return phone_col.rlike(PHONE_PATTERN)


def validate_zipcode(zip_col: Union[str, F.Column]) -> F.Column:
    """
    Validate US ZIP codes in a column.
    
    Args:
        zip_col: Column containing ZIP codes
        
    Returns:
        Boolean column indicating valid ZIP codes
    """
    if isinstance(zip_col, str):
        zip_col = F.col(zip_col)
    return zip_col.rlike(ZIP_CODE_PATTERN)


def validate_ssn(ssn_col: Union[str, F.Column]) -> F.Column:
    """
    Validate Social Security Numbers in a column.
    
    Args:
        ssn_col: Column containing SSNs
        
    Returns:
        Boolean column indicating valid SSNs
    """
    if isinstance(ssn_col, str):
        ssn_col = F.col(ssn_col)
    return ssn_col.rlike(SSN_PATTERN)


def check_data_quality(
    df: DataFrame,
    rules: Dict[str, Any],
    group_by: Optional[Union[str, List[str]]] = None
) -> DataFrame:
    """
    Perform data quality checks on a DataFrame.
    
    Args:
        df: Input DataFrame
        rules: Dictionary of validation rules
        group_by: Optional column(s) to group by for aggregated checks
        
    Returns:
        DataFrame with data quality metrics
    """
    from pyspark.sql import functions as F
    
    # Initialize metrics
    metrics = []
    
    # Add row count metric
    metrics.append(F.count('*').alias('row_count'))
    
    # Add null checks for specified columns
    if 'not_null_columns' in rules:
        for col_name in rules['not_null_columns']:
            if col_name in df.columns:
                metrics.append(
                    F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0))
                    .alias(f'null_{col_name}')
                )
    
    # Add distinct value counts for specified columns
    if 'unique_columns' in rules:
        for col_name in rules['unique_columns']:
            if col_name in df.columns:
                metrics.append(
                    F.countDistinct(col_name).alias(f'distinct_{col_name}')
                )
    
    # Add value range checks
    if 'value_ranges' in rules:
        for col_name, range_def in rules['value_ranges'].items():
            if col_name in df.columns:
                col = F.col(col_name)
                
                if 'min' in range_def:
                    metrics.append(
                        F.sum(F.when(col < range_def['min'], 1).otherwise(0))
                        .alias(f'below_min_{col_name}')
                    )
                
                if 'max' in range_def:
                    metrics.append(
                        F.sum(F.when(col > range_def['max'], 1).otherwise(0))
                        .alias(f'above_max_{col_name}')
                    )
    
    # Add pattern matching checks
    if 'patterns' in rules:
        for col_name, pattern in rules['patterns'].items():
            if col_name in df.columns:
                metrics.append(
                    F.sum(F.when(F.col(col_name).rlike(pattern), 0).otherwise(1))
                    .alias(f'invalid_pattern_{col_name}')
                )
    
    # Apply grouping if specified
    if group_by is not None:
        if isinstance(group_by, str):
            group_by = [group_by]
        
        # Only include grouping columns that exist in the DataFrame
        group_cols = [F.col(c) for c in group_by if c in df.columns]
        
        if group_cols:
            return df.groupBy(*group_cols).agg(*metrics)
    
    # If no grouping or no valid group columns, calculate overall metrics
    return df.agg(*metrics)


def generate_quality_report(
    df: DataFrame,
    rules: Dict[str, Any],
    output_format: str = 'pandas'
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Generate a data quality report.
    
    Args:
        df: Input DataFrame
        rules: Dictionary of validation rules
        output_format: Output format ('pandas' or 'dict')
        
    Returns:
        Quality report in the specified format
    """
    # Calculate metrics
    metrics_df = check_data_quality(df, rules)
    
    # Convert to pandas for easier manipulation
    metrics_pd = metrics_df.toPandas().T.reset_index()
    metrics_pd.columns = ['metric', 'value']
    
    # Categorize metrics
    metrics_pd['category'] = metrics_pd['metric'].apply(
        lambda x: 'null_check' if x.startswith('null_') 
        else 'distinct_count' if x.startswith('distinct_')
        else 'range_violation' if x.startswith(('below_min_', 'above_max_'))
        else 'pattern_violation' if x.startswith('invalid_pattern_')
        else 'summary'
    )
    
    # Format results
    if output_format.lower() == 'pandas':
        return metrics_pd
    
    # Convert to dictionary format
    result = {
        'summary': {
            'total_rows': int(metrics_pd[metrics_pd['metric'] == 'row_count']['value'].iloc[0])
        },
        'null_checks': {},
        'distinct_counts': {},
        'range_violations': {},
        'pattern_violations': {}
    }
    
    for _, row in metrics_pd.iterrows():
        metric = row['metric']
        value = int(row['value'])
        
        if metric == 'row_count':
            continue
            
        if row['category'] == 'null_check':
            col_name = metric.replace('null_', '')
            result['null_checks'][col_name] = value
            
        elif row['category'] == 'distinct_count':
            col_name = metric.replace('distinct_', '')
            result['distinct_counts'][col_name] = value
            
        elif row['category'] == 'range_violation':
            col_name = '_'.join(metric.split('_')[2:])
            violation_type = 'below_min' if 'below_min' in metric else 'above_max'
            
            if col_name not in result['range_violations']:
                result['range_violations'][col_name] = {}
                
            result['range_violations'][col_name][violation_type] = value
            
        elif row['category'] == 'pattern_violation':
            col_name = metric.replace('invalid_pattern_', '')
            result['pattern_violations'][col_name] = value
    
    return result
