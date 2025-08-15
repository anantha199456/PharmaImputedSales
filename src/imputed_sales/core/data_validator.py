"""
Data validation utilities for the Imputed Sales Analytics Platform.

This module provides functionality for validating data quality and integrity
using PySpark operations.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, DataType

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass

class DataValidator:
    """
    Handles data validation and quality checks.
    """
    
    def __init__(self, spark):
        """
        Initialize the DataValidator.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def validate_schema(
        self, 
        df: DataFrame, 
        expected_schema: Union[StructType, List[StructField], Dict[str, str]],
        strict: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate that a DataFrame matches an expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected schema as a StructType, list of StructFields,
                           or dictionary of column_name: type_name
            strict: If True, column order must match exactly
            
        Returns:
            Tuple of (is_valid, validation_results)
        """
        results = {
            'valid': True,
            'errors': [],
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        # Convert expected_schema to a dictionary of column_name: data_type
        expected_columns = {}
        
        if isinstance(expected_schema, StructType):
            expected_columns = {field.name: field.dataType for field in expected_schema.fields}
        elif isinstance(expected_schema, list) and all(isinstance(f, StructField) for f in expected_schema):
            expected_columns = {field.name: field.dataType for field in expected_schema}
        elif isinstance(expected_schema, dict):
            # Convert type strings to DataTypes
            from pyspark.sql.types import _parse_datatype_string
            expected_columns = {}
            for col_name, type_str in expected_schema.items():
                try:
                    expected_columns[col_name] = _parse_datatype_string(type_str)
                except Exception as e:
                    error_msg = f"Invalid type '{type_str}' for column '{col_name}': {str(e)}"
                    results['errors'].append(error_msg)
                    results['valid'] = False
        else:
            raise ValueError("expected_schema must be a StructType, list of StructFields, or dict")
        
        if not results['valid']:
            return False, results
        
        actual_columns = set(df.columns)
        expected_column_names = set(expected_columns.keys())
        
        # Check for missing columns
        missing_columns = expected_column_names - actual_columns
        if missing_columns:
            results['missing_columns'] = list(missing_columns)
            results['errors'].append(f"Missing columns: {missing_columns}")
            results['valid'] = False
        
        # Check for extra columns (only in strict mode)
        if strict:
            extra_columns = actual_columns - expected_column_names
            if extra_columns:
                results['extra_columns'] = list(extra_columns)
                results['errors'].append(f"Extra columns: {extra_columns}")
                results['valid'] = False
        
        # Check column types
        for col_name, expected_type in expected_columns.items():
            if col_name not in df.columns:
                continue
                
            actual_type = df.schema[col_name].dataType
            
            if not self._types_match(actual_type, expected_type):
                results['type_mismatches'].append({
                    'column': col_name,
                    'expected': str(expected_type),
                    'actual': str(actual_type)
                })
                results['errors'].append(
                    f"Type mismatch for column '{col_name}': "
                    f"expected {expected_type}, got {actual_type}"
                )
                results['valid'] = False
        
        return results['valid'], results
    
    def _types_match(self, actual_type: DataType, expected_type: DataType) -> bool:
        """Check if the actual type matches the expected type."""
        # For simple type checking, we'll just compare the type names
        # This can be enhanced for more complex type checking
        return str(actual_type).lower() == str(expected_type).lower()
    
    def validate_data_quality(
        self,
        df: DataFrame,
        rules: Dict[str, Any],
        fail_fast: bool = False
    ) -> Dict[str, Any]:
        """
        Validate data quality based on rules.
        
        Args:
            df: DataFrame to validate
            rules: Dictionary of validation rules
                {
                    'not_null': ['col1', 'col2'],
                    'unique': ['id', 'transaction_id'],
                    'value_ranges': {
                        'age': {'min': 0, 'max': 120},
                        'price': {'min': 0}
                    },
                    'allowed_values': {
                        'status': ['active', 'inactive', 'pending'],
                        'category': ['A', 'B', 'C']
                    },
                    'custom_checks': [
                        {
                            'name': 'check_positive_revenue',
                            'condition': 'revenue >= 0',
                            'description': 'Revenue must be non-negative'
                        }
                    ]
                }
            fail_fast: If True, stop after first validation failure
            
        Returns:
            Dictionary with validation results
        """
        results = {
            'valid': True,
            'checks': {},
            'error_count': 0,
            'warnings': []
        }
        
        # Check for null values
        if 'not_null' in rules:
            columns = rules['not_null']
            if not isinstance(columns, list):
                columns = [columns]
                
            for col_name in columns:
                if col_name not in df.columns:
                    results['warnings'].append(f"Column '{col_name}' not found for not_null check")
                    continue
                    
                null_count = df.filter(F.col(col_name).isNull()).count()
                
                results['checks'][f'not_null_{col_name}'] = {
                    'passed': null_count == 0,
                    'null_count': null_count,
                    'total_count': df.count(),
                    'null_percentage': (null_count / df.count() * 100) if df.count() > 0 else 0
                }
                
                if null_count > 0:
                    results['error_count'] += 1
                    results['valid'] = False
                    if fail_fast:
                        return results
        
        # Check for unique values
        if 'unique' in rules:
            columns = rules['unique']
            if not isinstance(columns, list):
                columns = [columns]
                
            for col_name in columns:
                if col_name not in df.columns:
                    results['warnings'].append(f"Column '{col_name}' not found for unique check")
                    continue
                    
                total_count = df.count()
                unique_count = df.select(col_name).distinct().count()
                is_unique = (total_count == unique_count)
                
                results['checks'][f'unique_{col_name}'] = {
                    'passed': is_unique,
                    'duplicate_count': total_count - unique_count,
                    'total_count': total_count,
                    'duplicate_percentage': ((total_count - unique_count) / total_count * 100) 
                                        if total_count > 0 else 0
                }
                
                if not is_unique:
                    results['error_count'] += 1
                    results['valid'] = False
                    if fail_fast:
                        return results
        
        # Check value ranges
        if 'value_ranges' in rules:
            for col_name, range_def in rules['value_ranges'].items():
                if col_name not in df.columns:
                    results['warnings'].append(f"Column '{col_name}' not found for value range check")
                    continue
                    
                condition = []
                if 'min' in range_def:
                    condition.append(f"{col_name} >= {range_def['min']}")
                if 'max' in range_def:
                    condition.append(f"{col_name} <= {range_def['max']}")
                
                if not condition:
                    continue
                    
                full_condition = " AND ".join(condition)
                invalid_count = df.filter(f"NOT({full_condition})").count()
                
                results['checks'][f'value_range_{col_name}'] = {
                    'passed': invalid_count == 0,
                    'invalid_count': invalid_count,
                    'total_count': df.count(),
                    'invalid_percentage': (invalid_count / df.count() * 100) if df.count() > 0 else 0,
                    'condition': full_condition
                }
                
                if invalid_count > 0:
                    results['error_count'] += 1
                    results['valid'] = False
                    if fail_fast:
                        return results
        
        # Check allowed values
        if 'allowed_values' in rules:
            for col_name, allowed in rules['allowed_values'].items():
                if col_name not in df.columns:
                    results['warnings'].append(f"Column '{col_name}' not found for allowed values check")
                    continue
                    
                if not isinstance(allowed, list):
                    allowed = [allowed]
                    
                # Create condition to check if value is in allowed list
                if not allowed:
                    continue
                    
                # Handle different data types in the allowed values
                if all(isinstance(x, str) for x in allowed):
                    allowed_condition = ", ".join([f"'{v}'" for v in allowed])
                else:
                    allowed_condition = ", ".join([str(v) for v in allowed])
                
                invalid_count = df.filter(
                    f"{col_name} IS NOT NULL AND {col_name} NOT IN ({allowed_condition})"
                ).count()
                
                results['checks'][f'allowed_values_{col_name}'] = {
                    'passed': invalid_count == 0,
                    'invalid_count': invalid_count,
                    'total_count': df.count(),
                    'invalid_percentage': (invalid_count / df.count() * 100) if df.count() > 0 else 0,
                    'allowed_values': allowed
                }
                
                if invalid_count > 0:
                    results['error_count'] += 1
                    results['valid'] = False
                    if fail_fast:
                        return results
        
        # Run custom checks
        if 'custom_checks' in rules:
            for check in rules['custom_checks']:
                check_name = check.get('name', 'unnamed_check')
                condition = check.get('condition')
                
                if not condition:
                    results['warnings'].append(f"No condition specified for custom check: {check_name}")
                    continue
                
                try:
                    invalid_count = df.filter(f"NOT({condition})").count()
                    
                    results['checks'][f'custom_{check_name}'] = {
                        'passed': invalid_count == 0,
                        'invalid_count': invalid_count,
                        'total_count': df.count(),
                        'invalid_percentage': (invalid_count / df.count() * 100) if df.count() > 0 else 0,
                        'condition': condition,
                        'description': check.get('description', '')
                    }
                    
                    if invalid_count > 0:
                        results['error_count'] += 1
                        results['valid'] = False
                        if fail_fast:
                            return results
                            
                except Exception as e:
                    error_msg = f"Error executing custom check '{check_name}': {str(e)}"
                    results['warnings'].append(error_msg)
                    results['checks'][f'custom_{check_name}'] = {
                        'error': error_msg,
                        'passed': False
                    }
                    results['valid'] = False
                    
                    if fail_fast:
                        return results
        
        return results
    
    def generate_validation_report(
        self,
        validation_results: Dict[str, Any],
        output_format: str = 'text'
    ) -> str:
        """
        Generate a human-readable validation report.
        
        Args:
            validation_results: Results from validate_data_quality()
            output_format: Output format ('text' or 'dict')
            
        Returns:
            Formatted validation report
        """
        if output_format == 'dict':
            return validation_results
            
        # Text format
        report = []
        report.append("=" * 80)
        report.append("DATA VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Status: {'PASSED' if validation_results['valid'] else 'FAILED'}")
        report.append(f"Checks Run: {len(validation_results.get('checks', {}))}")
        report.append(f"Errors Found: {validation_results.get('error_count', 0)}")
        
        if validation_results.get('warnings'):
            report.append("\nWARNINGS:")
            for warning in validation_results['warnings']:
                report.append(f"  - {warning}")
        
        if validation_results.get('checks'):
            report.append("\nCHECK RESULTS:")
            for check_name, result in validation_results['checks'].items():
                status = "PASSED" if result.get('passed', False) else "FAILED"
                report.append(f"\n{check_name}: {status}")
                
                if 'description' in result:
                    report.append(f"  Description: {result['description']}")
                    
                if 'condition' in result:
                    report.append(f"  Condition: {result['condition']}")
                    
                if 'invalid_count' in result:
                    report.append(
                        f"  Invalid: {result['invalid_count']:,} "
                        f"({result.get('invalid_percentage', 0):.2f}%)"
                    )
                
                if 'error' in result:
                    report.append(f"  Error: {result['error']}")
        
        if not validation_results['valid']:
            report.append("\nVALIDATION FAILED: Some checks did not pass")
        
        report.append("\n" + "=" * 80)
        
        return "\n".join(report)
