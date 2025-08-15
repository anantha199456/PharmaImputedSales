"""
Spark utilities for the Imputed Sales Analytics Platform.

This module provides helper functions and extensions for working with PySpark.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import (
    StructType, StructField, DataType, 
    StringType, IntegerType, DoubleType, DateType,
    TimestampType, BooleanType, ArrayType, MapType
)
import json
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def create_spark_session(
    app_name: str = "ImputedSalesApp",
    master: str = "local[*]",
    config: Optional[Dict[str, str]] = None,
    enable_hive: bool = False,
    log_level: str = "WARN"
) -> SparkSession:
    """
    Create and configure a SparkSession.
    
    Args:
        app_name: Application name
        master: Spark master URL
        config: Additional Spark configuration options
        enable_hive: Whether to enable Hive support
        log_level: Log level for Spark logs
        
    Returns:
        Configured SparkSession
    """
    from pyspark.sql import SparkSession
    
    # Default configuration
    default_config = {
        'spark.app.name': app_name,
        'spark.master': master,
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
    
    # Merge with user-provided config
    spark_config = {**default_config, **(config or {})}
    
    # Create SparkSession builder
    builder = SparkSession.builder.appName(app_name)
    
    # Apply configuration
    for key, value in spark_config.items():
        if value is not None:
            builder = builder.config(key, value)
    
    # Enable Hive support if needed
    if enable_hive:
        builder = builder.enableHiveSupport()
    
    # Create the session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(log_level)
    
    logger.info(f"Created Spark session with app name: {app_name}")
    return spark


def read_data(
    spark: SparkSession,
    path: str,
    format: str = "parquet",
    schema: Optional[Union[StructType, str]] = None,
    options: Optional[Dict[str, str]] = None,
    **kwargs
) -> DataFrame:
    """
    Read data from a file or directory.
    
    Args:
        spark: SparkSession
        path: Path to the data
        format: File format (parquet, csv, json, etc.)
        schema: Optional schema
        options: Additional options for the reader
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        DataFrame containing the data
    """
    reader = spark.read.format(format)
    
    if schema:
        if isinstance(schema, str):
            # If schema is a JSON string, parse it
            try:
                schema = StructType.fromJson(json.loads(schema))
            except json.JSONDecodeError:
                # If not valid JSON, assume it's a DDL string
                schema = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromDDL(schema).dataType()
        reader = reader.schema(schema)
    
    if options:
        reader = reader.options(**options)
    
    return reader.load(path, **kwargs)


def write_data(
    df: DataFrame,
    path: str,
    format: str = "parquet",
    mode: str = "overwrite",
    partition_by: Optional[Union[str, List[str]]] = None,
    options: Optional[Dict[str, str]] = None,
    **kwargs
) -> None:
    """
    Write data to a file or directory.
    
    Args:
        df: DataFrame to write
        path: Output path
        format: File format (parquet, csv, json, etc.)
        mode: Write mode (overwrite, append, ignore, error, errorifexists)
        partition_by: Columns to partition by
        options: Additional options for the writer
        **kwargs: Additional arguments to pass to the writer
    """
    writer = df.write.format(format).mode(mode)
    
    if options:
        writer = writer.options(**options)
    
    if partition_by:
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path, **kwargs)
    logger.info(f"Data written to {path} in {format} format")


def show_df_info(df: DataFrame, n: int = 5, truncate: bool = True) -> None:
    """
    Print DataFrame schema and sample data.
    
    Args:
        df: DataFrame to display
        n: Number of rows to show
        truncate: Whether to truncate long strings
    """
    print("\n=== DataFrame Schema ===")
    df.printSchema()
    
    print(f"\n=== First {n} Rows ===")
    df.show(n, truncate=truncate)
    
    print("\n=== Summary ===")
    print(f"Number of rows: {df.count():,}")
    print(f"Number of columns: {len(df.columns)}")
    
    # Show column statistics for numeric columns
    numeric_cols = [
        f.name for f in df.schema.fields 
        if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType))
    ]
    
    if numeric_cols:
        print("\nNumeric Column Statistics:")
        df.select(numeric_cols).summary().show(truncate=False)


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Add metadata columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with added metadata columns
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    return df.withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", lit(""))


def cast_columns(df: DataFrame, column_types: Dict[str, str]) -> DataFrame:
    """
    Cast DataFrame columns to specified types.
    
    Args:
        df: Input DataFrame
        column_types: Dictionary mapping column names to target types
        
    Returns:
        DataFrame with columns cast to specified types
    """
    from pyspark.sql import functions as F
    
    type_mapping = {
        'string': 'string',
        'int': 'int',
        'integer': 'int',
        'bigint': 'bigint',
        'long': 'bigint',
        'double': 'double',
        'float': 'float',
        'boolean': 'boolean',
        'date': 'date',
        'timestamp': 'timestamp',
        'array<string>': 'array<string>',
        'map<string,string>': 'map<string,string>'
    }
    
    for col_name, type_name in column_types.items():
        if col_name in df.columns:
            spark_type = type_mapping.get(type_name.lower(), 'string')
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
    
    return df


def deduplicate_dataframe(
    df: DataFrame,
    key_columns: List[str],
    order_by: Optional[Union[str, List[str]]] = None,
    keep: str = 'first'
) -> DataFrame:
    """
    Remove duplicate rows from a DataFrame based on key columns.
    
    Args:
        df: Input DataFrame
        key_columns: Columns to use for identifying duplicates
        order_by: Columns to determine which row to keep
        keep: Which duplicate to keep ('first' or 'last')
        
    Returns:
        Deduplicated DataFrame
    """
    if not order_by:
        order_by = df.columns
    
    if isinstance(order_by, str):
        order_by = [order_by]
    
    # Create window specification
    window_spec = Window.partitionBy(key_columns).orderBy(
        [F.desc(col) for col in order_by] if keep == 'first' else 
        [F.asc(col) for col in order_by]
    )
    
    # Add row number and filter
    return (df
            .withColumn('_row_num', F.row_number().over(window_spec))
            .filter(F.col('_row_num') == 1)
            .drop('_row_num'))


def join_dataframes(
    left: DataFrame,
    right: DataFrame,
    on: Union[str, List[str], List[Tuple[str, str]]],
    how: str = 'inner',
    suffix: str = '_right'
) -> DataFrame:
    """
    Join two DataFrames with automatic handling of duplicate column names.
    
    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Columns to join on (can be a column name, list of column names,
            or list of (left_col, right_col) tuples)
        how: Type of join (inner, outer, left, right, leftsemi)
        suffix: Suffix to add to duplicate column names from the right
        
    Returns:
        Joined DataFrame
    """
    # Handle different types of join conditions
    if isinstance(on, str):
        left_on = right_on = on
    elif isinstance(on, (list, tuple)) and all(isinstance(x, str) for x in on):
        left_on = right_on = on
    elif isinstance(on, (list, tuple)) and all(isinstance(x, (list, tuple)) for x in on):
        left_on = [x[0] for x in on]
        right_on = [x[1] for x in on]
    else:
        raise ValueError("Invalid join condition. Must be a column name, list of column names, "
                        "or list of (left_col, right_col) tuples.")
    
    # Get duplicate column names (excluding join keys)
    left_cols = set(left.columns)
    right_cols = set(right.columns)
    common_cols = left_cols.intersection(right_cols)
    
    if isinstance(on, str):
        join_keys = {on}
    elif all(isinstance(x, str) for x in on):
        join_keys = set(on)
    else:
        join_keys = set(x[1] for x in on)  # right side column names
    
    # Columns to rename in the right DataFrame
    cols_to_rename = list(common_cols - join_keys)
    
    # Rename duplicate columns in the right DataFrame
    if cols_to_rename:
        rename_map = {col: f"{col}{suffix}" for col in cols_to_rename}
        right = right.select([
            F.col(col).alias(rename_map.get(col, col)) 
            for col in right.columns
        ])
        
        # Update right_on if we renamed any join keys
        if not isinstance(on, str) and not all(isinstance(x, str) for x in on):
            right_on = [rename_map.get(col, col) for col in right_on]
    
    # Perform the join
    if isinstance(on, str) or all(isinstance(x, str) for x in on):
        return left.join(right, on=on, how=how)
    else:
        # Handle join with different column names
        join_condition = None
        for l, r in zip(left_on, right_on):
            cond = left[l] == right[r]
            join_condition = cond if join_condition is None else join_condition & cond
        
        return left.join(right, join_condition, how)


def add_partition_columns(
    df: DataFrame,
    partition_columns: Dict[str, str]
) -> DataFrame:
    """
    Add partition columns to a DataFrame based on file path.
    
    Args:
        df: Input DataFrame
        partition_columns: Dictionary mapping partition names to column names
            Example: {'year': 'yyyy', 'month': 'MM', 'day': 'dd'}
            
    Returns:
        DataFrame with added partition columns
    """
    from pyspark.sql.functions import input_file_name, regexp_extract
    
    result_df = df
    
    # Add file path column if not already present
    if '_file_path' not in df.columns:
        result_df = df.withColumn('_file_path', input_file_name())
    
    # Add partition columns
    for col_name, pattern in partition_columns.items():
        result_df = result_df.withColumn(
            col_name,
            regexp_extract('_file_path', pattern, 0)
        )
    
    # Remove the temporary file path column
    return result_df.drop('_file_path')


def to_pandas(df: DataFrame, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Convert a Spark DataFrame to a pandas DataFrame.
    
    Args:
        df: Spark DataFrame
        limit: Maximum number of rows to convert (None for all)
        
    Returns:
        pandas DataFrame
    """
    if limit:
        return df.limit(limit).toPandas()
    return df.toPandas()


def from_pandas(spark: SparkSession, pdf: pd.DataFrame) -> DataFrame:
    """
    Convert a pandas DataFrame to a Spark DataFrame.
    
    Args:
        spark: SparkSession
        pdf: pandas DataFrame
        
    Returns:
        Spark DataFrame
    """
    return spark.createDataFrame(pdf)


def get_distinct_values(df: DataFrame, column: str) -> List[Any]:
    """
    Get distinct values from a DataFrame column.
    
    Args:
        df: Input DataFrame
        column: Column name
        
    Returns:
        List of distinct values
    """
    return [row[0] for row in df.select(column).distinct().collect()]


def filter_by_date_range(
    df: DataFrame,
    date_column: str,
    start_date: str,
    end_date: str,
    date_format: str = 'yyyy-MM-dd'
) -> DataFrame:
    """
    Filter DataFrame by date range.
    
    Args:
        df: Input DataFrame
        date_column: Name of the date column
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        date_format: Date format string
        
    Returns:
        Filtered DataFrame
    """
    return df.filter(
        (F.col(date_column) >= F.to_date(F.lit(start_date), date_format)) &
        (F.col(date_column) <= F.to_date(F.lit(end_date), date_format))
    )


def add_missing_columns(
    df: DataFrame,
    required_columns: List[str],
    default_value: Any = None
) -> DataFrame:
    """
    Add missing columns with default values.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        default_value: Default value for missing columns
        
    Returns:
        DataFrame with all required columns
    """
    from pyspark.sql.functions import lit
    
    result_df = df
    
    for col_name in required_columns:
        if col_name not in df.columns:
            result_df = result_df.withColumn(col_name, lit(default_value))
    
    return result_df


def drop_null_columns(df: DataFrame, threshold: float = 0.9) -> DataFrame:
    """
    Drop columns with a high percentage of null values.
    
    Args:
        df: Input DataFrame
        threshold: Maximum allowed percentage of null values (0.0 to 1.0)
        
    Returns:
        DataFrame with columns dropped
    """
    from pyspark.sql.functions import col, count, when, isnan
    
    total_rows = df.count()
    if total_rows == 0:
        return df
    
    # Calculate null percentage for each column
    null_percentages = df.select([
        (count(when(col(c).isNull() | isnan(col(c)), c)) / total_rows).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Drop columns with null percentage above threshold
    cols_to_drop = [
        col_name for col_name, null_pct in null_percentages.items()
        if null_pct >= threshold
    ]
    
    if cols_to_drop:
        logger.info(f"Dropping columns with {threshold*100:.1f}% or more nulls: {', '.join(cols_to_drop)}")
        return df.drop(*cols_to_drop)
    
    return df
