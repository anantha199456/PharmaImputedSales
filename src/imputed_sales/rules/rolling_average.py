"""
BR-03: Rolling Average Estimation

Formula: Imputed Sales = Average of last N months' secondary sales

Logic:
- Calculate rolling average over 3-month window
- Apply seasonality adjustments if needed
- Used when current month data is incomplete or unreliable
"""

import os
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, avg, lag, lit, round as spark_round

def calculate_rolling_average_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate rolling average sales over a 3-month window.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with rolling average sales
    """
    # Load secondary sales data
    # Note: For this rule, we assume historical data is in the same file
    # In a real implementation, you might need to load historical data separately
    sales_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "secondary_sales.csv")
    )
    
    # Convert year_month to sortable format
    # For simplicity, we'll assume year_month is already in YYYYMM format
    # In a real implementation, you might need to convert other formats
    
    # Define window for rolling average calculation
    window_spec = Window.partitionBy("distributor_id", "product_id", "location_id").orderBy("year_month")
    
    # Calculate rolling average
    # In this simplified version, we're using lag functions to simulate a rolling window
    # In a real implementation with complete historical data, you would use a proper window function
    result_df = sales_df.withColumn(
        "prev_month_sales_qty", lag("secondary_sales_qty", 1).over(window_spec)
    ).withColumn(
        "prev_2_month_sales_qty", lag("secondary_sales_qty", 2).over(window_spec)
    ).withColumn(
        "prev_month_sales_dollars", lag("secondary_sales_dollars", 1).over(window_spec)
    ).withColumn(
        "prev_2_month_sales_dollars", lag("secondary_sales_dollars", 2).over(window_spec)
    )
    
    # Calculate average sales (current + previous 2 months)
    result_df = result_df.withColumn(
        "rolling_avg_qty",
        (
            col("secondary_sales_qty").cast("double") + 
            col("prev_month_sales_qty").cast("double").fillna(0) + 
            col("prev_2_month_sales_qty").cast("double").fillna(0)
        ) / 3
    ).withColumn(
        "rolling_avg_dollars",
        (
            col("secondary_sales_dollars").cast("double") + 
            col("prev_month_sales_dollars").cast("double").fillna(0) + 
            col("prev_2_month_sales_dollars").cast("double").fillna(0)
        ) / 3
    )
    
    # Round to 2 decimal places
    result_df = result_df.withColumn(
        "imputed_sales_qty", spark_round(col("rolling_avg_qty"), 2)
    ).withColumn(
        "imputed_sales_dollars", spark_round(col("rolling_avg_dollars"), 2)
    )
    
    # Select final columns
    final_df = result_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("location_id"),
        col("secondary_sales_qty").cast("double").alias("raw_sales_qty"),
        col("secondary_sales_dollars").cast("double").alias("raw_sales_dollars"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-03").alias("rule_id")
    )
    
    return final_df
