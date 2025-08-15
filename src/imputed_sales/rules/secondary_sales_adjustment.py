"""
BR-02: Secondary Sales Adjustment

Formula: Imputed Sales = Secondary Sales + Adjustments (returns, corrections)

Logic:
- Incorporate return data from distributor logs
- Add stock corrections and other adjustments
- Ensures sales figures reflect actual market consumption
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lit, round as spark_round

def calculate_adjusted_secondary_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate secondary sales adjusted for returns and corrections.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with adjusted secondary sales
    """
    # Load secondary sales data
    sales_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "secondary_sales.csv")
    )
    
    # Load adjustment logs
    adjustments_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "adjustment_logs.csv")
    )
    
    # Group adjustments by distributor, product, and year_month
    grouped_adjustments_df = adjustments_df.groupBy(
        "distributor_id", "product_id", "year_month"
    ).agg(
        spark_sum(col("adjustment_qty").cast("double")).alias("total_adjustment_qty"),
        spark_sum(col("adjustment_dollars").cast("double")).alias("total_adjustment_dollars")
    )
    
    # Join sales data with adjustments
    joined_df = sales_df.join(
        grouped_adjustments_df,
        on=["distributor_id", "product_id", "year_month"],
        how="left"
    )
    
    # Fill null adjustments with zeros
    joined_df = joined_df.fillna({
        "total_adjustment_qty": 0,
        "total_adjustment_dollars": 0
    })
    
    # Calculate adjusted sales
    result_df = joined_df.withColumn(
        "imputed_sales_qty",
        spark_round(col("secondary_sales_qty").cast("double") + col("total_adjustment_qty"), 2)
    ).withColumn(
        "imputed_sales_dollars",
        spark_round(col("secondary_sales_dollars").cast("double") + col("total_adjustment_dollars"), 2)
    )
    
    # Select final columns
    final_df = result_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("location_id"),
        col("secondary_sales_qty").cast("double").alias("raw_sales_qty"),
        col("secondary_sales_dollars").cast("double").alias("raw_sales_dollars"),
        col("total_adjustment_qty"),
        col("total_adjustment_dollars"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-02").alias("rule_id")
    )
    
    return final_df
