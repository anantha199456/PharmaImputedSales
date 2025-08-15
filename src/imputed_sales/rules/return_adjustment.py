"""
BR-06: Return Adjustment

Formula: Imputed Sales = Net Sales – Returns or Adjustments

Logic:
- Incorporate return data from distributor logs
- Ensure sales figures represent actual consumption
- Policies on return treatment may vary by company
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lit, when, round as spark_round

def calculate_return_adjusted_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate return-adjusted sales.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with return-adjusted sales
    """
    # Load secondary sales data
    sales_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "secondary_sales.csv")
    )
    
    # Load adjustment logs
    adjustments_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "adjustment_logs.csv")
    )
    
    # Filter for return adjustments only
    returns_df = adjustments_df.filter(col("adjustment_type") == "RETURN")
    
    # Group returns by distributor, product, and year_month
    grouped_returns_df = returns_df.groupBy(
        "distributor_id", "product_id", "year_month"
    ).agg(
        spark_sum(col("adjustment_qty").cast("double")).alias("total_returns_qty"),
        spark_sum(col("adjustment_dollars").cast("double")).alias("total_returns_dollars")
    )
    
    # Join sales data with returns
    joined_df = sales_df.join(
        grouped_returns_df,
        on=["distributor_id", "product_id", "year_month"],
        how="left"
    )
    
    # Fill null returns with zeros
    joined_df = joined_df.fillna({
        "total_returns_qty": 0,
        "total_returns_dollars": 0
    })
    
    # Calculate net sales after returns
    # Note: Returns are stored as negative values, so we add them
    result_df = joined_df.withColumn(
        "imputed_sales_qty",
        spark_round(col("secondary_sales_qty").cast("double") + col("total_returns_qty"), 2)
    ).withColumn(
        "imputed_sales_dollars",
        spark_round(col("secondary_sales_dollars").cast("double") + col("total_returns_dollars"), 2)
    )
    
    # Handle cases where returns exceed sales
    result_df = result_df.withColumn(
        "imputed_sales_qty",
        when(col("imputed_sales_qty") < 0, lit(0)).otherwise(col("imputed_sales_qty"))
    ).withColumn(
        "imputed_sales_dollars",
        when(col("imputed_sales_dollars") < 0, lit(0)).otherwise(col("imputed_sales_dollars"))
    )
    
    # Select final columns
    final_df = result_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("location_id"),
        col("secondary_sales_qty").cast("double").alias("raw_sales_qty"),
        col("secondary_sales_dollars").cast("double").alias("raw_sales_dollars"),
        col("total_returns_qty"),
        col("total_returns_dollars"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-06").alias("rule_id")
    )
    
    return final_df
