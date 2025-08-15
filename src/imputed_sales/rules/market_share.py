"""
BR-04: Market Share-Based Estimation

Formula: Imputed Sales = Market Size × Estimated Market Share

Logic:
- Determine overall market size from IQVIA data
- Calculate product-specific market share
- Used when market-level data is reliable
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lit, round as spark_round

def calculate_market_share_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate market share-based imputed sales.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with market share-based imputed sales
    """
    # Load market research data
    market_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "market_research_data.csv")
    )
    
    # Load secondary sales data
    sales_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "secondary_sales.csv")
    )
    
    # Calculate total market size by product and geography
    market_size_df = market_df.groupBy("product_id", "geography_id", "year_month").agg(
        spark_sum(col("market_size_units").cast("double")).alias("total_market_size_units"),
        spark_sum(col("market_size_dollars").cast("double")).alias("total_market_size_dollars")
    )
    
    # Calculate product sales by geography
    # For simplicity, we'll assume location_id maps to geography_id
    # In a real implementation, you might need a mapping table
    product_sales_df = sales_df.withColumnRenamed("location_id", "geography_id")
    
    # Group sales by product, geography, and year_month
    grouped_sales_df = product_sales_df.groupBy("product_id", "geography_id", "year_month").agg(
        spark_sum(col("secondary_sales_qty").cast("double")).alias("total_product_sales_qty"),
        spark_sum(col("secondary_sales_dollars").cast("double")).alias("total_product_sales_dollars")
    )
    
    # Join market size with product sales
    joined_df = market_size_df.join(
        grouped_sales_df,
        on=["product_id", "geography_id", "year_month"],
        how="left"
    )
    
    # Fill null sales with zeros
    joined_df = joined_df.fillna({
        "total_product_sales_qty": 0,
        "total_product_sales_dollars": 0
    })
    
    # Calculate market share
    result_df = joined_df.withColumn(
        "market_share_pct",
        when(col("total_market_size_units") > 0,
             (col("total_product_sales_qty") / col("total_market_size_units")) * 100
            ).otherwise(lit(0))
    )
    
    # Calculate imputed sales based on market share
    result_df = result_df.withColumn(
        "imputed_sales_qty",
        spark_round(col("total_market_size_units") * (col("market_share_pct") / 100), 2)
    ).withColumn(
        "imputed_sales_dollars",
        spark_round(col("total_market_size_dollars") * (col("market_share_pct") / 100), 2)
    )
    
    # Join back with original sales data to get distributor information
    # For simplicity, we'll duplicate the imputed sales for each distributor in the geography
    # In a real implementation, you might want to distribute based on historical distributor share
    expanded_df = result_df.join(
        sales_df.withColumnRenamed("location_id", "geography_id").select(
            "distributor_id", "product_id", "geography_id", "year_month"
        ).distinct(),
        on=["product_id", "geography_id", "year_month"],
        how="inner"
    )
    
    # Select final columns
    final_df = expanded_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("geography_id").alias("location_id"),
        col("total_product_sales_qty").alias("raw_sales_qty"),
        col("total_product_sales_dollars").alias("raw_sales_dollars"),
        col("market_share_pct"),
        col("total_market_size_units"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-04").alias("rule_id")
    )
    
    return final_df
