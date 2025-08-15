"""
BR-01: Inventory-Based Imputation

Formula: Imputed Sales = Opening Inventory + Shipments - Closing Inventory

Logic:
- Calculate adjustment factor based on inventory movement
- Apply to raw sales data to get adjusted figures
- Prioritized for products with significant inventory fluctuations
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round

def calculate_inventory_based_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate inventory-based imputed sales.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with inventory-adjusted sales
    """
    # Load inventory data
    inventory_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "distributor_inventory.csv")
    )
    
    # Load secondary sales data
    sales_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "secondary_sales.csv")
    )
    
    # Calculate inventory movement
    inventory_movement_df = inventory_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("location_id"),
        (col("opening_inventory").cast("double") + 
         col("shipments_received").cast("double") - 
         col("closing_inventory").cast("double")).alias("inventory_movement")
    )
    
    # Join with sales data
    joined_df = sales_df.join(
        inventory_movement_df,
        on=["distributor_id", "product_id", "year_month", "location_id"],
        how="inner"
    )
    
    # Calculate adjustment factor
    # If raw sales is zero, use default factor of 1
    result_df = joined_df.withColumn(
        "adjustment_factor",
        when(col("secondary_sales_qty").cast("double") > 0,
             col("inventory_movement") / col("secondary_sales_qty").cast("double")
            ).otherwise(lit(1.0))
    )
    
    # Apply adjustment factor to raw sales
    result_df = result_df.withColumn(
        "imputed_sales_qty", 
        spark_round(col("secondary_sales_qty").cast("double") * col("adjustment_factor"), 2)
    )
    
    # Calculate imputed sales dollars
    result_df = result_df.withColumn(
        "imputed_sales_dollars",
        spark_round(
            when(col("secondary_sales_qty").cast("double") > 0,
                 col("secondary_sales_dollars").cast("double") * 
                 (col("imputed_sales_qty") / col("secondary_sales_qty").cast("double"))
                ).otherwise(col("secondary_sales_dollars").cast("double")),
            2
        )
    )
    
    # Select final columns
    final_df = result_df.select(
        col("distributor_id"),
        col("product_id"),
        col("year_month"),
        col("location_id"),
        col("secondary_sales_qty").cast("double").alias("raw_sales_qty"),
        col("secondary_sales_dollars").cast("double").alias("raw_sales_dollars"),
        col("inventory_movement"),
        col("adjustment_factor"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-01").alias("rule_id")
    )
    
    return final_df
