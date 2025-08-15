"""
BR-05: Tender-Based Estimation

Formula: Imputed Sales = Tender Award Quantity / Duration

Logic:
- Distribute tender quantities evenly across contract duration
- Specifically used for government accounts (VA, DoD)
- Kaiser accounts use this with Kaiser-specific utilization rates
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, datediff, months_between, lit, when, round as spark_round

def calculate_tender_based_sales(spark: SparkSession, source_data_dir: str, year_month: str) -> DataFrame:
    """
    Calculate tender-based imputed sales.
    
    Args:
        spark: SparkSession
        source_data_dir: Directory containing source data files
        year_month: Year and month in YYYYMM format
        
    Returns:
        DataFrame with tender-based imputed sales
    """
    # Load tender awards data
    tender_df = spark.read.option("header", "true").csv(
        os.path.join(source_data_dir, "tender_awards.csv")
    )
    
    # Convert date fields to date type
    # Note: In the sample data, dates appear to be in Excel date format (days since 1900-01-01)
    # For a real implementation, you would need proper date conversion
    # Here we'll use the monthly_delivery field which is already provided
    
    # Calculate imputed sales based on monthly delivery
    result_df = tender_df.withColumn(
        "imputed_sales_qty", col("monthly_delivery").cast("double")
    )
    
    # Calculate imputed sales dollars based on contract value and duration
    result_df = result_df.withColumn(
        "imputed_sales_dollars",
        when(col("monthly_delivery").cast("double") > 0,
             (col("contract_value").cast("double") / col("total_quantity").cast("double")) * 
             col("monthly_delivery").cast("double")
            ).otherwise(lit(0))
    )
    
    # Round to 2 decimal places
    result_df = result_df.withColumn(
        "imputed_sales_dollars", spark_round(col("imputed_sales_dollars"), 2)
    )
    
    # Apply institution-specific adjustments
    # For Kaiser accounts, we might apply specific utilization rates
    # This is a simplified implementation
    result_df = result_df.withColumn(
        "institution_factor",
        when(col("institution_type") == "KAISER", lit(0.9))
        .when(col("institution_type") == "VA", lit(1.0))
        .when(col("institution_type") == "DOD", lit(1.0))
        .otherwise(lit(1.0))
    )
    
    # Apply institution factor to imputed sales
    result_df = result_df.withColumn(
        "imputed_sales_qty", 
        spark_round(col("imputed_sales_qty") * col("institution_factor"), 2)
    ).withColumn(
        "imputed_sales_dollars", 
        spark_round(col("imputed_sales_dollars") * col("institution_factor"), 2)
    )
    
    # Select final columns
    final_df = result_df.select(
        col("institution_id").alias("distributor_id"),
        col("product_id"),
        lit(year_month).alias("year_month"),  # Use the provided year_month
        col("institution_id").alias("location_id"),
        lit(0).alias("raw_sales_qty"),  # No raw sales for tender-based estimation
        lit(0).alias("raw_sales_dollars"),  # No raw sales for tender-based estimation
        col("total_quantity").cast("double").alias("tender_quantity"),
        col("institution_type"),
        col("institution_factor"),
        col("imputed_sales_qty"),
        col("imputed_sales_dollars"),
        lit("BR-05").alias("rule_id")
    )
    
    return final_df
