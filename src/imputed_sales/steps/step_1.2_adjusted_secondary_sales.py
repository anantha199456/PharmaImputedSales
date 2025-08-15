"""
Step 1.2: Secondary Sales Adjustment

Formula: Imputed Sales = Secondary Sales + Adjustments (returns, corrections)

This script implements BR-02: Secondary Sales Adjustment to calculate
imputed sales by incorporating return data and other adjustments.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit, round as spark_round
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_utils import get_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Step 1.2: Secondary Sales Adjustment") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/adjusted_secondary_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.2: Secondary Sales Adjustment for period {year_month}")
    
    try:
        # Load secondary sales data with schema
        sales_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("secondary_sales.csv")) \
            .csv(f"{source_data_path}/secondary_sales.csv")
        
        # Load adjustment logs with schema
        adjustments_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("adjustment_logs.csv")) \
            .csv(f"{source_data_path}/adjustment_logs.csv")
        
        # Group adjustments by distributor, product, and year_month
        grouped_adjustments_df = adjustments_df.groupBy(
            "distributor_id", "product_id", "year_month"
        ).agg(
            spark_sum(col("adjustment_qty")).alias("total_adjustment_qty"),
            spark_sum(col("adjustment_dollars")).alias("total_adjustment_dollars")
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
            spark_round(col("secondary_sales_qty") + col("total_adjustment_qty"), 2)
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(col("secondary_sales_dollars") + col("total_adjustment_dollars"), 2)
        )
        
        # Select final columns
        final_df = result_df.select(
            col("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("location_id"),
            col("secondary_sales_qty").alias("raw_sales_qty"),
            col("secondary_sales_dollars").alias("raw_sales_dollars"),
            col("total_adjustment_qty"),
            col("total_adjustment_dollars"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-02").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.2 completed: Adjusted secondary sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.2: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
