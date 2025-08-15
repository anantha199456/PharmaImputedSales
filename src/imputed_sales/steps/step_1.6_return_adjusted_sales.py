"""
Step 1.6: Return Adjustment

Formula: Imputed Sales = Net Sales – Returns or Adjustments

This script implements BR-06: Return Adjustment to calculate
imputed sales by incorporating return data from distributor logs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit, when, round as spark_round
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_utils import get_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Step 1.6: Return Adjustment") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/return_adjusted_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.6: Return Adjustment for period {year_month}")
    
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
        
        # Filter for return adjustments only
        returns_df = adjustments_df.filter(col("adjustment_type") == "RETURN")
        
        # Group returns by distributor, product, and year_month
        grouped_returns_df = returns_df.groupBy(
            "distributor_id", "product_id", "year_month"
        ).agg(
            spark_sum(col("adjustment_qty")).alias("total_returns_qty"),
            spark_sum(col("adjustment_dollars")).alias("total_returns_dollars")
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
            spark_round(col("secondary_sales_qty") + col("total_returns_qty"), 2)
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(col("secondary_sales_dollars") + col("total_returns_dollars"), 2)
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
            col("secondary_sales_qty").alias("raw_sales_qty"),
            col("secondary_sales_dollars").alias("raw_sales_dollars"),
            col("total_returns_qty"),
            col("total_returns_dollars"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-06").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.6 completed: Return-adjusted sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.6: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
