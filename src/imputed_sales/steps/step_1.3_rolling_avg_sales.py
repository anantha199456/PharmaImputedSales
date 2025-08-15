"""
Step 1.3: Rolling Average Estimation

Formula: Imputed Sales = Average of last N months' secondary sales

This script implements BR-03: Rolling Average Estimation to calculate
imputed sales based on a 3-month rolling average of historical sales data.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, lit, round as spark_round
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_utils import get_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Step 1.3: Rolling Average Estimation") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/rolling_avg_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.3: Rolling Average Estimation for period {year_month}")
    
    try:
        # Load secondary sales data with schema
        # Note: For this rule, we assume historical data is in the same file
        # In a real implementation, you might need to load historical data separately
        sales_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("secondary_sales.csv")) \
            .csv(f"{source_data_path}/secondary_sales.csv")
        
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
                col("secondary_sales_qty") + 
                col("prev_month_sales_qty").fillna(0) + 
                col("prev_2_month_sales_qty").fillna(0)
            ) / 3
        ).withColumn(
            "rolling_avg_dollars",
            (
                col("secondary_sales_dollars") + 
                col("prev_month_sales_dollars").fillna(0) + 
                col("prev_2_month_sales_dollars").fillna(0)
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
            col("secondary_sales_qty").alias("raw_sales_qty"),
            col("secondary_sales_dollars").alias("raw_sales_dollars"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-03").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.3 completed: Rolling average sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.3: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
