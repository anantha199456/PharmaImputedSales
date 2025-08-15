"""
Step 1.7: Lives-Based Imputation

Formula: Imputed Sales = Lives Covered × Utilization Rate × Product Unit Cost

This script implements BR-07: Lives-Based Imputation to calculate
imputed sales using payer lives data and product-specific utilization rates.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round as spark_round
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_utils import get_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Step 1.7: Lives-Based Imputation") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/lives_based_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.7: Lives-Based Imputation for period {year_month}")
    
    try:
        # Load payer lives data with schema
        lives_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("payer_lives_data.csv")) \
            .csv(f"{source_data_path}/payer_lives_data.csv")
        
        # Load utilization rates with schema
        utilization_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("utilization_rates.csv")) \
            .csv(f"{source_data_path}/utilization_rates.csv")
        
        # Load product master with schema
        product_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("product_master.csv")) \
            .csv(f"{source_data_path}/product_master.csv")
        
        # Join payer lives with utilization rates
        # For simplicity, we'll join on book_of_business
        joined_df = lives_df.join(
            utilization_df,
            on=["book_of_business"],
            how="inner"
        )
        
        # Join with product master
        result_df = joined_df.join(
            product_df,
            on=["product_id"],
            how="inner"
        )
        
        # Calculate imputed sales quantity
        result_df = result_df.withColumn(
            "imputed_sales_qty",
            spark_round(
                col("covered_lives") * 
                (col("utilization_rate_pct") / 100) * 
                col("monthly_conversion_factor"),
                2
            )
        )
        
        # Calculate imputed sales dollars
        # For simplicity, we'll use a fixed unit cost of $100 per unit
        # In a real implementation, you would use actual product costs
        result_df = result_df.withColumn(
            "unit_cost",
            lit(100)
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(col("imputed_sales_qty") * col("unit_cost"), 2)
        )
        
        # Select final columns
        final_df = result_df.select(
            col("payer_id").alias("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("geography_id").alias("location_id"),
            lit(0).alias("raw_sales_qty"),  # No raw sales for lives-based imputation
            lit(0).alias("raw_sales_dollars"),  # No raw sales for lives-based imputation
            col("covered_lives"),
            col("utilization_rate_pct"),
            col("monthly_conversion_factor"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-07").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.7 completed: Lives-based sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.7: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
