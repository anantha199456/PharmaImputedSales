"""
Step 1.10: Formulary Tier Influence

Formula: Adjusted Sales = Base Sales × Tier Adjustment Factor

This script implements BR-10: Formulary Tier Influence to adjust
sales estimates based on formulary access and tiering information.
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
        .appName("Step 1.10: Formulary Tier Influence") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/tier_adjusted_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    year_quarter = "2025Q1"  # Format: YYYYQN
    
    print(f"Starting Step 1.10: Formulary Tier Influence for period {year_month}")
    
    try:
        # Load secondary sales data with schema
        sales_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("secondary_sales.csv")) \
            .csv(f"{source_data_path}/secondary_sales.csv")
        
        # Load formulary data with schema
        formulary_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("formulary_data.csv")) \
            .csv(f"{source_data_path}/formulary_data.csv")
        
        # Filter formulary data for current quarter
        formulary_df = formulary_df.filter(col("year_quarter") == year_quarter)
        
        # Join sales data with formulary information
        # For simplicity, we'll assume we can join on product_id
        # In a real implementation, you might need a more complex join logic
        joined_df = sales_df.join(
            formulary_df.select("product_id", "formulary_tier", "tier_adjustment_factor"),
            on=["product_id"],
            how="left"
        )
        
        # Fill missing tier adjustment factors with default value of 1.0
        joined_df = joined_df.fillna({
            "tier_adjustment_factor": 1.0
        })
        
        # Apply tier-specific adjustment factors to base sales
        result_df = joined_df.withColumn(
            "imputed_sales_qty",
            spark_round(
                col("secondary_sales_qty") * 
                col("tier_adjustment_factor"),
                2
            )
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(
                col("secondary_sales_dollars") * 
                col("tier_adjustment_factor"),
                2
            )
        )
        
        # Select final columns
        final_df = result_df.select(
            col("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("location_id"),
            col("secondary_sales_qty").alias("raw_sales_qty"),
            col("secondary_sales_dollars").alias("raw_sales_dollars"),
            col("formulary_tier"),
            col("tier_adjustment_factor").alias("tier_adjustment_factor"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-10").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.10 completed: Tier-adjusted sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.10: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
