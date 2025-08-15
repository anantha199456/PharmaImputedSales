"""
Step 1.8: Claims-Based Estimation

Formula: Imputed Sales = (Approved Claims × Average Units per Claim) × Product Unit Cost

This script implements BR-08: Claims-Based Estimation to calculate
imputed sales using medical and pharmacy claims data.
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
        .appName("Step 1.8: Claims-Based Estimation") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/claims_based_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.8: Claims-Based Estimation for period {year_month}")
    
    try:
        # Load claims data with schema
        claims_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("claims_data.csv")) \
            .csv(f"{source_data_path}/claims_data.csv")
        
        # Load product master with schema
        product_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("product_master.csv")) \
            .csv(f"{source_data_path}/product_master.csv")
        
        # Join claims data with product master
        result_df = claims_df.join(
            product_df,
            on=["product_id"],
            how="inner"
        )
        
        # Calculate quantity of product units from claims
        result_df = result_df.withColumn(
            "imputed_sales_qty",
            spark_round(
                col("approved_claims") * 
                col("avg_units_per_claim"),
                2
            )
        )
        
        # Calculate imputed sales dollars
        result_df = result_df.withColumn(
            "imputed_sales_dollars",
            spark_round(col("imputed_sales_qty") * col("avg_cost_per_claim"), 2)
        )
        
        # Select final columns
        final_df = result_df.select(
            col("payer_id").alias("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("plan_id").alias("location_id"),
            lit(0).alias("raw_sales_qty"),  # No raw sales for claims-based estimation
            lit(0).alias("raw_sales_dollars"),  # No raw sales for claims-based estimation
            col("approved_claims").alias("approved_claims"),
            col("avg_units_per_claim").alias("avg_units_per_claim"),
            col("avg_cost_per_claim").alias("avg_cost_per_claim"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-08").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.8 completed: Claims-based sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.8: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
