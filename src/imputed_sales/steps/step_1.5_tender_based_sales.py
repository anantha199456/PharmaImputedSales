"""
Step 1.5: Tender-Based Estimation

Formula: Imputed Sales = Tender Award Quantity / Duration

This script implements BR-05: Tender-Based Estimation to calculate
imputed sales by distributing tender quantities evenly across contract duration.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round as spark_round
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_utils import get_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Step 1.5: Tender-Based Estimation") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/tender_based_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.5: Tender-Based Estimation for period {year_month}")
    
    try:
        # Load tender awards data with schema
        tender_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("tender_awards.csv")) \
            .csv(f"{source_data_path}/tender_awards.csv")
        
        # Calculate imputed sales based on monthly delivery
        result_df = tender_df.withColumn(
            "imputed_sales_qty", col("monthly_delivery")
        )
        
        # Calculate imputed sales dollars based on contract value and duration
        result_df = result_df.withColumn(
            "imputed_sales_dollars",
            when(col("monthly_delivery") > 0,
                 (col("contract_value") / col("total_quantity")) * 
                 col("monthly_delivery")
                ).otherwise(lit(0))
        )
        
        # Round to 2 decimal places
        result_df = result_df.withColumn(
            "imputed_sales_dollars", spark_round(col("imputed_sales_dollars"), 2)
        )
        
        # Apply institution-specific adjustments
        # For Kaiser accounts, we might apply specific utilization rates
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
            col("total_quantity").alias("tender_quantity"),
            col("institution_type"),
            col("institution_factor"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-05").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.5 completed: Tender-based sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.5: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
