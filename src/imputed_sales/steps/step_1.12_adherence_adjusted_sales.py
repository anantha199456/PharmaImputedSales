"""
Step 1.12: Adherence-Based Adjustment

Formula: Adjusted Sales = Raw Sales × Adherence Rate

This script implements BR-12: Adherence-Based Adjustment to account
for patient adherence patterns in sales estimates.
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
        .appName("Step 1.12: Adherence-Based Adjustment") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/adherence_adjusted_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.12: Adherence-Based Adjustment for period {year_month}")
    
    try:
        # Load secondary sales data with schema
        sales_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("secondary_sales.csv")) \
            .csv(f"{source_data_path}/secondary_sales.csv")
        
        # Load product master with schema
        product_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("product_master.csv")) \
            .csv(f"{source_data_path}/product_master.csv")
        
        # Load adherence study data with schema
        adherence_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("adherence_study.csv")) \
            .csv(f"{source_data_path}/adherence_study.csv")
        
        # Join sales data with product master
        joined_df = sales_df.join(
            product_df.select("product_id", "condition_type"),
            on=["product_id"],
            how="left"
        )
        
        # Join with adherence study data
        result_df = joined_df.join(
            adherence_df.select("product_id", "observed_adherence_rate"),
            on=["product_id"],
            how="left"
        )
        
        # Apply default adherence rates if missing
        result_df = result_df.withColumn(
            "adherence_rate",
            when(col("observed_adherence_rate").isNotNull(), 
                 col("observed_adherence_rate")
                ).when(col("condition_type") == "CHRONIC", 
                       lit(0.7)
                      ).when(col("condition_type") == "ACUTE", 
                             lit(0.9)
                            ).otherwise(lit(0.8))
        )
        
        # Apply adherence rate to raw sales
        result_df = result_df.withColumn(
            "imputed_sales_qty",
            spark_round(
                col("secondary_sales_qty") * 
                col("adherence_rate"),
                2
            )
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(
                col("secondary_sales_dollars") * 
                col("adherence_rate"),
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
            col("condition_type"),
            col("adherence_rate"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-12").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.12 completed: Adherence-adjusted sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.12: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
