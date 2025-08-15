"""
Step 1.11: Dosage Unit Conversion

Formula: Standardized Units = Raw Units × Conversion Factor

This script implements BR-11: Dosage Unit Conversion to standardize
units across different dosage forms for consistent measurement.
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
        .appName("Step 1.11: Dosage Unit Conversion") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/standardized_unit_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.11: Dosage Unit Conversion for period {year_month}")
    
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
        
        # Join sales data with product master
        joined_df = sales_df.join(
            product_df.select("product_id", "conversion_factor"),
            on=["product_id"],
            how="left"
        )
        
        # Fill missing conversion factors with default value of 1.0
        joined_df = joined_df.fillna({
            "conversion_factor": 1.0
        })
        
        # Apply conversion factor to raw units
        result_df = joined_df.withColumn(
            "standardized_units",
            spark_round(
                col("secondary_sales_qty") * 
                col("conversion_factor"),
                2
            )
        )
        
        # Calculate standardized sales dollars
        result_df = result_df.withColumn(
            "standardized_dollars",
            spark_round(
                when(col("secondary_sales_qty") > 0,
                     col("secondary_sales_dollars") * 
                     (col("standardized_units") / col("secondary_sales_qty"))
                    ).otherwise(col("secondary_sales_dollars")),
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
            col("conversion_factor").alias("conversion_factor"),
            col("standardized_units").alias("imputed_sales_qty"),
            col("standardized_dollars").alias("imputed_sales_dollars"),
            lit("BR-11").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.11 completed: Standardized unit sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.11: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
