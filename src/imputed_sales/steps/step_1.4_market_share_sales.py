"""
Step 1.4: Market Share-Based Estimation

Formula: Imputed Sales = Market Size × Estimated Market Share

This script implements BR-04: Market Share-Based Estimation to calculate
imputed sales based on market size and product-specific market share.
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
        .appName("Step 1.4: Market Share-Based Estimation") \
        .getOrCreate()
        
    # Define paths
    source_data_path = "source_data"
    output_path = "processed_data/market_share_sales.csv"
    
    # Current processing period
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting Step 1.4: Market Share-Based Estimation for period {year_month}")
    
    try:
        # Load market research data with schema
        market_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("market_research_data.csv")) \
            .csv(f"{source_data_path}/market_research_data.csv")
        
        # Load secondary sales data with schema
        sales_df = spark.read \
            .option("header", "true") \
            .schema(get_schema("secondary_sales.csv")) \
            .csv(f"{source_data_path}/secondary_sales.csv")
        
        # Calculate total market size by product and geography
        market_size_df = market_df.groupBy("product_id", "geography_id", "year_month").agg(
            spark_sum(col("market_size_units")).alias("total_market_size_units"),
            spark_sum(col("market_size_dollars")).alias("total_market_size_dollars")
        )
        
        # Calculate product sales by geography
        # For simplicity, we'll assume location_id maps to geography_id
        # In a real implementation, you might need a mapping table
        product_sales_df = sales_df.withColumnRenamed("location_id", "geography_id")
        
        # Group sales by product, geography, and year_month
        grouped_sales_df = product_sales_df.groupBy("product_id", "geography_id", "year_month").agg(
            spark_sum(col("secondary_sales_qty")).alias("total_product_sales_qty"),
            spark_sum(col("secondary_sales_dollars")).alias("total_product_sales_dollars")
        )
        
        # Join market size with product sales
        joined_df = market_size_df.join(
            grouped_sales_df,
            on=["product_id", "geography_id", "year_month"],
            how="left"
        )
        
        # Fill null sales with zeros
        joined_df = joined_df.fillna({
            "total_product_sales_qty": 0,
            "total_product_sales_dollars": 0
        })
        
        # Calculate market share
        result_df = joined_df.withColumn(
            "market_share_pct",
            when(col("total_market_size_units") > 0,
                 (col("total_product_sales_qty") / col("total_market_size_units")) * 100
                ).otherwise(lit(0))
        )
        
        # Calculate imputed sales based on market share
        result_df = result_df.withColumn(
            "imputed_sales_qty",
            spark_round(col("total_market_size_units") * (col("market_share_pct") / 100), 2)
        ).withColumn(
            "imputed_sales_dollars",
            spark_round(col("total_market_size_dollars") * (col("market_share_pct") / 100), 2)
        )
        
        # Join back with original sales data to get distributor information
        # For simplicity, we'll duplicate the imputed sales for each distributor in the geography
        # In a real implementation, you might want to distribute based on historical distributor share
        expanded_df = result_df.join(
            sales_df.withColumnRenamed("location_id", "geography_id").select(
                "distributor_id", "product_id", "geography_id", "year_month"
            ).distinct(),
            on=["product_id", "geography_id", "year_month"],
            how="inner"
        )
        
        # Select final columns
        final_df = expanded_df.select(
            col("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("geography_id").alias("location_id"),
            col("total_product_sales_qty").alias("raw_sales_qty"),
            col("total_product_sales_dollars").alias("raw_sales_dollars"),
            col("market_share_pct"),
            col("total_market_size_units"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-04").alias("rule_id")
        )
        
        # Write output
        final_df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"Step 1.4 completed: Market share-based sales saved to {output_path}")
        
    except Exception as e:
        print(f"Error in Step 1.4: {str(e)}")
        spark.stop()
        raise e
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
