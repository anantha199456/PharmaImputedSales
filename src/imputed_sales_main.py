"""
Imputed Sales Process - Main Orchestration Script

This script orchestrates the execution of various imputed sales calculation methods
based on the business rules provided.
"""

import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime

# Import all rule implementations
from rules.inventory_based import calculate_inventory_based_sales
from rules.secondary_sales_adjustment import calculate_adjusted_secondary_sales
from rules.rolling_average import calculate_rolling_average_sales
from rules.market_share import calculate_market_share_sales
from rules.tender_based import calculate_tender_based_sales
from rules.return_adjustment import calculate_return_adjusted_sales
from rules.lives_based import calculate_lives_based_sales
from rules.claims_based import calculate_claims_based_sales
from rules.utilization_based import calculate_utilization_based_sales
from rules.formulary_tier import calculate_tier_adjusted_sales
from rules.dosage_unit import calculate_standardized_unit_sales
from rules.adherence_based import calculate_adherence_adjusted_sales

def create_spark_session():
    """
    Create and configure a Spark session
    """
    return (SparkSession.builder
            .appName("Imputed Sales Process")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .getOrCreate())

def main():
    """
    Main function to orchestrate the imputed sales process
    """
    # Create Spark session
    spark = create_spark_session()
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    source_data_dir = os.path.join(base_dir, "source_data")
    output_dir = os.path.join(base_dir, "processed_data")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Current processing period (can be parameterized)
    year_month = "202501"  # Format: YYYYMM
    
    print(f"Starting imputed sales process for period: {year_month}")
    
    # Execute each rule and save output
    try:
        # BR-01: Inventory-Based Imputation
        inventory_adjusted_df = calculate_inventory_based_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        inventory_adjusted_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "inventory_adjusted_sales.csv"),
            header=True
        )
        print("BR-01: Inventory-Based Imputation - Completed")
        
        # BR-02: Secondary Sales Adjustment
        adjusted_secondary_df = calculate_adjusted_secondary_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        adjusted_secondary_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "adjusted_secondary_sales.csv"),
            header=True
        )
        print("BR-02: Secondary Sales Adjustment - Completed")
        
        # BR-03: Rolling Average Estimation
        rolling_avg_df = calculate_rolling_average_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        rolling_avg_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "rolling_avg_sales.csv"),
            header=True
        )
        print("BR-03: Rolling Average Estimation - Completed")
        
        # BR-04: Market Share-Based Estimation
        market_share_df = calculate_market_share_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        market_share_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "market_share_sales.csv"),
            header=True
        )
        print("BR-04: Market Share-Based Estimation - Completed")
        
        # BR-05: Tender-Based Estimation
        tender_based_df = calculate_tender_based_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        tender_based_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "tender_based_sales.csv"),
            header=True
        )
        print("BR-05: Tender-Based Estimation - Completed")
        
        # BR-06: Return Adjustment
        return_adjusted_df = calculate_return_adjusted_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        return_adjusted_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "return_adjusted_sales.csv"),
            header=True
        )
        print("BR-06: Return Adjustment - Completed")
        
        # BR-07: Lives-Based Imputation
        lives_based_df = calculate_lives_based_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        lives_based_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "lives_based_sales.csv"),
            header=True
        )
        print("BR-07: Lives-Based Imputation - Completed")
        
        # BR-08: Claims-Based Estimation
        claims_based_df = calculate_claims_based_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        claims_based_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "claims_based_sales.csv"),
            header=True
        )
        print("BR-08: Claims-Based Estimation - Completed")
        
        # BR-09: Utilization Rate-Based Estimation
        utilization_based_df = calculate_utilization_based_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        utilization_based_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "utilization_based_sales.csv"),
            header=True
        )
        print("BR-09: Utilization Rate-Based Estimation - Completed")
        
        # BR-10: Formulary Tier Influence
        tier_adjusted_df = calculate_tier_adjusted_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        tier_adjusted_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "tier_adjusted_sales.csv"),
            header=True
        )
        print("BR-10: Formulary Tier Influence - Completed")
        
        # BR-11: Dosage Unit Conversion
        standardized_unit_df = calculate_standardized_unit_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        standardized_unit_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "standardized_unit_sales.csv"),
            header=True
        )
        print("BR-11: Dosage Unit Conversion - Completed")
        
        # BR-12: Adherence-Based Adjustment
        adherence_adjusted_df = calculate_adherence_adjusted_sales(
            spark, 
            source_data_dir, 
            year_month
        )
        adherence_adjusted_df.write.mode("overwrite").csv(
            os.path.join(output_dir, "adherence_adjusted_sales.csv"),
            header=True
        )
        print("BR-12: Adherence-Based Adjustment - Completed")
        
        print("All imputed sales calculations completed successfully!")
        
    except Exception as e:
        print(f"Error in imputed sales process: {str(e)}")
        spark.stop()
        sys.exit(1)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
