"""
Step 1.1: Inventory-Based Imputation

Formula: Imputed Sales = Opening Inventory + Shipments - Closing Inventory

This script implements BR-01: Inventory-Based Imputation to calculate
imputed sales based on inventory movement.

This step can be run both locally and as an AWS Glue job.
"""

import sys
import os
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round, sum as _sum

# Import base step class
from .base_step import GlueCompatibleStep

class InventoryAdjustedSalesStep(GlueCompatibleStep):
    """Implements BR-01: Inventory-Based Imputation."""
    
    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        """Initialize the inventory adjusted sales step.
        
        Args:
            config: Configuration dictionary
            **kwargs: Additional keyword arguments for the base class
        """
        super().__init__(config=config, **kwargs)
        self.step_name = "1.1_inventory_adjusted_sales"
        self.output_name = "inventory_adjusted_sales"
        self.year_month = self.config.get('processing_date', '202501')  # Default to Jan 2025
    
    def extract(self) -> Dict[str, DataFrame]:
        """
            Extract required data sources
            Returns:
                Dictionary containing the extracted DataFrames
        """
        self.logger.info(f"Extracting data for {self.step_name}")
        
        # Load inventory data
        inventory_df = self.read_csv("distributor_inventory.csv")
        
        # Load secondary sales data
        sales_df = self.read_csv("secondary_sales.csv")
        
        return {
            'inventory': inventory_df,
            'sales': sales_df
        }
    
    def transform(self, data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Transform the extracted data.
        
        Args:
            data: Dictionary containing the extracted DataFrames
                
        Returns:
            Dictionary containing the transformed DataFrames
        """
        self.logger.info(f"Transforming data for {self.step_name}")
        
        inventory_df = data['inventory']
        sales_df = data['sales']
        
        # Calculate inventory movement
        inventory_movement_df = inventory_df.select(
            col("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("location_id"),
            (col("opening_inventory") + 
             col("shipments_received") - 
             col("closing_inventory")).alias("inventory_movement")
        )
        
        # Join with sales data
        joined_df = sales_df.join(
            inventory_movement_df,
            on=["distributor_id", "product_id", "year_month", "location_id"],
            how="inner"
        )
        
        # Calculate adjustment factor
        # If raw sales is zero, use default factor of 1
        result_df = joined_df.withColumn(
            "adjustment_factor",
            when(col("secondary_sales_qty") > 0,
                 col("inventory_movement") / col("secondary_sales_qty")
                ).otherwise(lit(1.0))
        )
        
        # Apply adjustment factor to raw sales
        result_df = result_df.withColumn(
            "imputed_sales_qty", 
            spark_round(col("secondary_sales_qty") * col("adjustment_factor"), 2)
        )
        
        # Calculate imputed sales dollars
        result_df = result_df.withColumn(
            "imputed_sales_dollars",
            spark_round(
                when(col("secondary_sales_qty") > 0,
                     col("secondary_sales_dollars") * 
                     (col("imputed_sales_qty") / col("secondary_sales_qty"))
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
            col("inventory_movement"),
            col("adjustment_factor"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-01").alias("rule_id")
        )
        
        # Join with secondary sales to get additional attributes
        result_df = final_df.join(
            sales_df.select("product_id", "product_name", "market", "region"),
            on="product_id",
            how="left"
        )
        
        # Add processing period
        result_df = result_df.withColumn("processing_period", lit(self.year_month))
        
        return {
            'result': result_df,
            'metrics': self._calculate_metrics(result_df)
        }
    
    def _calculate_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate metrics from the result DataFrame.
        
        Args:
            df: Result DataFrame
                
        Returns:
            DataFrame containing metrics
        """
        return df.groupBy("product_id", "market").agg(
            _sum("imputed_sales_qty").alias("total_adjusted_sales"),
            _sum("opening_inventory").alias("total_opening_inventory"),
            _sum("closing_inventory").alias("total_closing_inventory")
        )
    
    def load(self, data: Dict[str, DataFrame]) -> None:
        """Load the transformed data.
        
        Args:
            data: Dictionary containing the transformed DataFrames
        """
        self.logger.info(f"Loading data for {self.step_name}")
        
        # Write the main result
        result_df = data['result']
        self.write_output(result_df, self.output_name)
        
        # Write metrics
        metrics_df = data['metrics']
        self.write_output(metrics_df, f"{self.output_name}_metrics")
        
        self.logger.info(f"{self.step_name} completed successfully")


def main():
    """Main entry point for local execution."""
    step = InventoryAdjustedSalesStep()
    success = step.run()
    
    if not success:
        raise RuntimeError(f"{step.step_name} failed")


if __name__ == "__main__":
    main()
