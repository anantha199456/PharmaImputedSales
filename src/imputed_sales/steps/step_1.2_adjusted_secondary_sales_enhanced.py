"""
Step 1.2: Secondary Sales Adjustment

Formula: Imputed Sales = Secondary Sales + Adjustments (returns, corrections)

This script implements BR-02: Secondary Sales Adjustment to calculate
imputed sales by incorporating return data and other adjustments.

This step can be run both locally and as an AWS Glue job.
"""

import sys
import os
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sum as _sum, round as _round

# Import base step class
from .base_step import GlueCompatibleStep

class AdjustedSecondarySalesStep(GlueCompatibleStep):
    """Implements BR-02: Secondary Sales Adjustment."""
    
    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        """Initialize the adjusted secondary sales step.
        
        Args:
            config: Configuration dictionary
            **kwargs: Additional keyword arguments for the base class
        """
        super().__init__(config=config, **kwargs)
        self.step_name = "1.2_adjusted_secondary_sales"
        self.output_name = "adjusted_secondary_sales"
        self.year_month = self.config.get('processing_date', '202501')  # Default to Jan 2025
    
    def extract(self) -> Dict[str, DataFrame]:
        """Extract required data sources.
        
        Returns:
            Dictionary containing the extracted DataFrames
        """
        self.logger.info(f"Extracting data for {self.step_name}")
        
        # Load secondary sales data
        sales_df = self.read_csv("secondary_sales.csv")
        
        # Load adjustment logs
        adjustments_df = self.read_csv("adjustment_logs.csv")
        
        return {
            'sales': sales_df,
            'adjustments': adjustments_df
        }
    
    def transform(self, data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Transform the extracted data.
        
        Args:
            data: Dictionary containing the extracted DataFrames
                
        Returns:
            Dictionary containing the transformed DataFrames
        """
        self.logger.info(f"Transforming data for {self.step_name}")
        
        sales_df = data['sales']
        adjustments_df = data['adjustments']
        
        # Group adjustments by distributor, product, and year_month
        grouped_adjustments_df = adjustments_df.groupBy(
            "distributor_id", "product_id", "year_month"
        ).agg(
            _sum("adjustment_qty").alias("total_adjustment_qty"),
            _sum("adjustment_dollars").alias("total_adjustment_dollars")
        )
        
        # Join sales data with adjustments
        joined_df = sales_df.join(
            grouped_adjustments_df,
            on=["distributor_id", "product_id", "year_month"],
            how="left"
        )
        
        # Fill null adjustments with zeros
        joined_df = joined_df.fillna({
            "total_adjustment_qty": 0,
            "total_adjustment_dollars": 0
        })
        
        # Calculate adjusted sales
        result_df = joined_df.withColumn(
            "imputed_sales_qty",
            _round(col("secondary_sales_qty") + col("total_adjustment_qty"), 2)
        ).withColumn(
            "imputed_sales_dollars",
            _round(col("secondary_sales_dollars") + col("total_adjustment_dollars"), 2)
        )
        
        # Select final columns
        final_df = result_df.select(
            col("distributor_id"),
            col("product_id"),
            col("year_month"),
            col("location_id"),
            col("secondary_sales_qty").alias("raw_sales_qty"),
            col("secondary_sales_dollars").alias("raw_sales_dollars"),
            col("total_adjustment_qty"),
            col("total_adjustment_dollars"),
            col("imputed_sales_qty"),
            col("imputed_sales_dollars"),
            lit("BR-02").alias("rule_id")
        )
        
        return {
            'result': final_df,
            'metrics': self._calculate_metrics(final_df)
        }
    
    def _calculate_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate metrics from the result DataFrame.
        
        Args:
            df: Result DataFrame
                
        Returns:
            DataFrame containing metrics
        """
        return df.groupBy("product_id", "year_month").agg(
            _sum("imputed_sales_qty").alias("total_imputed_sales_qty"),
            _sum("imputed_sales_dollars").alias("total_imputed_sales_dollars"),
            _sum("total_adjustment_qty").alias("total_adjustment_qty"),
            _sum("total_adjustment_dollars").alias("total_adjustment_dollars")
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
    step = AdjustedSecondarySalesStep()
    success = step.run()
    
    if not success:
        raise RuntimeError(f"{step.step_name} failed")


if __name__ == "__main__":
    main()
