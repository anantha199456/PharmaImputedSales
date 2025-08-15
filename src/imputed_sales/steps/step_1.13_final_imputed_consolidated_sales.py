"""
consolidated_sales.py

Enhanced implementation of the sales consolidation process that:
1. Loads processed data from all business rule steps
2. Performs data quality validations
3. Applies business rules for consolidation
4. Generates comprehensive reports
5. Handles errors gracefully with detailed logging
"""

import os
import sys
import yaml
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, current_timestamp,
    when, count, sum as spark_sum, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sales_consolidation.log')
    ]
)
logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Custom exception for data quality issues"""
    pass

class SalesConsolidator:
    """Main class for consolidating sales data with enhanced features"""

    def __init__(self, config_path: str = "../config/sales_config.yaml"):
        """Initialize with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._init_spark_session()
        self.metrics = {
            'start_time': datetime.now(),
            'records_processed': 0,
            'data_quality_issues': 0,
            'sources_loaded': 0,
            'errors': []
        }

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Set up logging
            log_config = config.get('logging', {})
            logger.setLevel(log_config.get('level', 'INFO'))

            return config

        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise

    def _init_spark_session(self) -> SparkSession:
        """Initialize and return a Spark session with optimized settings"""
        perf_config = self.config.get('performance', {})

        return (SparkSession.builder
                .appName("EnhancedSalesConsolidation")
                .config("spark.sql.shuffle.partitions",
                       perf_config.get('shuffle_partitions', 8))
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.autoBroadcastJoinThreshold",
                      perf_config.get('auto_broadcast_join_threshold', 10485760))
                .config("spark.broadcast.timeout",
                      perf_config.get('broadcast_timeout', 300))
                .getOrCreate())

    def load_processed_data(self) -> Dict[str, DataFrame]:
        """Load and validate all processed data sources"""
        data = {}
        processed_data_path = self.config['paths']['processed_data']

        for source in self.config['sources']:
            try:
                if not source.get('required', False):
                    continue

                file_path = os.path.join(processed_data_path, source['filename'])
                logger.info(f"Loading {source['name']} from {file_path}")

                # Load data with schema inference
                df = (self.spark.read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(file_path))

                # Run data quality checks
                self._validate_data_quality(df, source)

                # Cache if specified in config
                if self.config['performance'].get('cache_enabled', True):
                    df = df.cache()

                data[source['name']] = df
                self.metrics['sources_loaded'] += 1

            except Exception as e:
                error_msg = f"Error loading {source.get('name', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.metrics['errors'].append(error_msg)

                if source.get('required', False):
                    raise DataQualityError(error_msg)

        return data

    def _validate_data_quality(self, df: DataFrame, source: Dict) -> None:
        """Run data quality validations on the DataFrame"""
        # Check for required columns
        required_cols = set(source.get('key_columns', []))
        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            raise DataQualityError(
                f"Missing required columns in {source['name']}: {missing_cols}"
            )

        # Check for nulls in key columns
        for col_name in required_cols:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(
                    f"Found {null_count} null values in {col_name} "
                    f"for {source['name']}"
                )

    def consolidate_sales(self, data: Dict[str, DataFrame]) -> DataFrame:
        """Consolidate all sales data with business rules"""
        try:
            # Start with inventory sales as base
            consolidated = data['inventory_sales'].alias('inventory')

            # Join with other data sources
            for name, df in data.items():
                if name != 'inventory_sales':
                    join_cols = list(
                        set(consolidated.columns) &
                        set(df.columns) &
                        set(self.config['data_quality']['required_columns'])
                    )

                    if join_cols:
                        consolidated = consolidated.join(
                            df.alias(name),
                            on=join_cols,
                            how='left'
                        )

            # Apply business rules for final sales calculation
            final_df = self._apply_business_rules(consolidated)

            # Add metadata
            final_df = final_df.withColumn(
                'processing_timestamp', current_timestamp()
            ).withColumn(
                'source_system', lit('ENHANCED_SALES_CONSOLIDATION')
            )

            self.metrics['records_processed'] = final_df.count()
            return final_df

        except Exception as e:
            error_msg = f"Consolidation failed: {str(e)}"
            logger.error(error_msg)
            self.metrics['errors'].append(error_msg)
            raise

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to calculate final sales"""
        # Example: Priority order for sales sources
        return df.withColumn(
            'final_sales',
            coalesce(
                col('adjusted_sales'),
                col('inventory_sales'),
                col('market_share_sales'),
                col('tender_sales'),
                lit(0)  # Default value
            )
        )

    def generate_reports(self, final_df: DataFrame) -> None:
        """Generate data quality and reconciliation reports"""
        try:
            # Calculate metrics
            self.metrics['end_time'] = datetime.now()
            self.metrics['processing_time'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()

            # Log summary
            logger.info("\n" + "="*50)
            logger.info("CONSOLIDATION SUMMARY")
            logger.info("="*50)
            logger.info(f"Sources loaded: {self.metrics['sources_loaded']}")
            logger.info(f"Records processed: {self.metrics['records_processed']}")
            logger.info(f"Processing time: {self.metrics['processing_time']:.2f} seconds")

            if self.metrics['errors']:
                logger.warning(f"\nEncountered {len(self.metrics['errors'])} errors:")
                for error in self.metrics['errors']:
                    logger.warning(f"- {error}")

            logger.info("="*50)

        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")

    def write_output(self, df: DataFrame) -> None:
        """Write the final output with partitioning and optimization"""
        output_path = self.config['paths']['output']['base_path']
        output_format = self.config['paths']['output'].get('format', 'parquet')

        try:
            logger.info(f"Writing output to {output_path}")

            (df.write
             .mode(self.config['paths']['output'].get('mode', 'overwrite'))
             .format(output_format)
             .option("compression", "snappy")
             .partitionBy("year_month")
             .save(output_path))

            logger.info("Output write completed successfully")

        except Exception as e:
            error_msg = f"Failed to write output: {str(e)}"
            logger.error(error_msg)
            raise

    def run(self) -> None:
        """Main execution method"""
        try:
            # Load data
            data = self.load_processed_data()

            # Consolidate
            final_df = self.consolidate_sales(data)

            # Generate reports
            self.generate_reports(final_df)

            # Write output
            self.write_output(final_df)

        except Exception as e:
            logger.critical("Fatal error in sales consolidation", exc_info=True)
            raise
        finally:
            # Clean up resources
            self.spark.stop()

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Sales Data Consolidation')
    parser.add_argument('--config', type=str, default='../config/sales_config.yaml',
                      help='Path to configuration file')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    try:
        consolidator = SalesConsolidator(args.config)
        consolidator.run()
        sys.exit(0)
    except Exception as e:
        logger.critical("Sales consolidation failed", exc_info=True)
        sys.exit(1)
