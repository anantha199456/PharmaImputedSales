"""
Schema Utilities for Imputed Sales Process

This module defines the schemas for all input CSV files used in the imputed sales process.
These schemas ensure consistent data types and column names across all processing steps.
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# Schema for distributor_inventory.csv
distributor_inventory_schema = StructType([
    StructField("distributor_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("opening_inventory", DoubleType(), True),
    StructField("shipments_received", DoubleType(), True),
    StructField("closing_inventory", DoubleType(), True),
    StructField("location_id", StringType(), False)
])

# Schema for secondary_sales.csv
secondary_sales_schema = StructType([
    StructField("distributor_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("secondary_sales_qty", DoubleType(), True),
    StructField("secondary_sales_dollars", DoubleType(), True),
    StructField("location_id", StringType(), False),
    StructField("customer_type", StringType(), True)
])

# Schema for adjustment_logs.csv
adjustment_logs_schema = StructType([
    StructField("distributor_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("adjustment_type", StringType(), False),
    StructField("adjustment_qty", DoubleType(), True),
    StructField("adjustment_dollars", DoubleType(), True),
    StructField("reason_code", StringType(), True)
])

# Schema for product_master.csv
product_master_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("dosage_form", StringType(), True),
    StructField("strength", StringType(), True),
    StructField("pack_size", IntegerType(), True),
    StructField("units_per_pack", IntegerType(), True),
    StructField("conversion_factor", DoubleType(), True),
    StructField("condition_type", StringType(), True),
    StructField("adherence_rate", DoubleType(), True)
])

# Schema for market_research_data.csv
market_research_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("competitor_id", StringType(), True),
    StructField("year_month", StringType(), False),
    StructField("geography_id", StringType(), False),
    StructField("market_size_units", DoubleType(), True),
    StructField("market_size_dollars", DoubleType(), True),
    StructField("product_share_pct", DoubleType(), True),
    StructField("data_source", StringType(), True)
])

# Schema for tender_awards.csv
tender_awards_schema = StructType([
    StructField("tender_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("institution_id", StringType(), False),
    StructField("institution_type", StringType(), True),
    StructField("award_date", IntegerType(), True),  # Excel date format
    StructField("start_date", IntegerType(), True),  # Excel date format
    StructField("end_date", IntegerType(), True),    # Excel date format
    StructField("total_quantity", DoubleType(), True),
    StructField("contract_value", DoubleType(), True),
    StructField("monthly_delivery", DoubleType(), True)
])

# Schema for payer_lives_data.csv
payer_lives_schema = StructType([
    StructField("payer_id", StringType(), False),
    StructField("plan_id", StringType(), False),
    StructField("geography_id", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("covered_lives", IntegerType(), True),
    StructField("plan_type", StringType(), True),
    StructField("book_of_business", StringType(), True),
    StructField("age_band", StringType(), True),
    StructField("gender", StringType(), True)
])

# Schema for utilization_rates.csv
utilization_rates_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("indication", StringType(), True),
    StructField("age_band", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("book_of_business", StringType(), True),
    StructField("utilization_rate_pct", DoubleType(), True),
    StructField("monthly_conversion_factor", DoubleType(), True),
    StructField("data_source", StringType(), True)
])

# Schema for claims_data.csv
claims_data_schema = StructType([
    StructField("payer_id", StringType(), False),
    StructField("plan_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("approved_claims", IntegerType(), True),
    StructField("denied_claims", IntegerType(), True),
    StructField("avg_units_per_claim", DoubleType(), True),
    StructField("avg_cost_per_claim", DoubleType(), True),
    StructField("patient_count", IntegerType(), True)
])

# Schema for formulary_data.csv
formulary_data_schema = StructType([
    StructField("payer_id", StringType(), False),
    StructField("plan_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("year_quarter", StringType(), False),
    StructField("formulary_tier", StringType(), True),
    StructField("pa_required", BooleanType(), True),
    StructField("step_therapy", BooleanType(), True),
    StructField("quantity_limit", IntegerType(), True),
    StructField("tier_adjustment_factor", DoubleType(), True)
])

# Schema for adherence_study.csv
adherence_study_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("indication", StringType(), True),
    StructField("patient_segment", StringType(), True),
    StructField("study_date", IntegerType(), True),  # Excel date format
    StructField("study_duration_months", IntegerType(), True),
    StructField("observed_adherence_rate", DoubleType(), True),
    StructField("refill_rate", DoubleType(), True),
    StructField("persistence_rate", DoubleType(), True),
    StructField("data_source", StringType(), True)
])

# Dictionary mapping file names to their schemas
schema_dict = {
    "distributor_inventory.csv": distributor_inventory_schema,
    "secondary_sales.csv": secondary_sales_schema,
    "adjustment_logs.csv": adjustment_logs_schema,
    "product_master.csv": product_master_schema,
    "market_research_data.csv": market_research_schema,
    "tender_awards.csv": tender_awards_schema,
    "payer_lives_data.csv": payer_lives_schema,
    "utilization_rates.csv": utilization_rates_schema,
    "claims_data.csv": claims_data_schema,
    "formulary_data.csv": formulary_data_schema,
    "adherence_study.csv": adherence_study_schema
}

def get_schema(file_name):
    """
    Get the schema for a specific file.
    
    Args:
        file_name: Name of the CSV file
        
    Returns:
        StructType schema for the file
    """
    return schema_dict.get(file_name)
