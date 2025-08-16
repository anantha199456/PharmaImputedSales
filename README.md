# Pharma Imputed Sales

A PySpark-based data processing pipeline for calculating imputed pharmaceutical sales using various business rules and methodologies.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Business Rules](#business-rules)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)

## Overview

This project implements a comprehensive imputed sales process using PySpark, based on 12 business rules for pharmaceutical sales data imputation. It's designed to handle large-scale pharmaceutical sales data processing efficiently using Apache Spark's distributed computing capabilities.

## Features

- **Multiple Sales Calculation Methods**: Implements 12 distinct business rules for sales imputation
- **Scalable Processing**: Built on Apache Spark for distributed data processing
- **Modular Architecture**: Easy to extend with additional rules or modify existing ones
- **Configurable Pipeline**: Flexible configuration for different processing scenarios
- **Comprehensive Output**: Generates detailed output files for each calculation method

## Project Structure

```
Pharma_ImputedSales/
├── config/                                  # Configuration files
│   ├── app/
│   │   ├── base.yaml
│   │   ├── development.yaml
│   │   └── production.yaml
│   ├── business_rules/
│   │   ├── inventory_rules.yaml
│   │   ├── sales_config.yaml
│   │   ├── sales_rules.yaml
│   │   └── validation_rules.yaml
│   └── spark/
│       ├── defaults.yaml
│       └── optimizations.yaml
├── processed_data/                            # Output files (generated)
│   ├── inventory_adjusted_sales.csv
│   ├── adjusted_secondary_sales.csv
│   └── ...
├── scripts/                                   # Utility and deployment scripts
│   ├── deploy_to_s3.py
│   └── run_enhanced_step.py
├── source_data/                               # Input data files
│   ├── adherence_study.csv
│   ├── adjustment_logs.csv
│   ├── claims_data.csv
│   ├── distributor_inventory.csv
│   ├── formulary_data.csv
│   ├── market_research_data.csv
│   ├── payer_lives_data.csv
│   ├── product_master.csv
│   ├── secondary_sales.csv
│   ├── tender_awards.csv
│   └── utilization_rates.csv
├── src/                                       # Source code
│   ├── README_consolidated_sales.md
│   ├── imputed_sales/
│   │   ├── __init__.py
│   │   ├── core/
│   │   │   ├── data_loader.py
│   │   │   ├── data_processor.py
│   │   │   ├── data_validator.py
│   │   │   └── spark_manager.py
│   │   ├── etl/
│   │   │   ├── dags/
│   │   │   │   └── imputed_sales_pipeline.py
│   │   │   └── scripts/
│   │   │       └── base_glue_job.py
│   │   ├── rules/
│   │   │   ├── inventory_based.py
│   │   │   ├── market_share.py
│   │   │   ├── return_adjustment.py
│   │   │   ├── rolling_average.py
│   │   │   ├── secondary_sales_adjustment.py
│   │   │   ├── tender_based.py
│   │   │   └── ...
│   │   ├── steps/
│   │   │   ├── base_step.py
│   │   │   ├── step_1.1_inventory_adjusted_sales.py
│   │   │   ├── step_1.2_adjusted_secondary_sales.py
│   │   │   ├── step_1.2_adjusted_secondary_sales_enhanced.py
│   │   │   ├── step_1.3_rolling_avg_sales.py
│   │   │   ├── step_1.4_market_share_sales.py
│   │   │   ├── step_1.5_tender_based_sales.py
│   │   │   ├── step_1.6_return_adjusted_sales.py
│   │   │   ├── step_1.7_lives_based_sales.py
│   │   │   ├── step_1.8_claims_based_sales.py
│   │   │   ├── step_1.9_utilization_based_sales.py
│   │   │   ├── step_1.10_tier_adjusted_sales.py
│   │   │   ├── step_1.11_standardized_unit_sales.py
│   │   │   ├── step_1.12_adherence_adjusted_sales.py
│   │   │   └── step_1.13_final_imputed_consolidated_sales.py
│   │   ├── utils/
│   │   │   ├── config_loader.py
│   │   │   ├── date_utils.py
│   │   │   ├── file_utils.py
│   │   │   ├── logger.py
│   │   │   ├── schema_utils.py
│   │   │   ├── spark_utils.py
│   │   │   └── validation_utils.py
│   ├── imputed_sales_main.py                  # Main orchestration script
│   └── run_all_steps.py                       # Script to run all processing steps
├── tests/                                     # Test files (currently empty)
├── .gitignore
├── README.md                                  # This file
└── requirements.txt                           # Dependencies
```

## Business Rules

### 1. Inventory-Based Imputation (BR-01)
- **Formula**: Imputed Sales = Opening Inventory + Shipments - Closing Inventory
- **Output**: `inventory_adjusted_sales.csv`
- **Use Case**: Calculates sales based on inventory movement data

### 2. Secondary Sales Adjustment (BR-02)
- **Formula**: Imputed Sales = Secondary Sales + Adjustments (returns, corrections)
- **Output**: `adjusted_secondary_sales.csv`
- **Use Case**: Adjusts secondary sales data for returns and corrections

### 3. Rolling Average Estimation (BR-03)
- **Formula**: Imputed Sales = Average of last N months' secondary sales
- **Output**: `rolling_avg_sales.csv`
- **Use Case**: Estimates sales using historical averages

### 4. Market Share-Based Estimation (BR-04)
- **Formula**: Imputed Sales = Market Size × Estimated Market Share
- **Output**: `market_share_sales.csv`
- **Use Case**: Estimates sales based on market share data

### 5. Tender-Based Estimation (BR-05)
- **Formula**: Imputed Sales = Tender Award Quantity / Duration
- **Output**: `tender_based_sales.csv`
- **Use Case**: Calculates sales from tender awards

### 6. Return Adjustment (BR-06)
- **Formula**: Imputed Sales = Net Sales – Returns or Adjustments
- **Output**: `return_adjusted_sales.csv`
- **Use Case**: Adjusts sales for product returns

### 7. Lives-Based Imputation (BR-07)
- **Formula**: Imputed Sales = Lives Covered × Utilization Rate × Product Unit Cost
- **Output**: `lives_based_sales.csv`
- **Use Case**: Estimates sales based on covered lives and utilization

### 8. Claims-Based Estimation (BR-08)
- **Formula**: Imputed Sales = (Approved Claims × Average Units per Claim) × Product Unit Cost
- **Output**: `claims_based_sales.csv`
- **Use Case**: Calculates sales from insurance claims data

### 9. Utilization Rate-Based Estimation (BR-09)
- **Formula**: Imputed Sales = Total Lives × Product-Specific Utilization % × Monthly Conversion Factor
- **Output**: `utilization_based_sales.csv`
- **Use Case**: Estimates sales based on population utilization rates

### 10. Formulary Tier Influence (BR-10)
- **Formula**: Adjusted Sales = Base Sales × Tier Adjustment Factor
- **Output**: `tier_adjusted_sales.csv`
- **Use Case**: Adjusts sales based on formulary tier status

### 11. Dosage Unit Conversion (BR-11)
- **Formula**: Standardized Units = Raw Units × Conversion Factor
- **Output**: `standardized_unit_sales.csv`
- **Use Case**: Standardizes sales units across different product forms

### 12. Adherence-Based Adjustment (BR-12)
- **Formula**: Adjusted Sales = Raw Sales × Adherence Rate
- **Output**: `adherence_adjusted_sales.csv`
- **Use Case**: Adjusts sales based on patient adherence rates

## Prerequisites

- Python 3.8+
- Java 8 or later (required for PySpark)
- Apache Spark 3.3.0
- Python packages (see `requirements.txt`):
  - pyspark==3.3.0
  - pandas==1.5.3
  - pyarrow==10.0.1
  - findspark==2.0.1

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd Pharma_ImputedSales
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Pipeline

1. Place your input data files in the `source_data/` directory.

2. Run the main processing pipeline:
   ```bash
   python -m src.imputed_sales_main
   ```

   Or use the run script:
   ```bash
   python src/run_all_steps.py
   ```

3. Processed output will be saved in the `processed_data/` directory.

### Running Individual Steps

To run a specific calculation method, you can execute the corresponding module in the `src/imputed_sales/` directory.

## Configuration

The application can be configured using the YAML files in the `config/` directory. Adjust these files to modify:
- Input/output file paths
- Processing parameters
- Calculation method parameters
- Spark configuration settings

For detailed configuration options, refer to the documentation in the `config/` directory.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

To run a specific imputation step:

```
cd ImputedSalesDemo
python code/steps/step_1.X_target_table_name.py
```

Replace `X` with the step number (1-12) and `target_table_name` with the corresponding output table name.

## Output

Each step generates a CSV file in the `processed_data` directory with the imputed sales data according to the respective business rule.
