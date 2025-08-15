# Enhanced Sales Consolidation

This module provides a robust implementation for consolidating sales data from multiple business rule processing steps into a single, comprehensive dataset with enhanced data quality checks, error handling, and reporting.

## Features

- **Modular Design**: Clean separation of concerns with dedicated methods for each processing stage
- **Data Quality**: Comprehensive validation of input data with detailed error reporting
- **Performance Optimized**: Configurable Spark settings for optimal performance
- **Detailed Logging**: Comprehensive logging for monitoring and debugging
- **Configuration Driven**: All parameters configurable via YAML files
- **Error Handling**: Graceful error handling with detailed error messages
- **Reporting**: Generates summary reports of the consolidation process

## Project Structure

```
ImputedSalesDemo/
├── config/
│   └── sales_config.yaml      # Main configuration file
├── code/
│   ├── consolidated_sales.py  # Main consolidation script
│   └── ...                    # Other project files
└── processed_data/            # Input data from processing steps
    ├── inventory_adjusted_sales.csv
    ├── adjusted_secondary_sales.csv
    └── ...
```

## Configuration

The `sales_config.yaml` file controls all aspects of the consolidation process:

```yaml
paths:
  processed_data: "processed_data"  # Input directory
  output:
    base_path: "final_output"       # Output directory
    format: "parquet"              # Output format (parquet/csv)
    mode: "overwrite"              # Write mode

# Data source configurations
sources:
  - name: "inventory_sales"
    filename: "inventory_adjusted_sales.csv"
    required: true
    key_columns: ["distributor_id", "product_id", "year_month"]
  # Additional sources...

data_quality:
  required_columns: ["product_id", "year_month"]
  allowed_null_ratio: 0.01

performance:
  shuffle_partitions: 8
  cache_enabled: true

logging:
  level: "INFO"
  file: "logs/sales_consolidation.log"
```

## Usage

### Prerequisites

- Python 3.7+
- PySpark
- PyYAML

### Running the Consolidation

1. **Basic Usage**:
   ```bash
   python code/consolidated_sales.py
   ```

2. **With Custom Config**:
   ```bash
   python code/consolidated_sales.py --config /path/to/custom_config.yaml
   ```

3. **Command Line Arguments**:
   - `--config`: Path to configuration file (default: `../config/sales_config.yaml`)

### Output

The consolidated output will be written to the directory specified in the configuration (`final_output` by default) in the specified format (Parquet by default). The output includes:

- All input columns from source files
- `final_sales`: The consolidated sales amount
- `source_system`: Identifier for the consolidation process
- `processing_timestamp`: When the record was processed

## Error Handling

The script includes comprehensive error handling:
- Missing required files are logged and may cause the process to fail
- Data quality issues are reported in the logs
- Detailed error messages are provided for troubleshooting

## Logging

Logs are written to both console and file (`sales_consolidation.log` by default). The log includes:
- Processing start/end times
- Data quality issues
- Performance metrics
- Any errors encountered

## Performance Considerations

- **Caching**: Intermediate DataFrames are cached when beneficial
- **Partitioning**: Output is partitioned by `year_month` for efficient querying
- **Broadcast Joins**: Small tables are automatically broadcast when joining
- **Shuffle Partitions**: Configurable to optimize performance

## Extending the Code

To add new data sources or modify business rules:

1. Update the configuration file with new sources
2. Modify the `_apply_business_rules` method to implement custom logic
3. Add new validation rules in `_validate_data_quality` if needed

## License

This project is licensed under the MIT License - see the LICENSE file for details.
