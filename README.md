# Imputed Sales Process Implementation

This project implements a comprehensive imputed sales process using PySpark, based on 12 business rules for pharmaceutical sales data imputation.

## Project Structure

```
ImputedSalesDemo/
├── code/
│   ├── steps/                  # Individual step implementations
│   │   ├── step_1.1_inventory_adjusted_sales.py
│   │   ├── step_1.2_adjusted_secondary_sales.py
│   │   ├── ...
│   │   └── step_1.12_adherence_adjusted_sales.py
│   └── run_all_steps.py        # Main orchestration script
├── source_data/                # Input CSV files
│   ├── distributor_inventory.csv
│   ├── secondary_sales.csv
│   ├── ...
│   └── adherence_study.csv
├── processed_data/             # Output files (generated)
│   ├── inventory_adjusted_sales.csv
│   ├── adjusted_secondary_sales.csv
│   ├── ...
│   └── adherence_adjusted_sales.csv
└── requirements.txt            # Dependencies
```

## Business Rules Implemented

1. **BR-01: Inventory-Based Imputation**
   - Formula: Imputed Sales = Opening Inventory + Shipments - Closing Inventory
   - Output: `inventory_adjusted_sales.csv`

2. **BR-02: Secondary Sales Adjustment**
   - Formula: Imputed Sales = Secondary Sales + Adjustments (returns, corrections)
   - Output: `adjusted_secondary_sales.csv`

3. **BR-03: Rolling Average Estimation**
   - Formula: Imputed Sales = Average of last N months' secondary sales
   - Output: `rolling_avg_sales.csv`

4. **BR-04: Market Share-Based Estimation**
   - Formula: Imputed Sales = Market Size × Estimated Market Share
   - Output: `market_share_sales.csv`

5. **BR-05: Tender-Based Estimation**
   - Formula: Imputed Sales = Tender Award Quantity / Duration
   - Output: `tender_based_sales.csv`

6. **BR-06: Return Adjustment**
   - Formula: Imputed Sales = Net Sales – Returns or Adjustments
   - Output: `return_adjusted_sales.csv`

7. **BR-07: Lives-Based Imputation**
   - Formula: Imputed Sales = Lives Covered × Utilization Rate × Product Unit Cost
   - Output: `lives_based_sales.csv`

8. **BR-08: Claims-Based Estimation**
   - Formula: Imputed Sales = (Approved Claims × Average Units per Claim) × Product Unit Cost
   - Output: `claims_based_sales.csv`

9. **BR-09: Utilization Rate-Based Estimation**
   - Formula: Imputed Sales = Total Lives × Product-Specific Utilization % × Monthly Conversion Factor
   - Output: `utilization_based_sales.csv`

10. **BR-10: Formulary Tier Influence**
    - Formula: Adjusted Sales = Base Sales × Tier Adjustment Factor
    - Output: `tier_adjusted_sales.csv`

11. **BR-11: Dosage Unit Conversion**
    - Formula: Standardized Units = Raw Units × Conversion Factor
    - Output: `standardized_unit_sales.csv`

12. **BR-12: Adherence-Based Adjustment**
    - Formula: Adjusted Sales = Raw Sales × Adherence Rate
    - Output: `adherence_adjusted_sales.csv`

## Setup and Installation

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Ensure you have Java installed for PySpark.

## Running the Code

### Run All Steps

To execute all imputation steps in sequence:

```
cd ImputedSalesDemo
python code/run_all_steps.py
```

### Run Individual Steps

To run a specific imputation step:

```
cd ImputedSalesDemo
python code/steps/step_1.X_target_table_name.py
```

Replace `X` with the step number (1-12) and `target_table_name` with the corresponding output table name.

## Output

Each step generates a CSV file in the `processed_data` directory with the imputed sales data according to the respective business rule.
