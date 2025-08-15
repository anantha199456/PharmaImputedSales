"""
Imputed Sales Process - Main Orchestration Script

This script orchestrates the execution of all imputed sales calculation steps
based on the business rules provided.
"""

import os
import sys
import importlib
import time
from datetime import datetime

def main():
    """
    Main function to orchestrate the imputed sales process
    """
    start_time = time.time()
    
    print(f"Starting imputed sales process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Define base directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    steps_dir = os.path.join(base_dir, "steps")
    
    # Ensure the processed_data directory exists
    processed_data_dir = os.path.join(os.path.dirname(base_dir), "processed_data")
    os.makedirs(processed_data_dir, exist_ok=True)
    
    # Get all step modules in order
    step_files = sorted([f for f in os.listdir(steps_dir) if f.startswith("step_") and f.endswith(".py")])
    
    if not step_files:
        print("No step files found in the steps directory.")
        return
    
    # Execute each step
    for step_file in step_files:
        step_name = step_file[:-3]  # Remove .py extension
        print(f"\n{'='*80}\nExecuting {step_name}\n{'='*80}")
        
        try:
            # Import the step module
            step_module = importlib.import_module(f"steps.{step_name}")
            
            # Execute the step
            step_module.main()
            
        except Exception as e:
            print(f"Error executing {step_name}: {str(e)}")
            print("Continuing with next step...")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"\n{'='*80}")
    print(f"Imputed sales process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {execution_time:.2f} seconds")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
