"""
Script to run enhanced step files locally for testing.

This script allows running individual enhanced step files with configurable parameters
for local testing and debugging.
"""

import os
import sys
import argparse
import importlib.util
import json
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

def load_step_class(step_file: Path):
    """Dynamically load the step class from a file.
    
    Args:
        step_file: Path to the step file
        
    Returns:
        The step class
    """
    # Get the module name from the file path
    module_name = step_file.stem
    
    # Import the module
    spec = importlib.util.spec_from_file_location(module_name, str(step_file))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not import module from {step_file}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    
    # Find the step class (class name should be derived from the filename)
    class_name = ''.join(word.capitalize() for word in module_name.split('_') if not word[0].isdigit()) + 'Step'
    step_class = getattr(module, class_name, None)
    
    if step_class is None:
        raise AttributeError(f"Could not find class '{class_name}' in {step_file}")
    
    return step_class

def load_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from a file.
    
    Args:
        config_file: Path to the config file (JSON or YAML)
        
    Returns:
        Dictionary containing the configuration
    """
    if not config_file:
        return {}
    
    config_file = Path(config_file)
    if not config_file.exists():
        print(f"Warning: Config file {config_file} does not exist")
        return {}
    
    try:
        with open(config_file, 'r') as f:
            if config_file.suffix.lower() == '.json':
                import json
                return json.load(f)
            elif config_file.suffix.lower() in ('.yaml', '.yml'):
                import yaml
                return yaml.safe_load(f)
            else:
                print(f"Warning: Unsupported config file format: {config_file.suffix}")
                return {}
    except Exception as e:
        print(f"Error loading config file {config_file}: {e}")
        return {}

def run_step(step_file: str, config: Dict[str, Any] = None, **kwargs) -> bool:
    """Run a single step.
    
    Args:
        step_file: Path to the step file
        config: Configuration dictionary
        **kwargs: Additional arguments to pass to the step
        
    Returns:
        True if the step completed successfully, False otherwise
    """
    step_path = Path(step_file)
    if not step_path.exists():
        print(f"Error: Step file {step_file} does not exist")
        return False
    
    print(f"Running step: {step_path.name}")
    
    try:
        # Load the step class
        step_class = load_step_class(step_path)
        
        # Create and run the step
        step = step_class(config=config, **kwargs)
        return step.run()
        
    except Exception as e:
        print(f"Error running step {step_path.name}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Run enhanced ETL steps locally')
    parser.add_argument('step_file', help='Path to the step file to run')
    parser.add_argument('--config', '-c', help='Path to config file (JSON or YAML)')
    parser.add_argument('--param', '-p', action='append', nargs=2, metavar=('KEY', 'VALUE'),
                       help='Additional parameters as key-value pairs')
    
    args = parser.parse_args()
    
    # Load config file if provided
    config = load_config(args.config)
    
    # Add command line parameters to config
    if args.param:
        for key, value in args.param:
            # Convert string values to appropriate types
            if value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            elif value.isdigit():
                value = int(value)
            elif value.replace('.', '', 1).isdigit():
                value = float(value)
                
            # Update config
            keys = key.split('.')
            current = config
            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]
            current[keys[-1]] = value
    
    # Run the step
    success = run_step(args.step_file, config)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
