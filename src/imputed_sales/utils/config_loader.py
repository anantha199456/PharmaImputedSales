"""
Configuration loader for the Imputed Sales Analytics Platform.

This module provides functionality to load and manage application configurations
from YAML files with support for environment-specific overrides.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Loads and manages application configurations from YAML files.
    
    Supports hierarchical configurations with environment-specific overrides.
    """
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize the ConfigLoader.
        
        Args:
            base_dir: Base directory containing config files
        """
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent.parent / 'config'
        self.config: Dict[str, Any] = {}
        self._load_configs()
    
    def _load_configs(self) -> None:
        """Load all configuration files from the config directory."""
        if not self.base_dir.exists():
            logger.warning(f"Config directory not found: {self.base_dir}")
            return
            
        # Load base config first
        base_config_path = self.base_dir / 'app' / 'base.yaml'
        if base_config_path.exists():
            self.config = self._load_yaml(base_config_path)
        else:
            logger.warning(f"Base config not found: {base_config_path}")
        
        # Load environment-specific overrides
        env = os.environ.get('APP_ENV', 'development').lower()
        env_config_path = self.base_dir / 'app' / f"{env}.yaml"
        
        if env_config_path.exists():
            env_config = self._load_yaml(env_config_path)
            self._deep_update(self.config, env_config)
            logger.info(f"Loaded environment config: {env_config_path}")
        else:
            logger.warning(f"Environment config not found: {env_config_path}")
        
        # Load Spark configurations
        self._load_spark_configs()
        
        # Load business rules
        self._load_business_rules()
    
    def _load_spark_configs(self) -> None:
        """Load Spark-specific configurations."""
        spark_configs = {}
        spark_dir = self.base_dir / 'spark'
        
        if not spark_dir.exists():
            return
            
        # Load default Spark configs
        default_spark = spark_dir / 'defaults.yaml'
        if default_spark.exists():
            spark_configs.update(self._load_yaml(default_spark))
        
        # Load optimizations if enabled
        if self.config.get('spark', {}).get('enable_optimizations', False):
            optimizations = spark_dir / 'optimizations.yaml'
            if optimizations.exists():
                spark_configs.update(self._load_yaml(optimizations))
        
        # Apply Spark configs to main config
        if 'spark' not in self.config:
            self.config['spark'] = {}
        
        self._deep_update(self.config['spark'], spark_configs)
    
    def _load_business_rules(self) -> None:
        """Load business rules configurations."""
        rules_dir = self.base_dir / 'business_rules'
        
        if not rules_dir.exists():
            return
            
        self.config['business_rules'] = {}
        
        # Load all YAML files in the business_rules directory
        for rule_file in rules_dir.glob('*.yaml'):
            rule_name = rule_file.stem
            self.config['business_rules'][rule_name] = self._load_yaml(rule_file)
    
    @staticmethod
    def _load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load a YAML file and return its contents as a dictionary.
        
        Args:
            file_path: Path to the YAML file
            
        Returns:
            Dictionary containing the YAML contents
        """
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Error loading YAML file {file_path}: {str(e)}")
            return {}
    
    @staticmethod
    def _deep_update(original: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively update a dictionary with another dictionary.
        
        Args:
            original: The original dictionary to update
            update: Dictionary with updates to apply
            
        Returns:
            The updated dictionary
        """
        for key, value in update.items():
            if key in original and isinstance(original[key], dict) and isinstance(value, dict):
                original[key] = ConfigLoader._deep_update(original[key], value)
            else:
                original[key] = value
        return original
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by dot notation key.
        
        Args:
            key: Dot notation key (e.g., 'app.name')
            default: Default value if key is not found
            
        Returns:
            The configuration value or default if not found
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark configuration as a flat dictionary.
        
        Returns:
            Dictionary of Spark configuration properties
        """
        spark_config = self.get('spark', {})
        return self._flatten_dict(spark_config, 'spark')
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, str]:
        """
        Flatten a nested dictionary into a single level with dot notation keys.
        
        Args:
            d: Dictionary to flatten
            parent_key: Parent key prefix
            sep: Separator between key parts
            
        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, str(v) if v is not None else ''))
        return dict(items)
    
    def reload(self) -> None:
        """Reload all configurations from disk."""
        self.config = {}
        self._load_configs()


# Global configuration instance
config = ConfigLoader()


def get_config() -> ConfigLoader:
    """
    Get the global configuration instance.
    
    Returns:
        The global ConfigLoader instance
    """
    return config
