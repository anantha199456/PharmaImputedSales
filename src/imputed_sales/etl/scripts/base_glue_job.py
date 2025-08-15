"""
Base AWS Glue job script for Imputed Sales ETL.

This script provides a base class for all Glue ETL jobs in the Imputed Sales Analytics Platform.
"""

import sys
import os
import logging
import yaml
import boto3
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class BaseGlueJob:
    """Base class for AWS Glue ETL jobs."""
    
    def __init__(self, args: Dict[str, str]):
        """Initialize the Glue job.
        
        Args:
            args: Command line arguments passed to the Glue job
        """
        self.args = args
        self.job_name = args.get('JOB_NAME', 'imputed-sales-job')
        self.env = args.get('ENV', 'dev')
        
        # Initialize Spark and Glue contexts
        self.sc = SparkContext.getOrCreate()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.job_name, args)
        
        # Load configuration
        self.config = self._load_config()
        
        # Set up S3 client
        self.s3_client = boto3.client('s3', region_name=self.config['aws']['region'])
        
        logger.info(f"Initialized Glue job: {self.job_name}")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from S3.
        
        Returns:
            Dictionary containing the configuration
        """
        try:
            # Download configs from S3
            bucket = self.args.get('CONFIG_BUCKET', self.config['aws']['s3_bucket'])
            config_key = self.args.get('CONFIG_KEY', 'configs/configs.zip')
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                self.s3_client.download_file(bucket, config_key, tmp_file.name)
                
                # Extract the configs
                with zipfile.ZipFile(tmp_file.name, 'r') as zip_ref:
                    zip_ref.extractall('/tmp/configs')
            
            # Load base config
            config_path = Path('/tmp/configs/config/app/base.yaml')
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Load environment-specific config if it exists
            env_config_path = Path(f'/tmp/configs/config/app/{self.env}.yaml')
            if env_config_path.exists():
                with open(env_config_path, 'r') as f:
                    env_config = yaml.safe_load(f)
                    config.update(env_config)
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def extract(self) -> Dict[str, Any]:
        """Extract data from source systems.
        
        Returns:
            Dictionary of extracted DataFrames
        """
        raise NotImplementedError("Subclasses must implement extract()")
    
    def transform(self, data: Dict[str, Any]) -> Any:
        """Transform the extracted data.
        
        Args:
            data: Dictionary of DataFrames from extract()
                
        Returns:
            Transformed data
        """
        raise NotImplementedError("Subclasses must implement transform()")
    
    def load(self, data: Any) -> None:
        """Load the transformed data to the target system.
        
        Args:
            data: Data to load (usually a DataFrame)
        """
        raise NotImplementedError("Subclasses must implement load()")
    
    def run(self) -> None:
        """Run the ETL job."""
        try:
            logger.info(f"Starting ETL job: {self.job_name}")
            
            # Execute ETL steps
            extracted_data = self.extract()
            transformed_data = self.transform(extracted_data)
            self.load(transformed_data)
            
            logger.info(f"Successfully completed ETL job: {self.job_name}")
            
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}", exc_info=True)
            raise
        finally:
            # Clean up Spark context
            self.job.commit()
            self.sc.stop()


def get_job_args() -> Dict[str, str]:
    """Get job arguments from command line and environment variables."""
    # Get arguments from Glue
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'ENV',
            'CONFIG_BUCKET',
            'CONFIG_KEY',
            '--additional-python-modules',
            '--extra-py-files',
            '--conf',
            '--job-bookmark-option',
            '--job-language',
            '--job-type',
            '--region',
            '--TempDir',
            '--enable-metrics',
            '--enable-spark-ui',
            '--spark-event-logs-path',
            '--enable-job-insights',
            '--enable-continuous-cloudwatch-log',
        ]
    )
    
    # Add any additional arguments from environment variables
    for key, value in os.environ.items():
        if key.startswith('JOB_ARG_'):
            arg_name = key[8:].lower()  # Remove 'JOB_ARG_' prefix
            args[arg_name] = value
    
    return args


def main():
    """Main entry point for the Glue job."""
    # Get job arguments
    args = get_job_args()
    
    try:
        # Import the job class dynamically
        job_module = os.environ.get('JOB_MODULE', 'base_glue_job')
        job_class = os.environ.get('JOB_CLASS', 'BaseGlueJob')
        
        # Import the module and get the class
        module = __import__(f'src.imputed_sales.etl.scripts.{job_module}', 
                          fromlist=[job_class])
        job_class = getattr(module, job_class)
        
        # Create and run the job
        job = job_class(args)
        job.run()
        
        return 0
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
