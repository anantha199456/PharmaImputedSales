"""
Deployment script for the Imputed Sales Analytics Platform.

This script packages the project and uploads it to S3 for AWS Glue job execution.
"""

import os
import sys
import yaml
import boto3
import shutil
import zipfile
import tempfile
import logging
from pathlib import Path
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Deployer:
    """Handles deployment of the Imputed Sales Analytics Platform to AWS."""
    
    def __init__(self, config_path: str = None):
        """Initialize the deployer with configuration."""
        self.config = self._load_config(config_path)
        self.s3_client = boto3.client('s3', region_name=self.config['aws']['region'])
        self.project_root = Path(__file__).parent.parent
        
    def _load_config(self, config_path: Optional[str] = None) -> Dict:
        """Load configuration from file."""
        if config_path is None:
            config_path = self.project_root / 'config' / 'app' / 'base.yaml'
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def create_zip_archive(self, source_dir: Path, output_path: Path) -> Path:
        """
        Create a zip archive of a directory.
        
        Args:
            source_dir: Directory to zip
            output_path: Path to save the zip file
            
        Returns:
            Path to the created zip file
        """
        logger.info(f"Creating zip archive of {source_dir} at {output_path}")
        
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    # Skip __pycache__ and .pyc files
                    if '__pycache__' in root or file.endswith('.pyc'):
                        continue
                        
                    file_path = Path(root) / file
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
        
        logger.info(f"Created zip archive at {output_path}")
        return output_path
    
    def upload_to_s3(self, local_path: Path, s3_key: str) -> str:
        """
        Upload a file to S3.
        
        Args:
            local_path: Path to the local file
            s3_key: S3 key (path) to upload to
            
        Returns:
            S3 URL of the uploaded file
        """
        bucket = self.config['aws']['s3_bucket']
        s3_url = f"s3://{bucket}/{s3_key}"
        
        logger.info(f"Uploading {local_path} to {s3_url}")
        
        self.s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={
                'ServerSideEncryption': 'AES256',
                'ContentType': 'application/zip' if local_path.suffix == '.zip' else 'text/plain'
            }
        )
        
        logger.info(f"Successfully uploaded to {s3_url}")
        return s3_url
    
    def deploy_scripts(self) -> Dict[str, str]:
        """
        Deploy Python scripts to S3.
        
        Returns:
            Dictionary of script names to their S3 paths
        """
        scripts_dir = self.project_root / 'src' / 'imputed_sales' / 'etl' / 'scripts'
        s3_scripts = {}
        
        for script_file in scripts_dir.glob('*.py'):
            s3_key = f"scripts/{script_file.name}"
            s3_url = self.upload_to_s3(script_file, s3_key)
            s3_scripts[script_file.stem] = s3_url
        
        return s3_scripts
    
    def deploy_dependencies(self) -> str:
        """
        Package and deploy project dependencies to S3.
        
        Returns:
            S3 URL of the dependencies package
        """
        # Create a temporary directory for the package
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            
            # Copy the source code to the temp directory
            src_dir = self.project_root / 'src'
            shutil.copytree(src_dir, temp_dir / 'src')
            
            # Create setup.py
            setup_py = """
            from setuptools import setup, find_packages
            
            setup(
                name='imputed_sales',
                version='1.0.0',
                packages=find_packages(where='src'),
                package_dir={'': 'src'},
                install_requires=[
                    'pyspark>=3.1.1',
                    'pyyaml>=5.4.1',
                    'boto3>=1.17.0',
                    'pandas>=1.2.0',
                    'numpy>=1.20.0',
                ],
                python_requires='>=3.8',
            )
            """
            
            with open(temp_dir / 'setup.py', 'w') as f:
                f.write(setup_py.strip())
            
            # Create the source distribution
            import subprocess
            subprocess.run(
                ['python', 'setup.py', 'bdist_egg'],
                cwd=temp_dir,
                check=True
            )
            
            # Find the created egg file
            dist_dir = temp_dir / 'dist'
            egg_file = next(dist_dir.glob('*.egg'))
            
            # Upload the egg file to S3
            s3_key = f"dependencies/{egg_file.name}"
            return self.upload_to_s3(egg_file, s3_key)
    
    def deploy_configs(self) -> str:
        """
        Package and deploy configuration files to S3.
        
        Returns:
            S3 URL of the configs package
        """
        # Create a temporary directory for the configs
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            
            # Copy config files
            config_src = self.project_root / 'config'
            shutil.copytree(config_src, temp_dir / 'config')
            
            # Create a zip archive of the configs
            zip_path = temp_dir / 'configs.zip'
            self.create_zip_archive(temp_dir / 'config', zip_path)
            
            # Upload the zip file to S3
            s3_key = "configs/configs.zip"
            return self.upload_to_s3(zip_path, s3_key)
    
    def deploy_all(self) -> Dict[str, str]:
        """
        Deploy all components to S3.
        
        Returns:
            Dictionary of deployment artifacts and their S3 paths
        """
        logger.info("Starting deployment to S3")
        
        deployment = {
            'scripts': self.deploy_scripts(),
            'dependencies': self.deploy_dependencies(),
            'configs': self.deploy_configs()
        }
        
        logger.info("Deployment completed successfully")
        return deployment


def main():
    """Main entry point for the deployment script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy Imputed Sales Analytics Platform to AWS')
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to the configuration file'
    )
    
    args = parser.parse_args()
    
    try:
        deployer = Deployer(args.config)
        deployment = deployer.deploy_all()
        
        print("\nDeployment Summary:")
        print("=" * 50)
        print(f"Scripts:")
        for name, url in deployment['scripts'].items():
            print(f"  {name}: {url}")
        print(f"\nDependencies: {deployment['dependencies']}")
        print(f"Configs: {deployment['configs']}")
        
        return 0
    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
