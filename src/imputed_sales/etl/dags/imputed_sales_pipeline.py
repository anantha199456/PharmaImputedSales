"""
Airflow DAG for Imputed Sales ETL Pipeline.

This DAG orchestrates the execution of AWS Glue jobs for the imputed sales ETL process.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from typing import Dict, List, Optional
import os
import yaml

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'aws_conn_id': 'aws_default',
    'region_name': 'us-west-2',  # Update with your AWS region
}

# Load configuration
config_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'config',
    'app',
    'base.yaml'
)

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# DAG definition
dag = DAG(
    'imputed_sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for imputed sales data processing',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['imputed_sales', 'etl', 'glue'],
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

def get_glue_job_config(step: str) -> Dict:
    """Get configuration for a specific Glue job step."""
    return {
        'JobName': f'imputed-sales-{step}',
        'ScriptLocation': f's3://{config["aws"]["s3_bucket"]}/scripts/{step}.py',
        'Role': config['aws']['glue_role'],
        'GlueVersion': '3.0',
        'WorkerType': 'G.1X',
        'NumberOfWorkers': 5,
        'DefaultArguments': {
            '--job-language': 'python',
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-metrics': '',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': f's3://{config["aws"]["s3_bucket"]}/spark-logs/',
            '--TempDir': f's3://{config["aws"]["s3_bucket"]}/temp/',
            '--enable-job-insights': 'true',
            '--extra-py-files': f's3://{config["aws"]["s3_bucket"]}/dependencies/imputed_sales-1.0.0-py3.8.egg',
        }
    }

def create_glue_task(step: str, depends_on: Optional[list] = None) -> GlueJobOperator:
    """Create a Glue job task for a specific step."""
    job_config = get_glue_job_config(step)
    
    return GlueJobOperator(
        task_id=f'run_{step}_job',
        job_name=job_config['JobName'],
        script_location=job_config['ScriptLocation'],
        aws_conn_id=default_args['aws_conn_id'],
        region_name=default_args['region_name'],
        iam_role_name=job_config['Role'],
        create_job_kwargs={
            'GlueVersion': job_config['GlueVersion'],
            'WorkerType': job_config['WorkerType'],
            'NumberOfWorkers': job_config['NumberOfWorkers'],
            'DefaultArguments': job_config['DefaultArguments'],
            'Command': {
                'Name': 'glueetl',
                'PythonVersion': '3',
                'ScriptLocation': job_config['ScriptLocation']
            },
            'ExecutionProperty': {
                'MaxConcurrentRuns': 1
            },
            'Tags': {
                'Project': 'ImputedSales',
                'Environment': config['environment']
            }
        },
        dag=dag,
    )

def create_glue_sensor(step: str, depends_on: Optional[list] = None) -> GlueJobSensor:
    """Create a sensor to monitor Glue job completion."""
    job_config = get_glue_job_config(step)
    
    return GlueJobSensor(
        task_id=f'wait_for_{step}_job',
        job_name=job_config['JobName'],
        run_id=f'{{{{ task_instance.xcom_pull(task_ids="run_{step}_job")["JobRunId"] }}}}',
        aws_conn_id=default_args['aws_conn_id'],
        region_name=default_args['region_name'],
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # 1 hour timeout
        mode='reschedule',
        dag=dag,
    )

# Define the ETL steps
etl_steps = [
    '1.1_inventory_adjusted_sales',
    '1.2_secondary_sales_adjustment',
    '1.3_rolling_average_estimation',
    '1.4_market_share_estimation',
    '1.5_tender_based_estimation',
    '1.6_return_adjustment',
    '1.7_lives_based_imputation',
    '1.8_claims_based_estimation',
    '1.9_utilization_rate_estimation',
    '1.10_formulary_tier_influence',
    '1.11_dosage_unit_conversion',
    '1.12_adherence_based_adjustment',
    '2.0_consolidated_sales'
]

# Create tasks for each step
tasks = {}
for step in etl_steps:
    task_id = step.replace('.', '_')
    tasks[f'run_{task_id}'] = create_glue_task(step)
    tasks[f'wait_{task_id}'] = create_glue_sensor(step)

# Set task dependencies
for i in range(len(etl_steps) - 1):
    current_step = etl_steps[i].replace('.', '_')
    next_step = etl_steps[i + 1].replace('.', '_')
    
    tasks[f'run_{next_step}'].set_upstream(tasks[f'wait_{current_step}'])
    tasks[f'wait_{next_step}'].set_upstream(tasks[f'run_{next_step}'])

# Add start and end tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Set initial and final dependencies
first_step = etl_steps[0].replace('.', '_')
last_step = etl_steps[-1].replace('.', '_')

tasks[f'run_{first_step}'].set_upstream(start_task)
tasks[f'wait_{last_step}'].set_downstream(end_task)

# Add monitoring and alerting
def send_slack_notification(context):
    """Send notification to Slack about the pipeline status."""
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    slack_msg = """
    :red_circle: Task Failed.
    *Task*: {task}  
    *Dag*: {dag} 
    *Execution Time*: {exec_date}  
    *Log Url*: {log_url} 
    """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date').isoformat(),
        log_url=context.get('task_instance').log_url,
    )
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg,
        username='airflow',
    )
    
    return failed_alert.execute(context=context)

# Set up error handling
for step in etl_steps:
    task_id = step.replace('.', '_')
    tasks[f'run_{task_id}'].on_failure_callback = send_slack_notification
    tasks[f'wait_{task_id}'].on_failure_callback = send_slack_notification

# Add data quality check task
def run_data_quality_checks(**kwargs):
    """Run data quality checks after the ETL pipeline completes."""
    from pyspark.sql import SparkSession
    from imputed_sales.core.data_validator import DataValidator
    
    spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()
    
    # Initialize the data validator
    validator = DataValidator(spark)
    
    # Define data quality checks
    checks = [
        # Example: Check if the final sales table has the expected columns
        {
            'name': 'check_final_sales_columns',
            'description': 'Check if final sales table has all required columns',
            'query': """
                SELECT 
                    COUNT(*) AS missing_columns
                FROM (
                    SELECT 
                        CASE 
                            WHEN COUNT(*) = COUNT(DISTINCT column_name) 
                                 AND SUM(CASE WHEN column_name IN ('product_id', 'date', 'quantity', 'amount') 
                                        THEN 1 ELSE 0 END) = 4 
                            THEN 1 
                            ELSE 0 
                        END AS all_columns_exist
                    FROM information_schema.columns 
                    WHERE table_name = 'consolidated_sales'
                ) t
                WHERE all_columns_exist = 0
            """,
            'expectation': 'result == 0',
            'severity': 'blocker'
        },
        # Add more data quality checks as needed
    ]
    
    # Run the checks
    results = []
    for check in checks:
        result = spark.sql(check['query']).collect()[0][0]
        passed = eval(check['expectation'], {'result': result})
        
        results.append({
            'check_name': check['name'],
            'description': check['description'],
            'result': 'PASSED' if passed else 'FAILED',
            'severity': check.get('severity', 'warning'),
            'details': f"Expected: {check['expectation']}, Actual: {result}"
        })
    
    # Log the results
    for result in results:
        if result['result'] == 'FAILED' and result['severity'] == 'blocker':
            logger.error(f"Data quality check failed: {result}")
            raise ValueError(f"Blocker data quality check failed: {result['check_name']}")
        elif result['result'] == 'FAILED':
            logger.warning(f"Data quality check warning: {result}")
        else:
            logger.info(f"Data quality check passed: {result['check_name']}")
    
    return results

# Add the data quality check task
data_quality_check = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag,
)

# Set the data quality check to run after the pipeline completes
data_quality_check.set_upstream(tasks[f'wait_{last_step}'])
data_quality_check.set_downstream(end_task)
