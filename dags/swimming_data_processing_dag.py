from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import shutil
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define paths
RAW_DATA_PATH = '/scripts/raw_data'
PROCESSED_PATH = '/scripts/processed_data'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_for_new_files(**context):
    """Check for new files in the raw_data directory"""
    try:
        if not os.path.exists(RAW_DATA_PATH):
            raise Exception(f"Raw data directory not found at {RAW_DATA_PATH}")
        
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith('_results.csv')]
        logger.info(f"Found {len(files)} files to process")
        return len(files) > 0
    except Exception as e:
        logger.error(f"Error checking for new files: {str(e)}")
        raise

def trigger_processing(**context):
    """Trigger the swimming data processing"""
    try:
        from DataExtraction.process_swimming_data import SwimmingDataProcessor
        
        # Initialize and run processor
        processor = SwimmingDataProcessor()
        processor.process_swimming_data()
        
        # Move processed files to processed directory
        if not os.path.exists(PROCESSED_PATH):
            os.makedirs(PROCESSED_PATH)
            
        for file in os.listdir(RAW_DATA_PATH):
            if file.endswith('_results.csv'):
                os.rename(
                    os.path.join(RAW_DATA_PATH, file),
                    os.path.join(PROCESSED_PATH, f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file}")
                )
        
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Error in processing: {str(e)}")
        raise

# Create the DAG
dag = DAG(
    'swimming_data_processing',
    default_args=default_args,
    description='Process swimming competition data and create views',
    schedule_interval=timedelta(minutes=5),
    catchup=False
)

# Define tasks
check_files = PythonOperator(
    task_id='check_for_new_files',
    python_callable=check_for_new_files,
    provide_context=True,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=trigger_processing,
    provide_context=True,
    dag=dag
)

# Create views task using PostgresOperator
create_views = PostgresOperator(
    task_id='create_views',
    postgres_conn_id='swimming_db',
    sql='/scripts/DataExtraction/create_views.sql',
    dag=dag
)

# Set task dependencies
check_files >> process_data >> create_views 