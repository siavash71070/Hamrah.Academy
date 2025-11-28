from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys
import os

def run_bulk_data():
    """Run the bulk data generation script using subprocess"""
    try:
        print("🚀 Starting bulk data generation...")
        
        # Run the script as subprocess
        result = subprocess.run(
            [sys.executable, "/opt/airflow/scripts/generate_fake_data.py"],
            capture_output=True,
            text=True,
            timeout=300,
            cwd="/opt/airflow/scripts"
        )
        
        if result.returncode == 0:
            print("✅ Bulk data generation successful!")
            print(f"Output: {result.stdout}")
        else:
            print(f"❌ Script failed: {result.stderr}")
            raise Exception(f"Script execution failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='lms_bulk_data_generator',
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    catchup=False,
    description='Generate bulk LMS data',
) as dag:

    generate_bulk_data_task = PythonOperator(
        task_id='generate_bulk_data',
        python_callable=run_bulk_data,
    )
