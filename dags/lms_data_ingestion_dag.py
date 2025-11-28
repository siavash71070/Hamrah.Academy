from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys
import os

def run_incremental_data():
    """Run the incremental data generation script using subprocess"""
    try:
        print("🚀 Starting incremental data generation...")
        
        # Run the script as subprocess
        result = subprocess.run(
            [sys.executable, "/opt/airflow/scripts/generate_incremental_data.py"],
            capture_output=True,
            text=True,
            timeout=120,
            cwd="/opt/airflow/scripts"
        )
        
        if result.returncode == 0:
            print("✅ Incremental data generation successful!")
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
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='lms_incremental_data_generator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    description='Generate incremental LMS data',
) as dag:

    generate_data_task = PythonOperator(
        task_id='generate_incremental_data_batch',
        python_callable=run_incremental_data,
    )
