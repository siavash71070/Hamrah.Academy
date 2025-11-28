from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

def test_environment():
    """Test if the environment is set up correctly"""
    try:
        # Test Python imports
        import psycopg2
        import faker
        print("✅ Python imports successful")
        
        # Test script execution
        result = subprocess.run(
            [sys.executable, '/opt/airflow/scripts/generate_incremental_data.py'],
            capture_output=True, text=True, timeout=60
        )
        
        if result.returncode == 0:
            print("✅ Script execution successful")
            print(f"Output: {result.stdout}")
        else:
            print(f"❌ Script failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Environment test failed: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'lms_test_dag',
    default_args=default_args,
    description='Test LMS environment and data generation',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['lms', 'test'],
) as dag:

    test_env = PythonOperator(
        task_id='test_environment',
        python_callable=test_environment,
    )

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='cd /opt/airflow/scripts && python generate_incremental_data.py',
    )

    test_env >> generate_data
