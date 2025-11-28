from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    'lms_simple_generator',
    default_args=default_args,
    description='Simple LMS data generator using Bash',
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=['lms', 'simple'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_incremental_data',
        bash_command='cd /opt/airflow/scripts && python generate_incremental_data.py',
    )
