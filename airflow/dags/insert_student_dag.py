from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
import uuid
import random

fake = Faker()


def insert_student():
    conn = psycopg2.connect(
        host="project_postgres",
        dbname="project_db",
        user="project_user",
        password="project_pass"
    )

    cur = conn.cursor()

    # Create schema and table if they don't exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS online_learning;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS online_learning.students (
            id UUID PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255) UNIQUE,
            password_hash VARCHAR(255),
            date_of_birth DATE,
            city VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    sid = str(uuid.uuid4())
    first = fake.first_name()
    last = fake.last_name()
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@{fake.free_email_domain()}"
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
    city = fake.city()
    pwdhash = fake.sha256()

    cur.execute(
        """
        INSERT INTO online_learning.students 
        (id, first_name, last_name, email, password_hash, date_of_birth, city)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
        """, (sid, first, last, email, pwdhash, dob, city)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted student: {first} {last} ({email})")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
        dag_id="insert_student_every_30s",
        default_args=default_args,
        start_date=datetime(2025, 11, 12),
        schedule_interval=timedelta(seconds=30),
        catchup=False,
        tags=["education", "postgres"],
) as dag:
    start_task = DummyOperator(task_id='start')

    insert_task = PythonOperator(
        task_id='insert_new_student',
        python_callable=insert_student
    )

    end_task = DummyOperator(task_id='end')

    # Create the workflow: start -> insert -> end
    start_task >> insert_task >> end_task