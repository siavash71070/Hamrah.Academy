import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()


FAKE = Faker()
NEW_STUDENTS_COUNT = 3
NEW_ENROLLMENTS_COUNT = 5

def connect_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST",'postgres'),
            port=os.getenv("DB_PORT",'5432'),
            database=os.getenv("DB_NAME",'lms_db'),
            user=os.getenv("DB_USER",'postgres'),
            password=os.getenv("DB_PASSWORD",'123456') 
        )
        return conn
    except Exception as e:

        print(f"Error connecting to the database: {e}")
        raise

def get_existing_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT student_id FROM students;")
    student_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT course_id FROM courses;")
    course_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT enrollment_id FROM enrollments;")
    enrollment_ids = [row[0] for row in cursor.fetchall()]

    return student_ids, course_ids, enrollment_ids

def insert_new_student(cursor):

    first = FAKE.first_name()
    last = FAKE.last_name()
    # Ensure a unique email by combining name, random number, and timestamp
    email = f"{first.lower()}.{last.lower()}_{random.randint(1, 1000)}@{FAKE.domain_name()}"
    reg_date = datetime.now() - timedelta(minutes=random.randint(1, 5))
    
    sql = """
    INSERT INTO students (first_name, last_name, email, created_at)
    VALUES (%s, %s, %s, %s) RETURNING student_id;
    """
    cursor.execute(sql, (first, last, email, reg_date))
    return cursor.fetchone()[0]

def insert_new_enrollment_and_payment(cursor, student_ids, course_ids):

    if not student_ids or not course_ids:

        return

    student_id = random.choice(student_ids)
    course_id = random.choice(course_ids)
    cursor.execute("SELECT COUNT(*) FROM enrollments WHERE student_id = %s AND course_id = %s;", (student_id, course_id))
    if cursor.fetchone()[0] > 0:
        return 

    enroll_date = datetime.now() - timedelta(minutes=random.randint(1, 15))
    progress = 0 
    
    # 1. Insert Enrollment
    enrollment_sql = """
    INSERT INTO enrollments (student_id, course_id, enrolled_at)
    VALUES (%s, %s, %s) RETURNING enrollment_id;
    """
    cursor.execute(enrollment_sql, (student_id, course_id, enroll_date))
    enrollment_id = cursor.fetchone()[0]
    print(f"  -> Inserted new enrollment: Student {student_id} in Course {course_id}")
    
    # 2. Insert Payment
    amount = round(random.uniform(20.00, 300.00), 2)
    payment_methods = ['Credit Card', 'PayPal']
    payment_date = enroll_date + timedelta(seconds=random.randint(5, 60))
    payment_sql = """
    INSERT INTO payments (enrollment_id, amount, payment_date, payment_method)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(payment_sql, (enrollment_id, amount, payment_date, random.choice(payment_methods)))
    print(f"  -> Inserted new payment for enrollment {enrollment_id}")


def run_incremental_generation():

    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        student_ids, course_ids, enrollment_ids = get_existing_ids(conn)
        
        # 1. Insert New Students
        for _ in range(NEW_STUDENTS_COUNT):
            new_student_id = insert_new_student(cursor)
            student_ids.append(new_student_id)

        # 2. Insert New Enrollments and Payments (using existing or newly created students)
        for _ in range(NEW_ENROLLMENTS_COUNT):
            insert_new_enrollment_and_payment(cursor, student_ids, course_ids)
            
        conn.commit()
        print(f"\nBatch completed: {NEW_STUDENTS_COUNT} students and {NEW_ENROLLMENTS_COUNT} enrollments/payments generated.")
    
    except Exception as e:
        conn.rollback()
        print(f"Transaction failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_incremental_generation()
