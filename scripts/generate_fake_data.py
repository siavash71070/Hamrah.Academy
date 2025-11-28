import os
import random
from faker import Faker
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
# Load .env file
load_dotenv()

fake = Faker()
NUM_STUDENTS = 100
NUM_INSTRUCTORS = 10
NUM_COURSES = 30

# Connect to PostgreSQL
def connect_db():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST',"postgres"),
            port=os.getenv('DB_PORT',"5432"),
            database=os.getenv('DB_NAME',"lms_db"),
            user=os.getenv('DB_USER',"postgres"),
            password=os.getenv('DB_PASSWORD',"123456") 
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


# -----------------------------
# Generate fake students
# -----------------------------
def generate_students(conn):
    cursor = conn.cursor()
    sql = """
    INSERT INTO students (first_name, last_name, email, created_at)
    VALUES (%s, %s, %s, %s) RETURNING student_id;
    """
    student_ids = []
    for _ in range(NUM_STUDENTS):
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{fake.domain_name()}"
        reg_date = datetime.now() - timedelta(days=random.randint(1, 365))
        try:
            cursor.execute(sql, (first, last, email, reg_date))
            student_ids.append(cursor.fetchone()[0])
        except psycopg2.IntegrityError:
            conn.rollback()
            continue 
    conn.commit()
    return student_ids



# -----------------------------
# Generate fake instructors
# -----------------------------
def generate_instructors(conn):
    cursor = conn.cursor()
    sql = """
    INSERT INTO instructors (first_name, last_name, email, bio)
    VALUES (%s, %s, %s, %s) RETURNING instructor_id;
    """
    instructor_ids = []
    for _ in range(NUM_INSTRUCTORS):
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}@profs.com"
        bio = fake.paragraph(nb_sentences=3)
        try:
            cursor.execute(sql, (first, last, email, bio))
            instructor_ids.append(cursor.fetchone()[0])
        except psycopg2.IntegrityError:
            conn.rollback()
            continue
    conn.commit()
    print(f"Generated {len(instructor_ids)} instructors.")
    return instructor_ids

# -----------------------------
# Generate fake courses
# -----------------------------
def generate_courses(conn, instructor_ids):

    cursor = conn.cursor()
    sql = """
    INSERT INTO courses (instructor_id, title, description, price, level)
    VALUES (%s, %s, %s, %s, %s) RETURNING course_id;
    """
    course_ids = []
    levels = ['Beginner', 'Intermediate', 'Advanced']
    
    for i in range(NUM_COURSES):
        instructor_id = random.choice(instructor_ids)
        title = fake.catch_phrase() + f" ({i+1})"
        price = round(random.uniform(9.99, 199.99), 2)
        level = random.choice(levels)
        
        try:
            cursor.execute(sql, (instructor_id, title, fake.text(max_nb_chars=200), price, level))
            course_ids.append(cursor.fetchone()[0])
        except psycopg2.IntegrityError:
            conn.rollback()
            continue

    conn.commit()
    print(f"Generated {len(course_ids)} courses.")
    return course_ids

def generate_quizzes(conn, course_ids):

    cursor = conn.cursor()
    quiz_sql = """
    INSERT INTO quizzes (course_id, title, max_score)
    VALUES (%s, %s, %s);
    """
    
    for course_id in course_ids:

        if random.random() < 0.3:
            quiz_title = f"Quiz: {fake.word()} Assessment"
            max_score = random.choice([10, 20, 25])  
            cursor.execute(quiz_sql, (course_id, quiz_title, max_score))

    conn.commit()
    print("Generated lessons and quizzes.")

def generate_enrollments_and_payments(conn, student_ids, course_ids):
    cursor = conn.cursor()
    enrollment_sql = """
    INSERT INTO enrollments (student_id, course_id, enrolled_at)
    VALUES (%s, %s, %s) RETURNING enrollment_id;
    """
    payment_sql = """
    INSERT INTO payments (enrollment_id, amount, payment_date, payment_method)
    VALUES (%s, %s, %s, %s);
    """
    
    enrollment_data = set()
    num_enrollments = int(NUM_STUDENTS * len(course_ids) * 0.1) # Enroll ~10% of total possibilities

    while len(enrollment_data) < num_enrollments:
        student_id = random.choice(student_ids)
        course_id = random.choice(course_ids)
        enrollment_data.add((student_id, course_id))

    for student_id, course_id in enrollment_data:
        # Fetch the course price (requires a small helper query or hardcoding prices during generation)
        # For simplicity, let's use a random amount that suggests a full price payment.
        amount = round(random.uniform(9.99, 199.99), 2)
        
        enroll_date = datetime.now() - timedelta(days=random.randint(1, 300))
        progress = random.randint(0, 100) if enroll_date < datetime.now() - timedelta(days=60) else random.randint(0, 30)
        
        try:
            cursor.execute(enrollment_sql, (student_id, course_id, enroll_date))
            enrollment_id = cursor.fetchone()[0]
            
            # Generate Payment
            payment_methods = ['Credit Card', 'PayPal', 'Cash']
            payment_date = enroll_date + timedelta(hours=random.randint(1, 24))
            cursor.execute(payment_sql, (enrollment_id, amount, payment_date, random.choice(payment_methods)))
            
        except psycopg2.IntegrityError as e:
            conn.rollback()
            # print(f"Skipping duplicate enrollment: {e}")
            continue

    conn.commit()
    print(f"Generated {len(enrollment_data)} enrollments and payments.")


def main_data_generation():
    conn = connect_db()
    if conn is None:
        print("Cannot proceed without database connection.")
        return

    try:
        # Get existing IDs or generate fresh data
        instructor_ids = generate_instructors(conn)
        student_ids = generate_students(conn)
        course_ids = generate_courses(conn, instructor_ids)
        if not student_ids or not instructor_ids or not course_ids:
            print("Not enough base data to continue. Check DB connection and previous generation.")
            return
        generate_quizzes(conn, course_ids)
        generate_enrollments_and_payments(conn, student_ids, course_ids)
        print("\nInitial bulk data generation complete.")

    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main_data_generation() 
