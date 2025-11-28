import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_cdc():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'lms_db'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', '123456')
    )
    
    cursor = conn.cursor()
    
    # Insert a test student
    cursor.execute("""
    INSERT INTO students (first_name, last_name, email, phone)
    VALUES ('Test', 'User', 'test.user@example.com', '123-456-7890')
    RETURNING student_id;
    """)
    
    student_id = cursor.fetchone()[0]
    conn.commit()
    
    print(f"✅ Inserted test student with ID: {student_id}")
    
    # Check current student count
    cursor.execute("SELECT count(*) FROM students;")
    count = cursor.fetchone()[0]
    print(f"✅ Total students in PostgreSQL: {count}")
    
    cursor.close()
    conn.close()
    
    return student_id, count

if __name__ == "__main__":
    test_cdc()
