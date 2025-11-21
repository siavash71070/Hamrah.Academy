#!/usr/bin/env python3
"""
generate_fake_data.py
پارامترها:
  --db-host (default: localhost)
  --db-port (default: 5432)
  --db-name (default: project_db)
  --db-user (default: project_user)
  --db-pass (default: project_pass)
  --n-students (default: 1000)
  --n-instructors (default: 100)
  --n-courses (default: 200)
  --max-lessons-per-course (default: 20)
  --seed (optional)
مثال:
  python generate_fake_data.py --n-students 500 --n-courses 50
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm



fake = Faker()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-host", default="localhost")
    p.add_argument("--db-port", default=5432, type=int)
    p.add_argument("--db-name", default="project_db")
    p.add_argument("--db-user", default="project_user")
    p.add_argument("--db-pass", default="project_pass")
    p.add_argument("--n-students", default=1000, type=int)
    p.add_argument("--n-instructors", default=100, type=int)
    p.add_argument("--n-courses", default=200, type=int)
    p.add_argument("--max-lessons-per-course", default=12, type=int)
    p.add_argument("--seed", default=None, type=int)

    return p.parse_args()


def connect(cfg):
    return psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_pass
    )

def bulk_insert_students(cur, n):
    rows = []
    for _ in range(n):
        sid = str(uuid.uuid4())
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1,9999)}@{fake.free_email_domain()}"
        dob = fake.date_of_birth(tzinfo=None, minimum_age=15, maximum_age=70)
        city = fake.city()
        pwdhash = fake.sha256()
        created = fake.date_time_between(start_date='-2y', end_date='now')
        rows.append((sid, first, last, email, pwdhash, dob, city, created, True))

    sql = """
      INSERT INTO online_learning.students
      (id, first_name, last_name, email, password_hash, date_of_birth, city, created_at, is_active)
      VALUES %s
      ON CONFLICT (email) DO NOTHING
        """
    execute_values(cur, sql, rows)
    return [r[0] for r in rows]

def bulk_insert_instructors(cur, n):
    rows = []
    for _ in range(n):
        iid = str(uuid.uuid4())
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}.{random.randint(1,9999)}@{fake.free_email_domain()}"
        bio = fake.sentence(nb_words=12)
        created = fake.date_time_between(start_date='-3y', end_date='now')
        rows.append((iid, first, last, email, bio, created, True))
    sql = """
      INSERT INTO online_learning.instructors
      (id, first_name, last_name, email, bio, created_at, is_active)
      VALUES %s
      ON CONFLICT (email) DO NOTHING
    """
    execute_values(cur, sql, rows)
    return [r[0] for r in rows]

def bulk_insert_courses(cur, n):
    rows = []
    for i in range(n):
        cid = str(uuid.uuid4())
        title = fake.sentence(nb_words=4).rstrip('.')
        slug = f"{title.lower().replace(' ', '-')}-{random.randint(1000,9999)}"
        desc = fake.paragraph(nb_sentences=3)
        price = round(random.choice([0, 9.99, 19.99, 29.99, 49.99, 99.99]) * random.uniform(0.5,1.5),2)
        lang = random.choice(['en','fa','es','de'])
        created = fake.date_time_between(start_date='-2y', end_date='now')
        is_published = random.random() < 0.8
        rows.append((cid, title, slug, desc, price, lang, created, is_published))
    sql = """
      INSERT INTO online_learning.courses
      (id, title, slug, description, price, language, created_at, is_published)
      VALUES %s
      ON CONFLICT (slug) DO NOTHING
    """
    execute_values(cur, sql, rows)
    return [r[0] for r in rows]

def bulk_insert_course_instructors(cur, course_ids, instr_ids):
    rows = []
    for cid in course_ids:
        # assign 1-3 instructors per course
        chosen = random.sample(instr_ids, k=max(1, min(3, random.randint(1,3))))
        for iid in chosen:
            role = random.choice(['author','co-author','assistant'])
            rows.append((cid, iid, role))
    sql = """
      INSERT INTO online_learning.course_instructors
      (course_id, instructor_id, role)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    execute_values(cur, sql, rows)
    return

def bulk_insert_lessons(cur, course_ids, max_per_course=12):
    rows = []
    for cid in course_ids:
        n = random.randint(3, max_per_course)
        for pos in range(1, n+1):
            lid = str(uuid.uuid4())
            title = f"Lesson {pos}: " + fake.sentence(nb_words=5).rstrip('.')
            dur = random.randint(60, 3600)  # seconds
            created = fake.date_time_between(start_date='-2y', end_date='now')
            rows.append((lid, cid, title, pos, None, dur, created))
    sql = """
      INSERT INTO online_learning.lessons
      (id, course_id, title, position, content, duration_seconds, created_at)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    execute_values(cur, sql, rows)
    return

def bulk_insert_quizzes(cur, course_ids):
    rows = []
    for cid in course_ids:
        if random.random() < 0.6:  # 60% courses have a quiz
            qid = str(uuid.uuid4())
            title = f"Quiz for {fake.word()}"
            passing = random.choice([50,60,70,80])
            created = fake.date_time_between(start_date='-2y', end_date='now')
            rows.append((qid, cid, title, passing, created))
    sql = """
      INSERT INTO online_learning.quizzes
      (id, course_id, title, passing_score, created_at)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    execute_values(cur, sql, rows)
    return

def bulk_insert_enrollments_payments_reviews_certificates(cur, student_ids, course_ids):
    enroll_rows = []
    payment_rows = []
    review_rows = []
    cert_rows = []
    for sid in tqdm(student_ids, desc="creating enrollments"):
        # each student enrolls in 0..10 courses
        n = random.choices(range(0,8), weights=[40,30,10,7,5,4,3,1], k=1)[0]
        chosen = random.sample(course_ids, k=min(len(course_ids), n)) if n>0 else []
        for cid in chosen:
            eid = str(uuid.uuid4())
            enrolled_at = fake.date_time_between(start_date='-2y', end_date='now')
            progress = round(random.uniform(0,100),2)
            status = 'completed' if progress > 95 else ('enrolled' if progress < 95 else 'completed')
            enroll_rows.append((eid, sid, cid, enrolled_at, progress, status))
            # payment sometimes exists (e.g., paid courses price>0)
            if random.random() < 0.8:
                payid = str(uuid.uuid4())
                amount = round(random.uniform(0,1) * 100,2)
                provider = random.choice(['stripe','paypal','razorpay','manual'])
                paid_at = enrolled_at + timedelta(days=random.randint(0,7))
                status_pay = 'succeeded' if random.random() < 0.95 else 'failed'
                payment_rows.append((payid, eid, sid, cid, amount, 'USD', provider, paid_at, status_pay))
            # review sometimes
            if random.random() < 0.2:
                rid = str(uuid.uuid4())
                rating = random.randint(1,5)
                comment = fake.sentence(nb_words=12)
                created = paid_at if 'paid_at' in locals() else fake.date_time_between(start_date=enrolled_at, end_date='now')
                review_rows.append((rid, sid, cid, rating, comment, created))
            # certificate for completed sometimes
            if status == 'completed' and random.random() < 0.6:
                cid_cert = str(uuid.uuid4())
                cert_code = f"CERT-{random.randint(100000,999999)}"
                issued = enrolled_at + timedelta(days=random.randint(7,120))
                cert_rows.append((cid_cert, sid, cid, issued, cert_code))

    if enroll_rows:
        sql_en = """
          INSERT INTO online_learning.enrollments
          (id, student_id, course_id, enrolled_at, progress_percent, status)
          VALUES %s
          ON CONFLICT DO NOTHING
        """
        execute_values(cur, sql_en, enroll_rows, page_size=500)

    if payment_rows:
        sql_pay = """
          INSERT INTO online_learning.payments
          (id, enrollment_id, student_id, course_id, amount, currency, provider, paid_at, status)
          VALUES %s
          ON CONFLICT DO NOTHING
        """
        execute_values(cur, sql_pay, payment_rows, page_size=500)

    if review_rows:
        sql_rev = """
          INSERT INTO online_learning.reviews
          (id, student_id, course_id, rating, comment, created_at)
          VALUES %s
          ON CONFLICT (student_id, course_id) DO NOTHING
        """
        execute_values(cur, sql_rev, review_rows, page_size=500)

    if cert_rows:
        sql_cert = """
          INSERT INTO online_learning.certificates
          (id, student_id, course_id, issued_at, cert_code)
          VALUES %s
          ON CONFLICT DO NOTHING
        """
        execute_values(cur, sql_cert, cert_rows, page_size=500)

    return

def insert_logs(cur, n=500):
    rows = []
    for _ in range(n):
        eid = str(uuid.uuid4())
        entity_type = random.choice(['student','course','enrollment','payment','lesson'])
        entity_id = str(uuid.uuid4())
        action = random.choice(['created','updated','deleted','viewed'])
        payload = {"info": fake.sentence(nb_words=6)}
        created = fake.date_time_between(start_date='-1y', end_date='now')
        rows.append((entity_type, entity_id, action, psycopg2.extras.Json(payload), created))
    sql = """
      INSERT INTO online_learning.logs
      (entity_type, entity_id, action, payload, created_at)
      VALUES %s
    """
    execute_values(cur, sql, rows)
    return

def main():
    cfg = parse_args()
    if cfg.seed is not None:
        random.seed(cfg.seed)
        Faker.seed(cfg.seed)

    conn = connect(cfg)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            print("Inserting students...")
            student_ids = bulk_insert_students(cur, cfg.n_students)
            print("Inserted students:", len(student_ids))

            print("Inserting instructors...")
            instr_ids = bulk_insert_instructors(cur, cfg.n_instructors)
            print("Inserted instructors:", len(instr_ids))

            print("Inserting courses...")
            course_ids = bulk_insert_courses(cur, cfg.n_courses)
            print("Inserted courses:", len(course_ids))

            print("Assigning instructors to courses...")
            bulk_insert_course_instructors(cur, course_ids, instr_ids)

            print("Creating lessons...")
            bulk_insert_lessons(cur, course_ids, cfg.max_lessons_per_course)

            print("Creating quizzes...")
            bulk_insert_quizzes(cur, course_ids)

            print("Creating enrollments, payments, reviews, certificates...")
            bulk_insert_enrollments_payments_reviews_certificates(cur, student_ids, course_ids)

            print("Inserting logs...")
            insert_logs(cur, n=min(500, cfg.n_students//2))

        conn.commit()
        print("All done. Committed to DB.")
    except Exception as e:
        conn.rollback()
        print("Error occurred, rolled back. Exception:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()



