\connect lms_db;

DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS enrollments CASCADE;
DROP TABLE IF EXISTS quizzes CASCADE;
DROP TABLE IF EXISTS lessons CASCADE;
DROP TABLE IF EXISTS courses CASCADE;
DROP TABLE IF EXISTS instructors CASCADE;
DROP TABLE IF EXISTS students CASCADE;
DROP TABLE IF EXISTS certificates CASCADE;
DROP TABLE IF EXISTS logs CASCADE;

CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE instructors (
    instructor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    bio TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
    
);

CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    instructor_id INT NOT NULL,
    title VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    price NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (price >=0),
    level VARCHAR(50) DEFAULT 'Beginner',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_instructor
        FOREIGN KEY (instructor_id)
        REFERENCES instructors(instructor_id)
        ON DELETE RESTRICT
);

CREATE TABLE lessons (
    lesson_id SERIAL PRIMARY KEY,
    course_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    lesson_order INT NOT NULL,
    content_url TEXT,
    UNIQUE (course_id, lesson_order), 
    CONSTRAINT fk_course
        FOREIGN KEY (course_id)
        REFERENCES courses(course_id)
        ON DELETE CASCADE 
);

CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    enrolled_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'enrolled',
    UNIQUE (student_id, course_id), 
    CONSTRAINT fk_student
        FOREIGN KEY (student_id)
        REFERENCES students(student_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_course_enrollment
        FOREIGN KEY (course_id)
        REFERENCES courses(course_id)
        ON DELETE CASCADE
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    enrollment_id INT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    payment_date TIMESTAMPTZ DEFAULT NOW(),
    payment_method VARCHAR(50),
    status VARCHAR(50) DEFAULT 'Completed', 
    CONSTRAINT fk_enrollment
        FOREIGN KEY (enrollment_id)
        REFERENCES enrollments(enrollment_id)
        ON DELETE RESTRICT 
);

CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    rating SMALLINT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    review_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (student_id, course_id), 
    CONSTRAINT fk_student_review
        FOREIGN KEY (student_id)
        REFERENCES students(student_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_course_review
        FOREIGN KEY (course_id)
        REFERENCES courses(course_id)
        ON DELETE CASCADE
);

CREATE TABLE quizzes (
    quiz_id SERIAL PRIMARY KEY,
    course_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    max_score INT NOT NULL,
    CONSTRAINT fk_course_quiz
        FOREIGN KEY (course_id)
        REFERENCES courses(course_id)
        ON DELETE CASCADE
);

CREATE TABLE certificates (
    certificate_id SERIAL PRIMARY KEY,
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    issue_date DATE DEFAULT CURRENT_DATE,
    unique_hash VARCHAR(255) UNIQUE NOT NULL,
    CONSTRAINT fk_student_cert
        FOREIGN KEY (student_id)
        REFERENCES students(student_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_course_cert
        FOREIGN KEY (course_id)
        REFERENCES courses(course_id)
        ON DELETE CASCADE
);

CREATE TABLE logs (
    log_id BIGSERIAL PRIMARY KEY,
    student_id INT, -- Can be NULL for system logs
    log_type VARCHAR(50) NOT NULL, -- e.g., 'Viewed Lesson', 'Quiz Taken', 'Login'
    description TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_student_log
        FOREIGN KEY (student_id)
        REFERENCES students(student_id)
        ON DELETE SET NULL
);

CREATE INDEX idx_enrollments_student ON enrollments(student_id);
CREATE INDEX idx_enrollments_course  ON enrollments(course_id);

