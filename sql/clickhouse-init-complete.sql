-- ایجاد دیتابیس اگر وجود ندارد
CREATE DATABASE IF NOT EXISTS lms_analytics;

USE lms_analytics;

-- =============================================
-- Students Tables 
-- =============================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS students_ingestion_mv;
DROP TABLE IF EXISTS students_merge;
DROP TABLE IF EXISTS students_kafka;

-- Students Kafka Table
CREATE TABLE students_kafka (
    message String
) ENGINE = Kafka(
    'kafka:9092',
    'lms.public.students',
    'clickhouse_students_consumer',
    'JSONAsString'
);

-- Students Merge Table
CREATE TABLE students_merge (
    student_id Int32,
    first_name String,
    last_name String,
    email String,
    phone Nullable(String),
    created_at_raw Int64,
    is_active Bool,
    __op String,
    __ts_ms Int64
) ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (student_id, __ts_ms);

-- Students Materialized View
CREATE MATERIALIZED VIEW students_ingestion_mv TO students_merge
AS SELECT
    JSONExtractInt(message, 'after', 'student_id') AS student_id,
    JSONExtractString(message, 'after', 'first_name') AS first_name,
    JSONExtractString(message, 'after', 'last_name') AS last_name,
    JSONExtractString(message, 'after', 'email') AS email,
    JSONExtractString(message, 'after', 'phone') AS phone,
    JSONExtractInt(message, 'after', 'created_at') AS created_at_raw,
    JSONExtractBool(message, 'after', 'is_active') AS is_active,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM students_kafka;

-- =============================================
-- Enrollments Tables  
-- =============================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS enrollments_ingestion_mv;
DROP TABLE IF EXISTS enrollments_merge;
DROP TABLE IF EXISTS enrollments_kafka;

-- Enrollments Kafka Table
CREATE TABLE enrollments_kafka (
    message String
) ENGINE = Kafka(
    'kafka:9092',
    'lms.public.enrollments',
    'clickhouse_enrollments_consumer',
    'JSONAsString'
);

-- Enrollments Merge Table
CREATE TABLE enrollments_merge (
    enrollment_id Int32,
    student_id Int32,
    course_id Int32,
    enrolled_at_raw Int64,
    progress Nullable(Int32),
    completed_at_raw Nullable(Int64),
    status String,
    __op String,
    __ts_ms Int64
) ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (enrollment_id, __ts_ms);

-- Enrollments Materialized View
CREATE MATERIALIZED VIEW enrollments_ingestion_mv TO enrollments_merge
AS SELECT
    JSONExtractInt(message, 'after', 'enrollment_id') AS enrollment_id,
    JSONExtractInt(message, 'after', 'student_id') AS student_id,
    JSONExtractInt(message, 'after', 'course_id') AS course_id,
    JSONExtractInt(message, 'after', 'enrolled_at') AS enrolled_at_raw,
    JSONExtractInt(message, 'after', 'progress') AS progress,
    JSONExtractInt(message, 'after', 'completed_at') AS completed_at_raw,
    JSONExtractString(message, 'after', 'status') AS status,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM enrollments_kafka;

-- =============================================
-- Payments Tables
-- =============================================

-- Drop existing tables if they exist  
DROP TABLE IF EXISTS payments_ingestion_mv;
DROP TABLE IF EXISTS payments_merge;
DROP TABLE IF EXISTS payments_kafka;

-- Payments Kafka Table
CREATE TABLE payments_kafka (
    message String
) ENGINE = Kafka(
    'kafka:9092',
    'lms.public.payments',
    'clickhouse_payments_consumer',
    'JSONAsString'
);

-- Payments Merge Table
CREATE TABLE payments_merge (
    payment_id Int32,
    enrollment_id Int32,
    amount Decimal64(2),
    payment_date Int64, 
    payment_method String,
    status String,
    __op String,
    __ts_ms Int64
) ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (payment_id, __ts_ms);

-- Create a corrected materialized view
CREATE MATERIALIZED VIEW payments_ingestion_mv TO payments_merge
AS SELECT
    JSONExtractInt(message, 'after', 'payment_id') AS payment_id,
    JSONExtractInt(message, 'after', 'enrollment_id') AS enrollment_id,
    -- Extract amount as string first, then convert to decimal
    toDecimal64(JSONExtractString(message, 'after', 'amount'), 2) AS amount,
    JSONExtractInt(message, 'after', 'payment_date') AS payment_date,
    JSONExtractString(message, 'after', 'payment_method') AS payment_method,
    JSONExtractString(message, 'after', 'status') AS status,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM payments_kafka
WHERE NOT empty(JSONExtractRaw(message, 'after'));



-- =============================================
-- Create User and Permissions
-- =============================================

CREATE USER IF NOT EXISTS lms_user IDENTIFIED WITH plaintext_password BY 'lms_password';
GRANT ALL ON lms_analytics.* TO lms_user;

SELECT 'ClickHouse CDC Schema created successfully!' as status;
