-- clickhouse_cdc_schema.sql
CREATE DATABASE IF NOT EXISTS lms_analytics;

USE lms_analytics;

-- Students table with Debezium format
CREATE TABLE IF NOT EXISTS students_kafka (
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'lms_server.public.students',
    kafka_group_name = 'clickhouse_students_group',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS students_merge (
    student_id Int32,
    first_name String,
    last_name String,
    email String,
    phone Nullable(String),
    created_at_raw Int64,
    is_active Bool,
    __op String,
    __ts_ms Int64
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (student_id, __ts_ms);

CREATE MATERIALIZED VIEW IF NOT EXISTS students_ingestion_mv 
TO students_merge AS
SELECT
    JSONExtractInt(message, 'after', 'student_id') AS student_id,
    JSONExtractString(message, 'after', 'first_name') AS first_name,
    JSONExtractString(message, 'after', 'last_name') AS last_name,
    JSONExtractString(message, 'after', 'email') AS email,
    JSONExtractString(message, 'after', 'phone') AS phone,
    JSONExtractInt(message, 'after', 'created_at') AS created_at_raw,
    JSONExtractBool(message, 'after', 'is_active') AS is_active,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM students_kafka
WHERE NOT empty(JSONExtractRaw(message, 'after'));

-- Enrollments tables
CREATE TABLE IF NOT EXISTS enrollments_kafka (
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'lms_server.public.enrollments',
    kafka_group_name = 'clickhouse_enrollments_group',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS enrollments_merge (
    enrollment_id Int32,
    student_id Int32,
    course_id Int32,
    enrolled_at_raw Int64,
    status String,
    __op String,
    __ts_ms Int64
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (enrollment_id, __ts_ms);

CREATE MATERIALIZED VIEW IF NOT EXISTS enrollments_ingestion_mv 
TO enrollments_merge AS
SELECT
    JSONExtractInt(message, 'after', 'enrollment_id') AS enrollment_id,
    JSONExtractInt(message, 'after', 'student_id') AS student_id,
    JSONExtractInt(message, 'after', 'course_id') AS course_id,
    JSONExtractInt(message, 'after', 'enrolled_at') AS enrolled_at_raw,
    JSONExtractString(message, 'after', 'status') AS status,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM enrollments_kafka
WHERE NOT empty(JSONExtractRaw(message, 'after'));

-- Payments tables (CORRECTED)
CREATE TABLE IF NOT EXISTS payments_kafka (
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'lms_server.public.payments',
    kafka_group_name = 'clickhouse_payments_group',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS payments_merge (
    payment_id Int32,
    enrollment_id Int32,
    amount Decimal64(2),
    payment_date_raw Int64,
    payment_method String,
    status String,
    __op String,
    __ts_ms Int64
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (payment_id, __ts_ms);

CREATE MATERIALIZED VIEW IF NOT EXISTS payments_ingestion_mv 
TO payments_merge AS
SELECT
    JSONExtractInt(message, 'after', 'payment_id') AS payment_id,
    JSONExtractInt(message, 'after', 'enrollment_id') AS enrollment_id,
    toDecimal64(JSONExtractFloat(message, 'after', 'amount'), 2) AS amount,
    JSONExtractInt(message, 'after', 'payment_date') AS payment_date_raw,
    JSONExtractString(message, 'after', 'payment_method') AS payment_method,
    JSONExtractString(message, 'after', 'status') AS status,
    JSONExtractString(message, 'op') AS __op,
    JSONExtractInt(message, 'ts_ms') AS __ts_ms
FROM payments_kafka
WHERE NOT empty(JSONExtractRaw(message, 'after'));

-- Create user and permissions
CREATE USER IF NOT EXISTS lms_user IDENTIFIED WITH plaintext_password BY 'lms_password';
GRANT ALL ON lms_analytics.* TO lms_user;

SELECT 'ClickHouse CDC Schema created successfully!' as status;
