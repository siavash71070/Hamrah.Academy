from kafka import KafkaConsumer
from clickhouse_driver import Client
import json
from datetime import datetime

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "lms_server.public.students"

# ClickHouse settings
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "lms_analytics"
CLICKHOUSE_USER = "lms_user"
CLICKHOUSE_PASSWORD = "lms_password"

def parse_created_at(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1_000_000)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return datetime.utcnow()
    return datetime.utcnow()

def main():
    # Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # ClickHouse client
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    print(f"Starting consumer for topic: {KAFKA_TOPIC}")

    for message in consumer:
        payload = message.value
        op = payload.get("op")
        after = payload.get("after")

        # Only process inserts and updates with data
        if after is None:
            continue

        try:
            # Use student_id instead of id
            row = (
                after.get("student_id"),
                after.get("first_name", ""),
                after.get("last_name", ""),
                after.get("email", ""),
                after.get("phone", ""),
                parse_created_at(after.get("created_at")),
                after.get("is_active", True),
                op,
                payload.get("ts_ms", 0)
            )

            client.execute(
                """
                INSERT INTO students_merge
                (student_id, first_name, last_name, email, phone, created_at, is_active, __op, __ts_ms)
                VALUES
                """,
                [row],
            )

            print(f"✅ Inserted to ClickHouse: student_id={row[0]}, email={row[3]}")
        except Exception as e:
            print(f"❌ Error inserting message: {e}")
            # Don't print full payload to avoid clutter

if __name__ == "__main__":
    main()
