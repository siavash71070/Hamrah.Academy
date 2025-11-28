from kafka import KafkaConsumer
from clickhouse_driver import Client
import json
from datetime import datetime

# تنظیمات Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"   # مطابق docker-compose
KAFKA_TOPIC = "lms.public.students"

# تنظیمات ClickHouse
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "lms_analytics"
CLICKHOUSE_USER = "lms_user"
CLICKHOUSE_PASSWORD = "lms_password"


def parse_created_at(value):
    # Debezium برای created_at عدد بزرگ (microseconds since epoch) می‌فرستد
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1_000_000)
    if isinstance(value, str):
        # اگر بعداً فرمت string شد، این هم جواب می‌دهد
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

    print("Starting consumer...")

    # حلقه خواندن پیام‌ها
    for message in consumer:
        payload = message.value

        # ساختار Debezium: before/after/op/...
        op = payload.get("op")
        after = payload.get("after")

        # فقط insert (op = c) را ذخیره کنیم
        if op != "c" or after is None:
            continue

        try:
            row = (
                after["id"],
                after.get("first_name", ""),
                after.get("last_name", ""),
                after.get("email", ""),
                after.get("phone", ""),
                parse_created_at(after.get("created_at")),
                op,
            )

            client.execute(
                """
                INSERT INTO students_events
                (id, first_name, last_name, email, phone, created_at, op)
                VALUES
                """,
                [row],
            )

            print(f"Inserted to ClickHouse: id={row[0]}, email={row[3]}")
        except Exception as e:
            print("Error inserting message:", e)
            print("Payload:", payload)


if __name__ == "__main__":
    main()
