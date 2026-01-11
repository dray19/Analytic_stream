import json
import csv
import os
import time
from kafka import KafkaConsumer
from common.metrics import ConsumerMetrics
from common.schema import  FlattenedMarketEvent
from pydantic import ValidationError

TOPIC = "market-events-agg"
OUT_DIR = "dashboard_data"

os.makedirs(OUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="dashboard-consumer-agg"   # new group id
)

metrics = ConsumerMetrics()

print("Dashboard CSV consumer (flattened) started...")

FIELDS = [
    "day",
    "priceType",
    "timestamp",
    "hour",
    "pnode",
    "lmp",
    "mcc",
    "mlc"
]

for msg in consumer:
    start_time = time.time()

    try:
        payload = msg.value
       
        event = FlattenedMarketEvent(**payload)
        clean = event.model_dump()
        pnode_name = clean.get("pnodeName") or clean.get("pnode") or "UNKNOWN"
        day = clean.get("day")
        price_type = clean.get("priceType")
        base_ts = clean.get("timestamp")
        hour = clean.get("hour")

        row = {
            "day": day,
            "priceType": price_type,
            "timestamp": base_ts,
            "hour": hour,
            "pnode": pnode_name,
            "lmp": payload.get("lmp"),
            "mcc": payload.get("mcc"),
            "mlc": payload.get("mlc")
        }

        csv_path = os.path.join(OUT_DIR, f"{pnode_name}.csv")
        file_exists = os.path.isfile(csv_path)

        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print(f"Updated {pnode_name}.csv | hour={hour} lmp={row['lmp']}")

        metrics.record_success(time.time() - start_time)

    except ValidationError as e:
        metrics.record_error()
        print("Schema validation failed:", e)
        continue

    except Exception as e:
        metrics.record_error()
        print("Error processing message:", e)

    stats = metrics.snapshot()
    print(
        f"Metrics | "
        f"Msgs/sec={stats['messages_per_sec']} | "
        f"Avg latency={stats['avg_latency_ms']} ms | "
        f"Errors={stats['errors']}"
    )
