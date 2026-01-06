import json
import csv
import os
import time
from kafka import KafkaConsumer
from common.schema import MarketEvent
from common.metrics import ConsumerMetrics
from pydantic import ValidationError

TOPIC = "market-events"
OUT_DIR = "dashboard_data"

os.makedirs(OUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="dashboard-consumer"
)

metrics = ConsumerMetrics()

print("Dashboard CSV consumer started...")

### Data would be sent to database but for the purposes of this example it is sent to a csv file per pnode

for msg in consumer:
    start_time = time.time()
    try:
        event = MarketEvent(**msg.value)
    except ValidationError as e:
        metrics.record_error()
        print("❌ Invalid message received — sending to DLQ")
        continue
    payload = msg.value

    day = payload["day"]
    price_type = payload["priceType"]
    base_ts = payload["timestamp"]

    for pnode in payload["pnodes"]:
        pnode_name = pnode["pnodeName"]
        csv_path = os.path.join(OUT_DIR, f"{pnode_name}.csv")
        file_exists = os.path.isfile(csv_path)

        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "day",
                    "priceType",
                    "timestamp",
                    "hour",
                    "pnode",
                    "lmp",
                    "mcc",
                    "mlc"
                ]
            )

            if not file_exists:
                writer.writeheader()

            for h in pnode["hours"]:
                writer.writerow({
                    "day": day,
                    "priceType": price_type,
                    "timestamp": base_ts,
                    "hour": h["hour"],
                    "pnode": pnode_name,
                    "lmp": h["lmp"],
                    "mcc": h["mcc"],
                    "mlc": h["mlc"]
                })

        print(f"Updated {pnode_name}.csv")
    # ----------------------------
    # Metrics update + snapshot
    # ----------------------------
    metrics.record_success(time.time() - start_time)
    stats = metrics.snapshot()

    print(
        f"Metrics | "
        f"Msgs/sec={stats['messages_per_sec']} | "
        f"Avg latency={stats['avg_latency_ms']} ms | "
        f"Errors={stats['errors']}"
    )
