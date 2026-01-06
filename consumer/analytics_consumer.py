import json
import csv
from kafka import KafkaConsumer
from collections import defaultdict

consumer = KafkaConsumer(
    "market-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="analytics-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

stats = defaultdict(lambda: {"count": 0, "price_sum": 0.0})

for msg in consumer:
    event = msg.value
    region = event["region"]
    price = float(event["price"])

    stats[region]["count"] += 1
    stats[region]["price_sum"] += price

    avg_price = stats[region]["price_sum"] / stats[region]["count"]

    print(f"{region} | avg_price={avg_price:.2f}")

    with open("output/metrics.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["region", "count", "avg_price"])
        for r, s in stats.items():
            writer.writerow([r, s["count"], s["price_sum"] / s["count"]])