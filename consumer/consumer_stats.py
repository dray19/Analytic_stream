import json
import time
from collections import defaultdict
from kafka import KafkaConsumer

from common.schema import MarketEvent
from common.metrics import ConsumerMetrics
from pydantic import ValidationError

TOPIC = "market-events"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="stats-consumer"
)

metrics = ConsumerMetrics()
total_messages = 0
total_rows = 0

pnode_counts = defaultdict(int)
lmp_sum = defaultdict(float)
mcc_sum = defaultdict(float)
mlc_sum = defaultdict(float)

print("Stats consumer started...")

for msg in consumer:
    start_time = time.time()
    try:
        event = MarketEvent(**msg.value)
    except ValidationError as e:
        metrics.record_error()
        print("❌ Invalid message received — sending to DLQ")
        continue
    payload = msg.value
    total_messages += 1

    for pnode in payload["pnodes"]:
        name = pnode["pnodeName"]

        for h in pnode["hours"]:
            total_rows += 1
            pnode_counts[name] += 1
            lmp_sum[name] += h["lmp"]
            mcc_sum[name] += h["mcc"]
            mlc_sum[name] += h["mlc"]

    # print("\n--- STATS SNAPSHOT ---")
    # print(f"Kafka messages: {total_messages}")
    # print(f"Rows processed: {total_rows}")
    # ----------------------------
    # Metrics update
    # ----------------------------
    metrics.record_success(time.time() - start_time)

    stats = metrics.snapshot()

    print("\n--- STATS SNAPSHOT ---")
    print(f"Kafka messages: {total_messages}")
    print(f"Rows processed: {total_rows}")
    print(
        f"Msgs/sec={stats['messages_per_sec']} | "
        f"Avg latency={stats['avg_latency_ms']} ms | "
        f"Errors={stats['errors']}"
    )

    for p in pnode_counts:
        print(
            f"{p} | "
            f"count={pnode_counts[p]} | "
            f"avg_lmp={lmp_sum[p]/pnode_counts[p]:.2f} | "
            f"avg_mcc={mcc_sum[p]/pnode_counts[p]:.2f} | "
            f"avg_mlc={mlc_sum[p]/pnode_counts[p]:.2f}"
        )