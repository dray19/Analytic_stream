import pandas as pd
import json
from kafka import KafkaProducer
import time
from common.schema import MarketEvent
from pydantic import ValidationError

TOPIC = "market-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# ----------------------------
# Load CSV
# ----------------------------
df = pd.read_csv("data/input.csv")

# ----------------------------
# Group by minute snapshot
# ----------------------------
for (day, price_type, timestamp), g_min in df.groupby(
    ["day", "priceType", "timestamp"]):
    pnodes = []

    for pnode, g_pnode in g_min.groupby("pnodeName"):
        row = g_pnode.iloc[0]

        pnodes.append({
            "pnodeName": pnode,
            "hours": [{
                "hour": int(row["hour"]),
                "lmp": float(row["lmp"]),
                "mcc": float(row["mcc"]),
                "mlc": float(row["mlc"])
            }]
        })

    payload = {
        "day": day,
        "priceType": price_type,
        "timestamp": timestamp,
        "pnodes": pnodes
    }

    # producer.send(TOPIC, key=timestamp, value=payload)
    # print("Sent 5 minute event:", timestamp)

    try:
        event = MarketEvent(**payload)

        producer.send(
            TOPIC,
            key=event.timestamp.isoformat(),
            value=payload
        )

        print("Sent 5 minute event:", event.timestamp)

    except ValidationError as e:
        print("Invalid event, not sent to Kafka")
        print(e)

    time.sleep(4)

producer.flush()
producer.close()