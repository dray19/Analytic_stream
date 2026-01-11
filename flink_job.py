import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types


TOPIC_IN = "market-events"
TOPIC_OUT = "market-events-agg"

TOPIC_DAILYAVG = "market-events-dailyavg"   # ✅ NEW OUTPUT TOPIC

BOOTSTRAP = "localhost:9092"
GROUP_ID = "flink-market-stream"


def parse_event(raw: str):
    try:
        obj = json.loads(raw)
        return obj
    except Exception:
        return None


def expand_pnodes(event):
    out = []
    if event is None:
        return out

    ts = event.get("timestamp")
    day = event.get("day")
    price_type = event.get("priceType")

    for p in event.get("pnodes", []):
        name = p.get("pnodeName", "UNKNOWN")
        for h in p.get("hours", []):
            row = {
                "day": day,
                "priceType": price_type,
                "timestamp": ts,
                "hour": h.get("hour"),
                "lmp": h.get("lmp"),
                "mcc": h.get("mcc"),
                "mlc": h.get("mlc")
            }
            out.append((name, row))
    return out


def to_json_record(tup):
    key, payload = tup
    payload["pnodeName"] = key
    return json.dumps(payload)


# -----------------------------
# ✅ Daily Avg Functions
# -----------------------------
def parse_flattened_json(raw: str):
    try:
        return json.loads(raw)
    except Exception:
        return None


def to_day_pnode_kv(event):
    """
    event dict -> (key, (sum_lmp, count))
    key = "day|pnode"
    """
    if event is None:
        return None

    day = event.get("day")
    pnode = event.get("pnodeName")
    lmp = event.get("lmp")

    if day is None or pnode is None or lmp is None:
        return None

    try:
        lmp_val = float(lmp)
    except Exception:
        return None

    key = f"{day}|{pnode}"
    return (key, (lmp_val, 1))


def reduce_sum_count(a, b):
    # a,b = (sum,count)
    return (a[0] + b[0], a[1] + b[1])


def to_dailyavg_json(tup):
    """
    (key, (sum,count)) -> json string
    """
    key, sc = tup
    sum_lmp, count = sc

    day, pnode = key.split("|", 1)
    avg_lmp = sum_lmp / count if count else None

    out = {
        "day": day,
        "pnodeName": pnode,
        "avg_lmp": avg_lmp,
        "count": count
    }
    return json.dumps(out)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_topics(TOPIC_IN) \
        .set_group_id(GROUP_ID) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Sink 1: flattened output
    sink_flat = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_OUT)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    # Sink 2: daily avg output
    sink_avg = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_DAILYAVG)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource",
        type_info=Types.STRING()
    )

    # -----------------------------
    # Flatten
    # -----------------------------
    processed = (
        stream
        .map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .flat_map(
            lambda e: expand_pnodes(e),
            output_type=Types.TUPLE([Types.STRING(), Types.PICKLED_BYTE_ARRAY()])
        )
        .map(to_json_record, output_type=Types.STRING())
    )

    processed.print()  # flattened debug
    processed.sink_to(sink_flat)

    # -----------------------------
    # ✅ Daily average branch
    # -----------------------------
    daily_avg = (
        processed
        .map(parse_flattened_json, output_type=Types.PICKLED_BYTE_ARRAY())
        .map(
            to_day_pnode_kv,
            output_type=Types.TUPLE([Types.STRING(), Types.TUPLE([Types.FLOAT(), Types.INT()])])
        )
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0])
        .reduce(lambda a, b: (a[0], reduce_sum_count(a[1], b[1])))
        .map(to_dailyavg_json, output_type=Types.STRING())
    )

    daily_avg.print()   # daily avg debug
    daily_avg.sink_to(sink_avg)

    env.execute("Market Events Flink Processor (flatten + daily avg)")


if __name__ == "__main__":
    main()