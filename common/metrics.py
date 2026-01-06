import time
from collections import deque


class ConsumerMetrics:
    """
    Lightweight in-memory metrics collector for Kafka consumers.

    Tracks:
    - processed messages
    - error count
    - processing latency
    - throughput (messages/sec)
    """

    def __init__(self, window: int = 300):
        """
        Args:
            window (int): Rolling window size for latency calculation
        """
        self.processed = 0
        self.errors = 0
        self.latencies = deque(maxlen=window)
        self.start_time = time.time()

    def record_success(self, latency_seconds: float):
        """Record a successfully processed message."""
        self.processed += 1
        self.latencies.append(latency_seconds)

    def record_error(self):
        """Record a failed message."""
        self.errors += 1

    def snapshot(self) -> dict:
        """
        Return a snapshot of current metrics.

        Returns:
            dict: metrics summary
        """
        elapsed = max(time.time() - self.start_time, 1)

        avg_latency_ms = (
            (sum(self.latencies) / len(self.latencies)) * 1000
            if self.latencies else 0.0
        )

        return {
            "messages_per_sec": round(self.processed / elapsed, 2),
            "avg_latency_ms": round(avg_latency_ms, 2),
            "processed": self.processed,
            "errors": self.errors
        }