"""Central configuration. Every value can be overridden with an environment variable."""
import os
from dataclasses import dataclass


def _env(name: str, default, cast=str):
    return cast(os.getenv(name, default))


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str = "localhost:9092"
    raw_topic: str = "career.events.raw"
    dlq_topic: str = "career.events.dlq"
    consumer_group: str = "career-db-writer"

    # topic layout (used by scripts/create_topics.py)
    raw_partitions: int = 6
    dlq_partitions: int = 3
    replication_factor: int = 1          # use 3 (+ min.insync.replicas=2) in production
    raw_retention_ms: int = 7 * 24 * 3600 * 1000
    dlq_retention_ms: int = 30 * 24 * 3600 * 1000

    # retry policy for transient DB failures
    max_retries: int = 5                 # retries AFTER the first attempt
    backoff_base_s: float = 0.5          # delay = min(max, base * 2**retry), with jitter
    backoff_max_s: float = 30.0
    backoff_jitter: bool = True

    # consumer
    max_poll_interval_ms: int = 300_000
    dedup_cache_size: int = 100_000      # recent event_ids remembered in-process

    def worst_case_backoff_s(self) -> float:
        return sum(min(self.backoff_max_s, self.backoff_base_s * 2 ** i)
                   for i in range(self.max_retries))


def load_config() -> Config:
    d = Config()
    return Config(
        bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
        raw_topic=_env("RAW_TOPIC", d.raw_topic),
        dlq_topic=_env("DLQ_TOPIC", d.dlq_topic),
        consumer_group=_env("CONSUMER_GROUP", d.consumer_group),
        raw_partitions=_env("RAW_PARTITIONS", d.raw_partitions, int),
        dlq_partitions=_env("DLQ_PARTITIONS", d.dlq_partitions, int),
        replication_factor=_env("REPLICATION_FACTOR", d.replication_factor, int),
        max_retries=_env("MAX_RETRIES", d.max_retries, int),
        backoff_base_s=_env("BACKOFF_BASE_S", d.backoff_base_s, float),
        backoff_max_s=_env("BACKOFF_MAX_S", d.backoff_max_s, float),
    )
