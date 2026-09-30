"""Create career.events.raw and career.events.dlq (idempotent)."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka.admin import AdminClient, NewTopic
from career_queue.config import load_config


def main():
    cfg = load_config()
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    topics = [
        NewTopic(cfg.raw_topic, cfg.raw_partitions, cfg.replication_factor, config={
            "retention.ms": str(cfg.raw_retention_ms),
            "min.insync.replicas": "1",          # production: RF=3, min.insync.replicas=2
            "cleanup.policy": "delete",
        }),
        NewTopic(cfg.dlq_topic, cfg.dlq_partitions, cfg.replication_factor, config={
            "retention.ms": str(cfg.dlq_retention_ms),
            "min.insync.replicas": "1",
            "cleanup.policy": "delete",
        }),
    ]
    for name, fut in admin.create_topics(topics).items():
        try:
            fut.result()
            print(f"created  {name}")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"exists   {name}")
            else:
                print(f"FAILED   {name}: {e}")
                sys.exit(1)


if __name__ == "__main__":
    main()
