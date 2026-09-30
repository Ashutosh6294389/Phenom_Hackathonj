"""Quick health view: consumer lag per partition + DLQ depth (+ optional DLQ tail).

    python scripts/monitor.py            # one-shot
    python scripts/monitor.py --watch    # refresh every 2s
    python scripts/monitor.py --tail 5   # print the last 5 DLQ records
"""
import argparse, json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka import Consumer, TopicPartition
from career_queue.config import load_config


def snapshot(cfg):
    c = Consumer({"bootstrap.servers": cfg.bootstrap_servers, "group.id": cfg.consumer_group,
                  "enable.auto.commit": False})       # never subscribes -> does not disturb the group
    md = c.list_topics(cfg.raw_topic, timeout=10).topics[cfg.raw_topic]
    tps = [TopicPartition(cfg.raw_topic, p) for p in md.partitions]
    committed = {tp.partition: tp.offset for tp in c.committed(tps, timeout=10)}
    rows, total_lag = [], 0
    for p in sorted(md.partitions):
        lo, hi = c.get_watermark_offsets(TopicPartition(cfg.raw_topic, p), timeout=10)
        cur = committed.get(p, -1)
        cur = lo if cur < 0 else cur                   # no commit yet -> whole partition is lag
        rows.append((p, hi, cur, hi - cur)); total_lag += hi - cur

    dlq_md = c.list_topics(cfg.dlq_topic, timeout=10).topics[cfg.dlq_topic]
    dlq_depth = 0
    for p in dlq_md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(cfg.dlq_topic, p), timeout=10)
        dlq_depth += hi - lo
    c.close()
    return rows, total_lag, dlq_depth


def tail_dlq(cfg, n):
    c = Consumer({"bootstrap.servers": cfg.bootstrap_servers, "group.id": "dlq-tail",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([cfg.dlq_topic]); recs, end = [], time.time() + 8
    while time.time() < end:
        m = c.poll(1.0)
        if m and not m.error():
            recs.append(json.loads(m.value()))
    c.close()
    for r in recs[-n:]:
        print(json.dumps({k: r[k] for k in ("source", "retry_count", "error", "failed_at")}, indent=2))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--watch", action="store_true"); ap.add_argument("--tail", type=int)
    a = ap.parse_args(); cfg = load_config()
    while True:
        rows, lag, dlq = snapshot(cfg)
        print(f"\ngroup={cfg.consumer_group}  topic={cfg.raw_topic}")
        print("partition  end_offset  committed  lag")
        for p, hi, cur, l in rows:
            print(f"{p:>9}  {hi:>10}  {cur:>9}  {l:>3}")
        print(f"TOTAL LAG {lag}    DLQ depth ({cfg.dlq_topic}) = {dlq}")
        if not a.watch: break
        time.sleep(2)
    if a.tail:
        tail_dlq(cfg, a.tail)


if __name__ == "__main__":
    main()
