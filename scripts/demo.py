"""End-to-end demo against a real local Kafka (docker compose up -d first).

 generator -> producer -> career.events.raw -> consumer -> DB ack -> offset commit
 and the failure path: consumer -> DB failure -> retries (backoff) -> career.events.dlq

The "database" is InMemoryDatabase with injected failures, so you can watch both paths.
"""
import dataclasses, json, logging, pathlib, sys, threading, time, uuid
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka import Consumer
from career_queue.config import load_config
from career_queue.consumer import EventConsumer
from career_queue.db import InMemoryDatabase
from career_queue.producer import EventProducer
from career_queue.sample_events import make_event
import create_topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-9s %(message)s", datefmt="%H:%M:%S")
logging.getLogger("consumer").setLevel(logging.INFO)


def main():
    create_topics.main()
    cfg = dataclasses.replace(load_config(), max_retries=3, backoff_base_s=0.3,
                              consumer_group=f"career-db-writer-demo-{uuid.uuid4().hex[:6]}")

    e_ok      = make_event("JOB_VIEWED",          user_id="u1", job_id="job_1")
    e_flaky   = make_event("APPLICATION_STARTED", user_id="u1", job_id="job_1")   # fails twice, then OK
    e_dead    = make_event("APPLICATION_SUBMITTED", user_id="u2", job_id="job_2") # DB never recovers
    e_wish    = make_event("JOB_WISHLISTED",      user_id="u3", job_id="job_3")
    ids = lambda e: e["event_header"]["event_id"]

    db = InMemoryDatabase()
    db.fail_first[ids(e_flaky)] = 2
    db.always_fail.add(ids(e_dead))

    consumer = EventConsumer(cfg, db)
    t = threading.Thread(target=consumer.run, daemon=True)
    t.start()
    time.sleep(4)                                   # let the consumer join the group

    print("\n=== PRODUCING 4 EVENTS ===")
    producer = EventProducer(cfg)
    for e in (e_ok, e_flaky, e_dead, e_wish):
        producer.publish(e)
    producer.flush()

    want = {ids(e_ok), ids(e_flaky), ids(e_wish)}
    deadline = time.time() + 60
    while time.time() < deadline and not (want <= set(db.rows) and consumer.metrics.get("dead_lettered") >= 1):
        time.sleep(0.5)
    consumer.stop(); t.join(10)

    print("\n=== RESULT ===")
    print("stored in DB     :", sorted(db.rows) == sorted(want) and "3 events (ok, flaky-after-retries, wishlist)" or db.rows.keys())
    print("consumer metrics :", consumer.metrics.snapshot())

    print("\n=== DLQ CONTENTS ===")
    c = Consumer({"bootstrap.servers": cfg.bootstrap_servers, "group.id": f"dlq-reader-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([cfg.dlq_topic])
    end = time.time() + 10
    while time.time() < end:
        m = c.poll(1.0)
        if m and not m.error():
            rec = json.loads(m.value())
            if isinstance(rec["original_event"], dict) and rec["original_event"]["event_header"]["event_id"] == ids(e_dead):
                print(json.dumps(rec, indent=2)); break
    c.close()


if __name__ == "__main__":
    main()
