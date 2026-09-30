import json
import pytest

from career_queue.config import Config
from career_queue.consumer import DLQPublishError, EventConsumer
from career_queue.db import InMemoryDatabase
from career_queue.events import InvalidEventError, partition_key
from career_queue.producer import EventProducer
from career_queue.sample_events import make_event
from fakes import FakeConsumer, FakeDLQProducer, FakeKafkaProducer, FakeMsg


def build(max_retries=3, dlq_fail=False, db=None):
    log, sleeps = [], []
    cfg = Config(max_retries=max_retries, backoff_base_s=0.5, backoff_max_s=30, backoff_jitter=False)
    db = db or InMemoryDatabase()
    orig = db.write_event

    def traced(ev):
        try:
            orig(ev)
            log.append(("db_ok", ev["event_header"]["event_id"]))
        except Exception:
            log.append(("db_fail", ev["event_header"]["event_id"]))
            raise
    db.write_event = traced
    c, d = FakeConsumer(log), FakeDLQProducer(log, fail=dlq_fail)
    ec = EventConsumer(cfg, db, consumer=c, dlq_producer=d, sleep=sleeps.append)
    return ec, db, c, d, log, sleeps


def test_happy_path_commits_only_after_db_ack():
    ec, db, c, d, log, _ = build()
    ev = make_event()
    ec.process_message(FakeMsg(ev, offset=7))
    assert [x[0] for x in log] == ["db_ok", "commit"]          # write BEFORE commit
    assert c.commits == [(0, 8)]                               # committed offset = 7 + 1
    assert ev["event_header"]["event_id"] in db.rows


def test_transient_failures_retry_with_exponential_backoff_then_succeed():
    db = InMemoryDatabase()
    ev = make_event()
    db.fail_first[ev["event_header"]["event_id"]] = 3
    ec, db, c, d, log, sleeps = build(max_retries=5, db=db)
    ec.process_message(FakeMsg(ev, offset=1))
    assert sleeps == [0.5, 1.0, 2.0]                           # exponential
    assert [x[0] for x in log] == ["db_fail"] * 3 + ["db_ok", "commit"]
    assert d.records == []                                     # never hit the DLQ
    assert ec.metrics.get("retries") == 3


def test_no_commit_while_db_is_failing():
    ec, db, c, d, log, _ = build(max_retries=3)
    db.outage = True
    ev = make_event()
    ec.process_message(FakeMsg(ev, offset=4))
    kinds = [x[0] for x in log]
    assert kinds.index("dlq_publish") < kinds.index("commit")  # commit only after DLQ ack
    assert kinds.count("commit") == 1


def test_retries_exhausted_goes_to_dlq_with_full_context():
    ec, db, c, d, log, sleeps = build(max_retries=3)
    db.outage = True
    ev = make_event()
    ec.process_message(FakeMsg(ev, partition=2, offset=42))
    assert sleeps == [0.5, 1.0, 2.0]
    (topic, key, rec), = d.records
    assert topic == "career.events.dlq"
    assert rec["original_event"] == ev
    assert rec["retry_count"] == 3
    assert rec["source"] == {"topic": "career.events.raw", "partition": 2, "offset": 42}
    assert rec["error"]["type"] == "TransientDBError" and "unavailable" in rec["error"]["message"]
    assert c.commits == [(2, 43)]                              # moved past the poison event


def test_dlq_failure_does_not_commit():
    ec, db, c, d, log, _ = build(max_retries=1, dlq_fail=True)
    db.outage = True
    with pytest.raises(DLQPublishError):
        ec.process_message(FakeMsg(make_event(), offset=9))
    assert c.commits == []                                     # will be redelivered


def test_permanent_error_skips_retries():
    db = InMemoryDatabase()
    ev = make_event()
    db.permanent_fail.add(ev["event_header"]["event_id"])
    ec, db, c, d, log, sleeps = build(db=db)
    ec.process_message(FakeMsg(ev))
    assert sleeps == [] and d.records[0][2]["retry_count"] == 0
    assert d.records[0][2]["error"]["type"] == "PermanentDBError"


def test_invalid_json_and_invalid_schema_go_straight_to_dlq():
    ec, db, c, d, log, sleeps = build()
    ec.process_message(FakeMsg(b"{not json", offset=1))
    ec.process_message(FakeMsg({"event_header": {"event_type": "JOB_VIEWED"}}, offset=2))
    assert len(d.records) == 2 and sleeps == [] and db.write_calls == 0
    assert d.records[0][2]["original_event"] == "{not json"
    assert c.commits == [(0, 2), (0, 3)]


def test_redelivered_event_is_written_once():
    ec, db, c, d, log, _ = build()
    ev = make_event()
    ec.process_message(FakeMsg(ev, offset=1))
    ec.process_message(FakeMsg(ev, offset=2))                  # same event_id, new offset
    assert db.write_calls == 1 and len(db.rows) == 1
    assert ec.metrics.get("duplicates_skipped") == 1
    assert c.commits == [(0, 2), (0, 3)]                       # still advances


def test_config_rejects_retry_budget_longer_than_poll_interval():
    with pytest.raises(ValueError):
        EventConsumer(Config(max_retries=10, backoff_max_s=300, max_poll_interval_ms=60_000),
                      InMemoryDatabase(), consumer=object(), dlq_producer=object())


# ---- producer / key ----------------------------------------------------------
def test_key_is_site_and_user_so_one_user_stays_ordered():
    a = make_event(user_id="u1", event_type="APPLICATION_STARTED")
    b = make_event(user_id="u1", event_type="APPLICATION_SUBMITTED")
    c = make_event(user_id="u2")
    assert partition_key(a) == partition_key(b) == "tenant_acme_corp_8921:u1"
    assert partition_key(c) != partition_key(a)


def test_anonymous_event_falls_back_to_session_id():
    ev = make_event()
    del ev["candidate_metadata"]["user_id"]
    assert partition_key(ev) == "tenant_acme_corp_8921:sess_usr_1"


def test_producer_publishes_to_raw_topic_with_key_and_headers():
    fake = FakeKafkaProducer()
    p = EventProducer(Config(), producer=fake)
    ev = make_event(user_id="u9")
    key = p.publish(ev)
    topic, k, value, headers = fake.sent[0]
    assert topic == "career.events.raw" and k == key.encode() == b"tenant_acme_corp_8921:u9"
    assert json.loads(value) == ev
    assert ("event_id", ev["event_header"]["event_id"].encode()) in headers


def test_producer_rejects_invalid_event():
    with pytest.raises(InvalidEventError):
        EventProducer(Config(), producer=FakeKafkaProducer()).publish({"oops": 1})
