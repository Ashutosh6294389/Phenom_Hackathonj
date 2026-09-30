"""Consumer: career.events.raw -> database, with manual offset commits, retries and a DLQ.

Per-message lifecycle (one partition is processed strictly in order):

    poll -> validate -> DB write --ok--> commit offset
                          | transient error
                          +--> sleep(backoff) -> retry ... (max_retries)
                                  | still failing / permanent error / invalid event
                                  +--> publish to career.events.dlq --ok--> commit offset
                                                                    --fail-> NO commit, raise

The offset is committed ONLY after the event is safely in Cassandra or safely in the DLQ.
A crash at any point means redelivery (at-least-once), so writes must be idempotent.
"""
import json
import logging
import random
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Callable, Optional

from .config import Config
from .db import DatabaseWriter, PermanentDBError
from .events import InvalidEventError, event_id, validate_event
from .metrics import Metrics

log = logging.getLogger("consumer")


class DLQPublishError(RuntimeError):
    """Could not park a failed event in the DLQ -> offset stays uncommitted."""


class EventConsumer:
    def __init__(self, cfg: Config, db: DatabaseWriter, consumer=None, dlq_producer=None,
                 metrics: Optional[Metrics] = None, sleep: Callable[[float], None] = time.sleep):
        if cfg.worst_case_backoff_s() * 1000 >= cfg.max_poll_interval_ms * 0.8:
            raise ValueError(
                f"worst-case retry time {cfg.worst_case_backoff_s():.0f}s is too close to "
                f"max.poll.interval.ms={cfg.max_poll_interval_ms}; the broker would evict this "
                "consumer mid-retry. Lower max_retries/backoff or raise max_poll_interval_ms.")
        self.cfg, self.db, self.sleep = cfg, db, sleep
        self.metrics = metrics or Metrics()
        if consumer is None:
            from confluent_kafka import Consumer
            consumer = Consumer({
                "bootstrap.servers": cfg.bootstrap_servers,
                "group.id": cfg.consumer_group,
                "enable.auto.commit": False,               # we commit manually
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": cfg.max_poll_interval_ms,
                "partition.assignment.strategy": "cooperative-sticky",
            })
        if dlq_producer is None:
            from confluent_kafka import Producer
            dlq_producer = Producer({"bootstrap.servers": cfg.bootstrap_servers,
                                     "enable.idempotence": True, "acks": "all",
                                     "client.id": "career-dlq-producer"})
        self._c, self._dlq = consumer, dlq_producer
        self._seen: "OrderedDict[str, None]" = OrderedDict()   # recent event_ids (best-effort dedup)
        self._running = False

    # ---------------------------------------------------------------- main loop
    def run(self):
        from confluent_kafka import KafkaError, KafkaException
        self._c.subscribe([self.cfg.raw_topic])
        self._running = True
        log.info("consumer started group=%s topic=%s", self.cfg.consumer_group, self.cfg.raw_topic)
        try:
            while self._running:
                msg = self._c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                self.process_message(msg)
        finally:
            self._c.close()
            log.info("consumer stopped metrics=%s", self.metrics.snapshot())

    def stop(self):
        self._running = False

    # ------------------------------------------------------ one message, end to end
    def process_message(self, msg) -> None:
        where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
        raw = msg.value()

        try:
            event = validate_event(json.loads(raw))
        except (ValueError, TypeError) as e:          # bad JSON or InvalidEventError (ValueError)
            log.warning("INVALID %s: %s", where, e)
            self._dead_letter(msg, e, retry_count=0)
            return

        eid = event_id(event)
        if eid in self._seen:
            log.info("DUPLICATE %s event_id=%s -> skip write, commit", where, eid)
            self.metrics.inc("duplicates_skipped")
            self._commit(msg)
            return

        retries = 0
        while True:
            try:
                self.db.write_event(event)
            except PermanentDBError as e:
                log.error("DB_PERMANENT_FAILURE %s event_id=%s: %s", where, eid, e)
                self._dead_letter(msg, e, retry_count=retries)
                return
            except Exception as e:                    # TransientDBError or anything unexpected
                if retries >= self.cfg.max_retries:
                    log.error("RETRIES_EXHAUSTED %s event_id=%s after %d retries: %s",
                              where, eid, retries, e)
                    self._dead_letter(msg, e, retry_count=retries)
                    return
                delay = self._backoff(retries)
                retries += 1
                self.metrics.inc("retries")
                log.warning("DB_FAILED %s event_id=%s (%s) -> retry %d/%d in %.2fs, offset NOT committed",
                            where, eid, e, retries, self.cfg.max_retries, delay)
                self.sleep(delay)
                continue

            log.info("DB_ACK %s event_id=%s", where, eid)
            self._remember(eid)
            self.metrics.inc("processed")
            self._commit(msg)
            return

    # ------------------------------------------------------------------ helpers
    def _backoff(self, retry_index: int) -> float:
        ceiling = min(self.cfg.backoff_max_s, self.cfg.backoff_base_s * 2 ** retry_index)
        return random.uniform(ceiling / 2, ceiling) if self.cfg.backoff_jitter else ceiling

    def _remember(self, eid: str):
        self._seen[eid] = None
        if len(self._seen) > self.cfg.dedup_cache_size:
            self._seen.popitem(last=False)

    def _commit(self, msg):
        """Synchronously commit offset+1 for this message's partition."""
        from confluent_kafka import KafkaException
        try:
            self._c.commit(message=msg, asynchronous=False)
            self.metrics.inc("offsets_committed")
            log.info("OFFSET_COMMITTED %s[%d]@%d", msg.topic(), msg.partition(), msg.offset() + 1)
        except KafkaException as e:
            # Not fatal: the event is already stored; worst case it is redelivered and deduped.
            self.metrics.inc("commit_failures")
            log.error("COMMIT_FAILED %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), e)

    def _dead_letter(self, msg, error: Exception, retry_count: int):
        raw = msg.value()
        try:
            original = json.loads(raw)
        except Exception:
            original = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        record = {
            "original_event": original,
            "error": {"type": type(error).__name__, "message": str(error)},
            "retry_count": retry_count,
            "source": {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
            "consumer_group": self.cfg.consumer_group,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        outcome = {}

        def _cb(err, _m):
            outcome["err"] = err

        self._dlq.produce(self.cfg.dlq_topic, key=msg.key(),
                          value=json.dumps(record).encode(),
                          headers=[("error_type", type(error).__name__.encode())],
                          on_delivery=_cb)
        self._dlq.flush(10)
        if outcome.get("err", "unset") is not None:    # not delivered (error or timeout)
            self.metrics.inc("dlq_publish_failures")
            raise DLQPublishError(f"DLQ publish failed for {msg.topic()}[{msg.partition()}]@{msg.offset()}: "
                                  f"{outcome.get('err', 'timeout')}")
        self.metrics.inc("dead_lettered")
        log.error("SENT_TO_DLQ %s[%d]@%d retry_count=%d", msg.topic(), msg.partition(),
                  msg.offset(), retry_count)
        self._commit(msg)       # only after the DLQ has acknowledged the record
