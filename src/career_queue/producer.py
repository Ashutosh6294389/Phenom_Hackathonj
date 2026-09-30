"""Producer interface handed to the event-generator teammate.

    producer = EventProducer(load_config())
    producer.publish(event_dict)      # non-blocking; raises InvalidEventError on bad events
    producer.flush()                  # call on shutdown
"""
import json
import logging
from typing import Callable, Optional

from .config import Config
from .events import event_id, partition_key, validate_event

log = logging.getLogger("producer")


class EventProducer:
    def __init__(self, cfg: Config, producer=None):
        self.cfg = cfg
        if producer is None:
            from confluent_kafka import Producer
            producer = Producer({
                "bootstrap.servers": cfg.bootstrap_servers,
                "client.id": "career-event-producer",
                # no duplicates / no reordering from producer-side retries
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 5,
                "delivery.timeout.ms": 30_000,
            })
        self._p = producer

    def publish(self, event: dict, on_delivery: Optional[Callable] = None) -> str:
        """Validate and enqueue one event. Returns the message key used."""
        validate_event(event)
        key = partition_key(event)
        value = json.dumps(event, separators=(",", ":")).encode()
        headers = [("event_id", event_id(event).encode()),
                   ("event_type", event["event_header"]["event_type"].encode())]

        def _cb(err, msg):
            if err is not None:
                log.error("DELIVERY_FAILED event_id=%s key=%s err=%s", event_id(event), key, err)
            else:
                log.info("PRODUCED event_id=%s key=%s -> %s[%d]@%d", event_id(event), key,
                         msg.topic(), msg.partition(), msg.offset())
            if on_delivery:
                on_delivery(err, msg)

        while True:
            try:
                self._p.produce(self.cfg.raw_topic, key=key.encode(), value=value,
                                headers=headers, on_delivery=_cb)
                break
            except BufferError:          # local queue full -> drain and retry
                self._p.poll(0.5)
        self._p.poll(0)                  # serve delivery callbacks
        return key

    def flush(self, timeout: float = 10.0) -> int:
        return self._p.flush(timeout)
