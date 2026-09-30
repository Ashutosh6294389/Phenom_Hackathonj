import json


class FakeMsg:
    def __init__(self, value, topic="career.events.raw", partition=0, offset=0, key=b"k"):
        self._v = value if isinstance(value, bytes) or value is None else json.dumps(value).encode()
        self._t, self._p, self._o, self._k = topic, partition, offset, key

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def error(self): return None


class FakeConsumer:
    def __init__(self, log):
        self.log, self.commits = log, []

    def commit(self, message=None, asynchronous=False):
        self.log.append(("commit", message.offset()))
        self.commits.append((message.partition(), message.offset() + 1))


class FakeDLQProducer:
    def __init__(self, log, fail=False):
        self.log, self.fail, self.records, self._cbs = log, fail, [], []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.log.append(("dlq_publish", topic))
        self.records.append((topic, key, json.loads(value)))
        self._cbs.append(on_delivery)

    def flush(self, timeout=None):
        for cb in self._cbs:
            cb("BROKER_DOWN" if self.fail else None, None)
        self._cbs = []
        return 0


class FakeKafkaProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, headers))

    def poll(self, t=0): return 0
    def flush(self, t=None): return 0
