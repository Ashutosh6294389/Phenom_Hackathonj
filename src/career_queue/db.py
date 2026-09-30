"""Contract with the database-processing layer (owned by the DB teammate).

The consumer only needs ONE method. Semantics:
  * return normally            -> event is durably stored; consumer commits the offset
  * raise TransientDBError     -> DB unavailable/timeout; consumer retries with backoff
  * raise PermanentDBError     -> bad data / constraint violation; straight to the DLQ
  * any other exception        -> treated as transient
write_event MUST be idempotent on event_id (see README: counters need a dedup guard).
"""
import threading
from typing import Protocol


class TransientDBError(Exception):
    pass


class PermanentDBError(Exception):
    pass


class DatabaseWriter(Protocol):
    def write_event(self, event: dict) -> None: ...


class InMemoryDatabase:
    """Stand-in for Cassandra, with failure injection for the demo and tests."""

    def __init__(self):
        self.rows: dict[str, dict] = {}            # event_id -> event (idempotent upsert)
        self.write_calls = 0
        self.fail_first: dict[str, int] = {}       # event_id -> fail N times, then succeed
        self.always_fail: set[str] = set()         # event_id -> transient failure forever
        self.permanent_fail: set[str] = set()      # event_id -> permanent failure
        self.outage = False                        # every write fails transiently
        self._lock = threading.Lock()

    def write_event(self, event: dict) -> None:
        eid = event["event_header"]["event_id"]
        with self._lock:
            self.write_calls += 1
            if eid in self.permanent_fail:
                raise PermanentDBError("simulated constraint violation")
            if self.outage or eid in self.always_fail:
                raise TransientDBError("simulated: Cassandra unavailable")
            if self.fail_first.get(eid, 0) > 0:
                self.fail_first[eid] -= 1
                raise TransientDBError("simulated: write timeout")
            self.rows[eid] = event                 # same event_id twice -> still one row
