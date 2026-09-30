"""Event contract helpers: validation and the Kafka message key.

The schema is the one in the architecture doc (event_header / candidate_metadata /
event_payload). The queue layer only checks what it needs to route and dedupe;
full schema validation belongs to the event generator / ingestion gateway.
"""
from typing import Any

SUPPORTED_EVENT_TYPES = {
    "JOB_VIEWED", "JOB_WISHLISTED", "APPLICATION_STARTED",
    "APPLICATION_STEP_COMPLETED", "APPLICATION_DRAFT_SAVED",
    "APPLICATION_SUBMITTED", "APPLICATION_ABANDONED",
    "USER_REGISTERED", "USER_LOGGED_IN",
}


class InvalidEventError(ValueError):
    """The event can never be processed successfully -> never retry, go to DLQ."""


def _get(ev: dict, *path: str) -> Any:
    cur: Any = ev
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def validate_event(ev: Any) -> dict:
    if not isinstance(ev, dict):
        raise InvalidEventError("event must be a JSON object")
    missing = [
        ".".join(p) for p in (
            ("event_header", "event_id"),
            ("event_header", "event_type"),
            ("event_header", "timestamp"),
            ("event_header", "career_site_id"),
        ) if not _get(ev, *p)
    ]
    if missing:
        raise InvalidEventError(f"missing required fields: {', '.join(missing)}")
    if ev["event_header"]["event_type"] not in SUPPORTED_EVENT_TYPES:
        raise InvalidEventError(f"unsupported event_type: {ev['event_header']['event_type']}")
    partition_key(ev)   # raises if neither user_id nor session_id is present
    return ev


def partition_key(ev: dict) -> str:
    """`career_site_id:user_id` -> same user on the same site always lands on the
    same partition (strict per-user ordering); different users spread across partitions.

    Anonymous visitors (e.g. JOB_VIEWED before login) have no user_id, so we fall back
    to their session_id to keep one session ordered.
    """
    site = _get(ev, "event_header", "career_site_id")
    who = _get(ev, "candidate_metadata", "user_id") or _get(ev, "event_payload", "session_id")
    if not site or not who:
        raise InvalidEventError("cannot build key: need career_site_id and user_id (or session_id)")
    return f"{site}:{who}"


def event_id(ev: dict) -> str:
    return ev["event_header"]["event_id"]
