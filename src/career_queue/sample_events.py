"""Stand-in for the teammate's event generator (same schema as the architecture doc)."""
import uuid
from datetime import datetime, timezone


def make_event(event_type="JOB_VIEWED", career_site_id="tenant_acme_corp_8921",
               user_id="usr_1", job_id="job_1", **extra_action) -> dict:
    return {
        "event_header": {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "career_site_id": career_site_id,
            "source_channel": "WEB_APP",
            "attribution": {"referrer_type": "SEARCH"},
        },
        "candidate_metadata": {"user_id": user_id, "email": f"{user_id}@example.com"},
        "event_payload": {"job_id": job_id, "session_id": f"sess_{user_id}",
                          "action_details": extra_action},
    }
