"""Transformation utilities for listening events.

This module contains helper functions that validate and enrich raw listening
events before they are persisted to PostgreSQL.  The unit tests in
`tests/unit/test_transformations.py` exercise a subset of the validation
rules:

* Required keys must be present.
* The ISO-8601 timestamp must not be in the future.
* Very short, non-completed events (< 5 seconds) are considered bot-like
  and are rejected.

The function returns ``True`` when the event passes all checks and ``False``
otherwise.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict


def _has_required_keys(event: Dict[str, Any]) -> bool:
    """Return ``True`` if *event* contains all mandatory keys.

    The test suite only verifies the presence of ``user_id`` but the real
    pipeline expects a richer set.  We therefore check a sensible superset of
    fields used throughout the code base.
    """
    required = {
        "event_id",
        "user_id",
        "track_id",
        "timestamp",
        "duration_ms",
        "completed",
        "device_type",
        "geo_country",
        "event_source",
    }
    # ``source_peer`` or ``source_peer_id`` is optional - the DAG supplies a
    # default, so we do not enforce it here.
    return required.issubset(event.keys())


def _timestamp_not_future(ts: str) -> bool:
    """Validate that *ts* (ISO-8601) is not later than ``datetime.utcnow()``.
    ``ValueError`` from ``fromisoformat`` is treated as invalid.
    """
    try:
        # ``fromisoformat`` does not understand a trailing ``Z`` - replace it.
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        event_dt = datetime.datetime.fromisoformat(ts)
    except Exception:
        return False
    # Compare in UTC - ``event_dt`` may be timezone-aware after the replace.
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return event_dt <= now


def _is_not_bot(event: Dict[str, Any]) -> bool:
    """Detect bot-like patterns.

    The current rule is simple: if the duration is less than five seconds
    (5 000 ms) **and** the event is not marked as ``completed``, it is
    considered a bot and rejected.
    """
    try:
        duration = int(event.get("duration_ms", 0))
        completed = bool(event.get("completed", False))
    except Exception:
        return False
    if duration < 5_000 and not completed:
        return False
    return True


def is_valid_listening_event(event: Dict[str, Any]) -> bool:
    """Validate a listening event.

    The validation combines three independent checks:

    1. **Required keys** - the event must contain a baseline set of fields.
    2. **Timestamp** - the ``timestamp`` must not be in the future.
    3. **Bot detection** - short, non-completed events are filtered out.

    If any check fails, the function returns ``False``; otherwise ``True``.
    """
    if not _has_required_keys(event):
        return False
    if not _timestamp_not_future(event.get("timestamp", "")):
        return False
    if not _is_not_bot(event):
        return False
    return True
