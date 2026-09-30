from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class Event:
    stream: str
    sequence: int
    event_id: str
    occurred_at: int
    kind: str
    data: dict[str, Any]


class EventLog:
    """In-memory append-only log with optimistic concurrency and idempotency."""

    def __init__(self) -> None:
        self._streams: dict[str, list[Event]] = {}
        self._ids: dict[str, Event] = {}

    def append(
        self,
        stream: str,
        expected_sequence: int,
        event_id: str,
        occurred_at: int,
        kind: str,
        data: dict[str, Any],
    ) -> Event:
        existing = self._ids.get(event_id)
        candidate = (stream, occurred_at, kind, data)
        if existing is not None:
            stored = (existing.stream, existing.occurred_at, existing.kind, existing.data)
            if stored != candidate:
                raise ValueError("event id was reused with different content")
            return deepcopy(existing)
        events = self._streams.setdefault(stream, [])
        if len(events) != expected_sequence:
            raise ValueError("stream sequence conflict")
        if events and occurred_at < events[-1].occurred_at:
            raise ValueError("event time moved backwards")
        event = Event(stream, len(events) + 1, event_id, occurred_at, kind, deepcopy(data))
        events.append(event)
        self._ids[event_id] = event
        return deepcopy(event)

    def event(self, event_id: str) -> Event | None:
        existing = self._ids.get(event_id)
        return deepcopy(existing) if existing is not None else None

    def read(self, stream: str) -> tuple[Event, ...]:
        return tuple(deepcopy(self._streams.get(stream, [])))

    def replay(self, streams: Iterable[str]) -> tuple[Event, ...]:
        events = [event for stream in streams for event in self._streams.get(stream, [])]
        return tuple(deepcopy(sorted(events, key=lambda event: (event.occurred_at, event.stream, event.sequence))))
