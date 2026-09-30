"""Reticle cleaning holds.

A reticle is automatically detained once it reaches its exposure limit or is
flagged contaminated.  While detained it must travel through a fixed sequence
of cleaning events before it can serve production again::

    AVAILABLE -> detained -> sent_to_clean -> returned_from_clean
              -> inspected (passed) -> released -> AVAILABLE

Every transition is an immutable, monotonically ordered event in the shared
append-only log, so duplicate commands replay instead of advancing and an
out-of-order or illegal transition can never move the state.  A failed
inspection leaves the reticle held and the cycle has to be repeated.  The
append history also answers what state (and on which policy/inspection basis)
a reticle was in at any past point in time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import Event, EventLog
from fabweave.core.timebox import TimeBox
from fabweave.core.versioning import RevisionHistory


class CleaningState(str, Enum):
    AVAILABLE = "available"
    DETAINED = "detained"
    SENT_FOR_CLEAN = "sent_for_clean"
    RETURNED_FROM_CLEAN = "returned_from_clean"
    INSPECTED = "inspected"
    INSPECTION_REJECTED = "inspection_rejected"


# event kind recorded for each transition and the state it moves the reticle to
_KIND_STATE: dict[str, CleaningState] = {
    "cleaning_detained": CleaningState.DETAINED,
    "sent_for_clean": CleaningState.SENT_FOR_CLEAN,
    "returned_from_clean": CleaningState.RETURNED_FROM_CLEAN,
    "inspection_passed": CleaningState.INSPECTED,
    "inspection_failed": CleaningState.INSPECTION_REJECTED,
    "released_from_clean": CleaningState.AVAILABLE,
}

# only these transitions are legal from the current state
_TRANSITIONS: dict[CleaningState, frozenset[str]] = {
    CleaningState.AVAILABLE: frozenset({"cleaning_detained"}),
    CleaningState.DETAINED: frozenset({"sent_for_clean"}),
    CleaningState.SENT_FOR_CLEAN: frozenset({"returned_from_clean"}),
    CleaningState.RETURNED_FROM_CLEAN: frozenset({"inspection_passed", "inspection_failed"}),
    CleaningState.INSPECTED: frozenset({"released_from_clean"}),
    CleaningState.INSPECTION_REJECTED: frozenset({"sent_for_clean"}),
}


@dataclass(frozen=True, slots=True)
class CleaningPolicy:
    revision: int
    exposure_limit: int

    def __post_init__(self) -> None:
        if self.revision <= 0 or self.exposure_limit <= 0:
            raise ValueError("invalid cleaning policy")


@dataclass(frozen=True, slots=True)
class CleaningSnapshot:
    reticle_id: str
    state: CleaningState
    cycle: int
    as_of: int
    last_event_id: str | None
    last_kind: str | None
    held_reasons: tuple[str, ...]
    policy_revision: int | None
    inspection_id: str | None
    inspection_passed: bool | None


@dataclass(frozen=True, slots=True)
class CleaningRecord:
    reticle_id: str
    event_id: str
    sequence: int
    occurred_at: int
    kind: str
    state: CleaningState
    cycle: int
    replay: bool


def _stream(reticle_id: str) -> str:
    return f"reticle-clean:{reticle_id}"


def _fold(events: tuple[Event, ...]) -> tuple[CleaningState, int, str | None, str | None, tuple[str, ...], int | None, str | None, bool | None]:
    state = CleaningState.AVAILABLE
    cycle = 0
    last_event_id: str | None = None
    last_kind: str | None = None
    held_reasons: tuple[str, ...] = ()
    policy_revision: int | None = None
    inspection_id: str | None = None
    inspection_passed: bool | None = None
    for event in events:
        kind = event.kind
        if kind == "cleaning_detained":
            cycle += 1
            held_reasons = tuple(event.data.get("reasons", ()))
            policy_revision = event.data.get("policy_revision")
        if kind in {"inspection_passed", "inspection_failed"}:
            inspection_id = event.data.get("inspection_id")
            inspection_passed = kind == "inspection_passed"
        if kind == "released_from_clean":
            held_reasons = ()
            inspection_id = None
            inspection_passed = None
        state = _KIND_STATE[kind]
        last_event_id = event.event_id
        last_kind = kind
    return state, cycle, last_event_id, last_kind, held_reasons, policy_revision, inspection_id, inspection_passed


def snapshot(reticle_id: str, events: tuple[Event, ...], as_of: int | None = None) -> CleaningSnapshot:
    if as_of is not None:
        events = tuple(event for event in events if event.occurred_at <= as_of)
    state, cycle, last_event_id, last_kind, reasons, policy_revision, inspection_id, passed = _fold(events)
    at = events[-1].occurred_at if events else (as_of if as_of is not None else 0)
    return CleaningSnapshot(reticle_id, state, cycle, at, last_event_id, last_kind, reasons, policy_revision, inspection_id, passed)


def valid_inspection(snapshot: CleaningSnapshot) -> bool:
    """Only a passed inspection recorded after the reticle came back is valid."""

    return snapshot.state is CleaningState.INSPECTED and snapshot.inspection_passed is True


def release_decision(snapshot: CleaningSnapshot) -> Decision:
    """Stage gate for putting a cleaned reticle back into service."""

    if snapshot.state is CleaningState.INSPECTED:
        if snapshot.inspection_passed:
            return allow(
                cycle=snapshot.cycle,
                inspection_id=snapshot.inspection_id,
                policy_revision=snapshot.policy_revision,
            )
        return deny("inspection_not_passed", inspection_id=snapshot.inspection_id)
    if snapshot.state is CleaningState.RETURNED_FROM_CLEAN:
        return deny("awaiting_inspection", cycle=snapshot.cycle)
    if snapshot.state in {CleaningState.DETAINED, CleaningState.SENT_FOR_CLEAN, CleaningState.INSPECTION_REJECTED}:
        return deny(f"cleaning_stage:{snapshot.state.value}", cycle=snapshot.cycle)
    return deny("reticle_not_detained", cycle=snapshot.cycle)


def cleaning_windows(reticle_id: str, events: tuple[Event, ...], open_end: int) -> tuple[TimeBox, ...]:
    """Half-open detained intervals; an open hold runs to ``open_end``."""

    detained_at: int | None = None
    cycle = 0
    windows: list[TimeBox] = []
    for event in events:
        if event.kind == "cleaning_detained":
            cycle += 1
            detained_at = event.occurred_at
        elif event.kind == "released_from_clean" and detained_at is not None:
            windows.append(TimeBox(detained_at, event.occurred_at))
            detained_at = None
    if detained_at is not None and open_end > detained_at:
        windows.append(TimeBox(detained_at, open_end))
    return tuple(windows)


class ReticleCleaningLog:
    """Event-sourced cleaning lifecycle for one or more reticles."""

    def __init__(self, log: EventLog | None = None) -> None:
        self.log = log if log is not None else EventLog()

    # ----- read side -------------------------------------------------------
    def history(self, reticle_id: str) -> tuple[Event, ...]:
        return self.log.read(_stream(reticle_id))

    def snapshot(self, reticle_id: str, as_of: int | None = None) -> CleaningSnapshot:
        return snapshot(reticle_id, self.history(reticle_id), as_of)

    def is_held(self, reticle_id: str) -> bool:
        return self.snapshot(reticle_id).state is not CleaningState.AVAILABLE

    def blocked_windows(self, reticle_id: str, open_end: int) -> tuple[TimeBox, ...]:
        return cleaning_windows(reticle_id, self.history(reticle_id), open_end)

    # ----- write side ------------------------------------------------------
    def _append(self, reticle_id: str, event_id: str, at: int, kind: str, data: dict[str, Any]) -> CleaningRecord:
        stream = _stream(reticle_id)
        prior = self.log.event(event_id)
        if prior is not None:
            state, cycle, *_ = _fold(self.log.read(stream))
            return CleaningRecord(reticle_id, prior.event_id, prior.sequence, prior.occurred_at, prior.kind, state, cycle, replay=True)
        current = self.snapshot(reticle_id)
        if kind not in _TRANSITIONS[current.state]:
            raise ValueError(f"illegal cleaning transition:{current.state.value}:{kind}")
        if kind == "cleaning_detained":
            cycle = current.cycle + 1
        else:
            cycle = current.cycle
        event = self.log.append(stream, len(self.log.read(stream)), event_id, at, kind, data)
        return CleaningRecord(reticle_id, event.event_id, event.sequence, event.occurred_at, event.kind, _KIND_STATE[kind], cycle, replay=False)

    def detain_if_required(
        self,
        reticle_id: str,
        event_id: str,
        at: int,
        exposures: int,
        contaminated: bool,
        policies: RevisionHistory[CleaningPolicy],
    ) -> CleaningRecord | None:
        """Auto-hold at the exposure limit or when contaminated; fail closed."""

        current = self.snapshot(reticle_id)
        if current.state is not CleaningState.AVAILABLE:
            return None
        try:
            revision = policies.at(at)
        except LookupError:
            raise ValueError("no cleaning policy effective at that time")
        reasons = []
        if exposures >= revision.value.exposure_limit:
            reasons.append("exposure_limit_exceeded")
        if contaminated:
            reasons.append("contaminated")
        if not reasons:
            return None
        return self._append(
            reticle_id,
            event_id,
            at,
            "cleaning_detained",
            {"reasons": reasons, "exposures": exposures, "policy_revision": revision.value.revision},
        )

    def send_to_clean(self, reticle_id: str, event_id: str, at: int, vendor: str) -> CleaningRecord:
        return self._append(reticle_id, event_id, at, "sent_for_clean", {"vendor": vendor})

    def return_from_clean(self, reticle_id: str, event_id: str, at: int, vendor: str) -> CleaningRecord:
        return self._append(reticle_id, event_id, at, "returned_from_clean", {"vendor": vendor})

    def record_inspection(
        self,
        reticle_id: str,
        event_id: str,
        at: int,
        inspection_id: str,
        passed: bool,
        findings: dict[str, Any] | None = None,
    ) -> CleaningRecord:
        kind = "inspection_passed" if passed else "inspection_failed"
        data: dict[str, Any] = {"inspection_id": inspection_id}
        if findings:
            data["findings"] = findings
        return self._append(reticle_id, event_id, at, kind, data)

    def release(self, reticle_id: str, event_id: str, at: int) -> CleaningRecord:
        current = self.snapshot(reticle_id)
        decision = release_decision(current)
        if not decision.allowed:
            raise ValueError(f"release_rejected:{':'.join(decision.reasons)}")
        return self._append(
            reticle_id,
            event_id,
            at,
            "released_from_clean",
            {"inspection_id": current.inspection_id, "policy_revision": current.policy_revision},
        )


def cleaning_reservation_decision(
    reticle_id: str,
    window: TimeBox,
    cleaning: ReticleCleaningLog,
    open_end: int,
) -> Decision:
    """Reservations overlapping a cleaning hold are rejected (fail closed)."""

    windows = cleaning.blocked_windows(reticle_id, open_end)
    overlap = tuple(item for item in windows if item.overlaps(window))
    evidence = {"reticle_id": reticle_id, "blocked_windows": tuple((item.start, item.end) for item in overlap)}
    if overlap or cleaning.is_held(reticle_id):
        return deny("reticle_cleaning_hold", **evidence)
    return allow(reticle_id=reticle_id, blocked_windows=())
