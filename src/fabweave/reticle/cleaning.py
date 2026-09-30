"""Reticle cleaning-hold lifecycle.

A reticle must be withheld from production once it reaches its exposure limit
or is reported contaminated.  While it is away for cleaning it moves through a
fixed order of recordable events::

    AVAILABLE -> CLEANING_HOLD -> SENT_TO_WASH -> RETURNED -> INSPECTED -> AVAILABLE

A failed inspection is a valid inspection record, but it cannot release the
reticle: it opens a fresh cleaning hold and the whole sequence restarts.  Only
a passed inspection followed by an explicit release returns the reticle to
service and zeroes the exposure counter.

Every accepted transition is appended to the reticle's event stream, so the
state at any past instant, and the evidence that justified every release, can
be reconstructed.  Duplicate command ids replay the original decision and out
of order or invalid commands are rejected without appending anything, so the
lifecycle can never skip a stage.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import Event, EventLog
from fabweave.core.ids import validate_id
from fabweave.core.timebox import TimeBox


class ReticleStatus(str, Enum):
    AVAILABLE = "available"
    CLEANING_HOLD = "cleaning_hold"
    SENT_TO_WASH = "sent_to_wash"
    RETURNED = "returned"
    INSPECTED = "inspected"


@dataclass(frozen=True, slots=True)
class CleaningPolicy:
    """Exposure cap registered with a reticle."""

    revision: int
    exposure_limit: int

    def __post_init__(self) -> None:
        if self.revision <= 0 or self.exposure_limit <= 0:
            raise ValueError("invalid cleaning policy")


@dataclass(frozen=True, slots=True)
class InspectionCriteria:
    """Acceptance bands an inspection report must satisfy to pass."""

    bands: dict[str, tuple[float, float]]
    required_metrics: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.bands:
            raise ValueError("inspection criteria need at least one band")
        for name, (low, high) in self.bands.items():
            if not math.isfinite(low) or not math.isfinite(high) or high <= low:
                raise ValueError(f"invalid inspection band:{name}")
        required = frozenset(self.required_metrics)
        if not required.issubset(self.bands):
            raise ValueError("required metric without an acceptance band")
        object.__setattr__(self, "required_metrics", required)


@dataclass(frozen=True, slots=True)
class HoldCycle:
    """One completed or open cleaning detention."""

    cycle: int
    reasons: frozenset[str]
    held_at: int
    sent_at: int | None
    returned_at: int | None
    inspected_at: int | None
    passed: bool | None
    released_at: int | None

    @property
    def closed(self) -> bool:
        return self.released_at is not None

    def window(self) -> TimeBox | None:
        if self.released_at is None or self.released_at <= self.held_at:
            return None
        return TimeBox(self.held_at, self.released_at)


@dataclass(frozen=True, slots=True)
class ReleaseBasis:
    """Everything a release decision was based on."""

    cycle: int
    held_at: int
    reasons: frozenset[str]
    sent_at: int
    returned_at: int
    inspected_at: int
    inspection_passed: bool
    measurements: dict[str, float]
    metrics: tuple[str, ...]
    inspection_event_id: str
    released_at: int
    release_event_id: str
    exposure_limit: int
    policy_revision: int


@dataclass(frozen=True, slots=True)
class ReticleSnapshot:
    reticle_id: str
    status: ReticleStatus
    at: int
    total_exposures: int
    exposure_limit: int
    policy_revision: int
    cycle: int
    hold_reasons: frozenset[str]
    open_hold_since: int | None
    last_inspection_passed: bool | None


@dataclass(slots=True)
class _MutableState:
    reticle_id: str
    policy_revision: int
    exposure_limit: int
    total_exposures: int = 0
    status: ReticleStatus = ReticleStatus.AVAILABLE
    cycle: int = 0
    open_cycle: HoldCycle | None = None
    closed_cycles: list[HoldCycle] = field(default_factory=list)
    last_inspection: dict[str, Any] | None = None

    def hold(self, reasons: set[str], at: int) -> HoldCycle:
        self.cycle += 1
        self.open_cycle = HoldCycle(self.cycle, frozenset(reasons), at, None, None, None, None, None)
        self.status = ReticleStatus.CLEANING_HOLD
        return self.open_cycle

    def _require_open(self) -> HoldCycle:
        if self.open_cycle is None:  # pragma: no cover - guarded by every command
            raise LookupError("no open cleaning cycle")
        return self.open_cycle

    def mark_sent(self, at: int) -> None:
        cycle = self._require_open()
        self.open_cycle = HoldCycle(cycle.cycle, cycle.reasons, cycle.held_at, at, None, None, None, None)
        self.status = ReticleStatus.SENT_TO_WASH

    def mark_returned(self, at: int) -> None:
        cycle = self._require_open()
        self.open_cycle = HoldCycle(cycle.cycle, cycle.reasons, cycle.held_at, cycle.sent_at, at, None, None, None)
        self.status = ReticleStatus.RETURNED

    def mark_inspected(self, at: int, passed: bool) -> None:
        cycle = self._require_open()
        self.open_cycle = HoldCycle(
            cycle.cycle, cycle.reasons, cycle.held_at, cycle.sent_at, cycle.returned_at, at, passed, None
        )
        self.status = ReticleStatus.INSPECTED

    def close(self, at: int) -> HoldCycle:
        cycle = self._require_open()
        closed = HoldCycle(
            cycle.cycle, cycle.reasons, cycle.held_at, cycle.sent_at, cycle.returned_at,
            cycle.inspected_at, cycle.passed, at,
        )
        self.closed_cycles.append(closed)
        self.open_cycle = None
        self.status = ReticleStatus.AVAILABLE
        self.total_exposures = 0
        return closed


def _stream(reticle_id: str) -> str:
    return f"reticle-cleaning:{validate_id(reticle_id)}"


def _fold(event: Event, state: _MutableState) -> _MutableState:
    data = event.data
    if event.kind == "reticle_registered":
        state.policy_revision = data["policy_revision"]
        state.exposure_limit = data["exposure_limit"]
    elif event.kind == "exposures_added":
        state.total_exposures = data["total_exposures"]
    elif event.kind == "cleaning_held":
        state.hold(set(data["reasons"]), event.occurred_at)
        if data.get("auto"):
            state.total_exposures = data["total_exposures"]
    elif event.kind == "sent_to_wash":
        state.mark_sent(event.occurred_at)
    elif event.kind == "returned_from_clean":
        state.mark_returned(event.occurred_at)
    elif event.kind == "inspected":
        state.mark_inspected(event.occurred_at, data["passed"])
        state.last_inspection = data
    elif event.kind == "released_after_clean":
        state.close(event.occurred_at)
    return state


def _load(events: tuple[Event, ...]) -> _MutableState | None:
    state: _MutableState | None = None
    for event in events:
        if event.kind == "reticle_registered":
            state = _MutableState(event.data["reticle_id"], event.data["policy_revision"], event.data["exposure_limit"])
            continue
        if state is not None:
            _fold(event, state)
    return state


class ReticleCleaningService:
    """Event-sourced cleaning-hold lifecycle for every registered reticle."""

    def __init__(self, log: EventLog) -> None:
        self._log = log

    # ------------------------------------------------------------------ load

    def _state(self, reticle_id: str) -> _MutableState:
        state = _load(self._log.read(_stream(reticle_id)))
        if state is None:
            raise LookupError("reticle is not registered")
        return state

    def register(self, command_id: str, reticle_id: str, policy: CleaningPolicy, at: int) -> Decision:
        stream = _stream(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id)
        if self._log.read(stream):
            return deny("reticle_already_registered", reticle_id=reticle_id)
        self._log.append(
            stream, 0, command_id, at, "reticle_registered",
            {
                "reticle_id": reticle_id,
                "policy_revision": policy.revision,
                "exposure_limit": policy.exposure_limit,
            },
        )
        return allow(reticle_id=reticle_id, event_id=command_id, policy_revision=policy.revision,
                      exposure_limit=policy.exposure_limit)

    # ------------------------------------------------------------ production

    def add_exposures(self, command_id: str, reticle_id: str, count: int, at: int) -> Decision:
        if not isinstance(count, int) or count <= 0:
            raise ValueError("exposure count must be a positive integer")
        state = self._state(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id,
                          total_exposures=state.total_exposures, status=state.status.value)
        if state.status != ReticleStatus.AVAILABLE:
            return deny("reticle_cleaning_hold", reticle_id=reticle_id, status=state.status.value,
                        cycle=state.cycle)
        total = state.total_exposures + count
        sequence = self._log.read(_stream(reticle_id))[-1].sequence
        self._log.append(
            _stream(reticle_id), sequence, command_id, at, "exposures_added",
            {"count": count, "total_exposures": total},
        )
        evidence = {"reticle_id": reticle_id, "total_exposures": total,
                    "exposure_limit": state.exposure_limit}
        if total >= state.exposure_limit:
            held = self._log.append(
                _stream(reticle_id), sequence + 1, f"{command_id}-HOLD", at, "cleaning_held",
                {"reasons": ["exposure_limit_reached"], "auto": True, "total_exposures": total,
                 "cycle": state.cycle + 1},
            )
            evidence.update(auto_held=True, hold_event_id=held.event_id, cycle=held.data["cycle"])
        else:
            evidence.update(auto_held=False)
        return allow(**evidence)

    def report_contamination(self, command_id: str, reticle_id: str, contaminant: str, at: int) -> Decision:
        if not contaminant or not isinstance(contaminant, str):
            raise ValueError("contaminant must be a non-empty label")
        state = self._state(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id, status=state.status.value)
        reason = f"contamination:{contaminant}"
        if state.status == ReticleStatus.CLEANING_HOLD and state.open_cycle is not None:
            if reason in state.open_cycle.reasons:
                return deny("duplicate_cleaning_event", reticle_id=reticle_id, cycle=state.cycle)
            return deny("reticle_already_on_cleaning_hold", reticle_id=reticle_id, cycle=state.cycle)
        if state.status != ReticleStatus.AVAILABLE:
            return deny("reticle_cleaning_hold", reticle_id=reticle_id, status=state.status.value,
                        cycle=state.cycle)
        sequence = self._log.read(_stream(reticle_id))[-1].sequence
        event = self._log.append(
            _stream(reticle_id), sequence, command_id, at, "cleaning_held",
            {"reasons": [reason], "auto": False, "total_exposures": state.total_exposures,
             "cycle": state.cycle + 1},
        )
        return allow(reticle_id=reticle_id, hold_event_id=event.event_id, cycle=event.data["cycle"],
                      reason=reason)

    # ------------------------------------------------------- cleaning sequence

    def send_to_wash(self, command_id: str, reticle_id: str, vendor: str, at: int) -> Decision:
        if not vendor or not isinstance(vendor, str):
            raise ValueError("vendor must be a non-empty label")
        return self._advance(command_id, reticle_id, at, ReticleStatus.CLEANING_HOLD,
                             "sent_to_wash", {"vendor": vendor})

    def return_from_clean(self, command_id: str, reticle_id: str, at: int) -> Decision:
        return self._advance(command_id, reticle_id, at, ReticleStatus.SENT_TO_WASH,
                             "returned_from_clean", {})

    def inspect(
        self,
        command_id: str,
        reticle_id: str,
        at: int,
        measurements: dict[str, float],
        criteria: InspectionCriteria,
        inspector: str,
    ) -> Decision:
        if not inspector or not isinstance(inspector, str):
            raise ValueError("inspector must be a non-empty label")
        if not isinstance(measurements, dict) or not measurements:
            raise ValueError("inspection needs measurements")
        state = self._state(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            if existing.kind != "inspected":
                raise ValueError("command id was reused with different content")
            data = existing.data
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id, passed=data["passed"],
                          metrics=tuple(data["metrics"]), status=state.status.value)
        if state.status != ReticleStatus.RETURNED:
            return deny("inspection_out_of_order", reticle_id=reticle_id, status=state.status.value,
                        cycle=state.cycle)
        failures: list[str] = []
        for metric in sorted(criteria.required_metrics):
            if metric not in measurements:
                failures.append(f"missing_metric:{metric}")
        for metric, value in measurements.items():
            if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                failures.append(f"non_finite_metric:{metric}")
                continue
            band = criteria.bands.get(metric)
            if band is None:
                failures.append(f"unexpected_metric:{metric}")
            elif not band[0] <= value <= band[1]:
                failures.append(f"metric_out_of_tolerance:{metric}")
        passed = not failures
        sequence = self._log.read(_stream(reticle_id))[-1].sequence
        metrics = tuple(sorted(measurements))
        event = self._log.append(
            _stream(reticle_id), sequence, command_id, at, "inspected",
            {"passed": passed, "measurements": dict(measurements), "metrics": list(metrics),
             "inspector": inspector, "failures": failures},
        )
        state = self._state(reticle_id)
        evidence: dict[str, Any] = {"reticle_id": reticle_id, "event_id": command_id, "passed": passed,
                                    "metrics": metrics, "cycle": state.cycle}
        if passed:
            evidence.update(status=ReticleStatus.INSPECTED.value)
            return allow(**evidence)
        held = self._log.append(
            _stream(reticle_id), sequence + 1, f"{command_id}-REHOLD", at, "cleaning_held",
            {"reasons": ["inspection_failed"], "auto": True, "total_exposures": state.total_exposures,
             "inspection_event_id": event.event_id, "failures": failures, "cycle": state.cycle + 1},
        )
        evidence.update(status=ReticleStatus.CLEANING_HOLD.value, reheld=True,
                        rehold_event_id=held.event_id, failures=tuple(failures),
                        cycle=held.data["cycle"])
        return allow(**evidence)

    def release(self, command_id: str, reticle_id: str, at: int) -> Decision:
        state = self._state(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id,
                          status=state.status.value)
        if state.status != ReticleStatus.INSPECTED or state.open_cycle is None or not state.open_cycle.passed:
            reasons: list[str] = ["release_requires_passed_inspection"]
            if state.status == ReticleStatus.AVAILABLE:
                reasons = ["reticle_not_on_hold"]
            elif state.status in (ReticleStatus.CLEANING_HOLD, ReticleStatus.SENT_TO_WASH):
                reasons = ["cleaning_sequence_incomplete"]
            elif state.status == ReticleStatus.RETURNED:
                reasons = ["release_requires_inspection"]
            return deny(*reasons, reticle_id=reticle_id, status=state.status.value, cycle=state.cycle)
        sequence = self._log.read(_stream(reticle_id))[-1].sequence
        event = self._log.append(
            _stream(reticle_id), sequence, command_id, at, "released_after_clean",
            {"cycle": state.cycle},
        )
        return allow(reticle_id=reticle_id, event_id=command_id, cycle=state.cycle,
                      released_at=event.occurred_at, total_exposures=0)

    def _advance(
        self, command_id: str, reticle_id: str, at: int, expected: ReticleStatus, kind: str, data: dict[str, Any]
    ) -> Decision:
        state = self._state(reticle_id)
        existing = self._log.get(command_id)
        if existing is not None:
            if existing.kind != kind:
                raise ValueError("command id was reused with different content")
            return allow(reticle_id=reticle_id, replay=True, event_id=command_id, status=state.status.value)
        if state.status != expected:
            reason = {
                ReticleStatus.CLEANING_HOLD: "send_to_wash_out_of_order",
                ReticleStatus.SENT_TO_WASH: "return_out_of_order",
            }[expected]
            return deny(reason, reticle_id=reticle_id, status=state.status.value, cycle=state.cycle)
        sequence = self._log.read(_stream(reticle_id))[-1].sequence
        self._log.append(_stream(reticle_id), sequence, command_id, at, kind, data)
        state = self._state(reticle_id)
        return allow(reticle_id=reticle_id, event_id=command_id, status=state.status.value, cycle=state.cycle)

    # ---------------------------------------------------------------- queries

    def events(self, reticle_id: str) -> tuple[Event, ...]:
        return self._log.read(_stream(reticle_id))

    def snapshot_at(self, reticle_id: str, at: int) -> ReticleSnapshot:
        state = self._state(reticle_id)
        projected = _MutableState(reticle_id, state.policy_revision, state.exposure_limit)
        for event in self.events(reticle_id):
            if event.occurred_at > at:
                break
            _fold(event, projected)
        cycle = projected.open_cycle
        return ReticleSnapshot(
            reticle_id,
            projected.status,
            at,
            projected.total_exposures,
            projected.exposure_limit,
            projected.policy_revision,
            projected.cycle,
            cycle.reasons if cycle is not None else frozenset(),
            cycle.held_at if cycle is not None else None,
            projected.last_inspection["passed"] if projected.last_inspection is not None else None,
        )

    def cleaning_windows(self, reticle_id: str) -> tuple[TimeBox, ...]:
        self._state(reticle_id)
        windows: list[TimeBox] = []
        state = _MutableState(reticle_id, 0, 0)
        for event in self.events(reticle_id):
            before = state.open_cycle
            _fold(event, state)
            if event.kind == "released_after_clean" and before is not None:
                window = TimeBox(before.held_at, event.occurred_at)
                windows.append(window)
        return tuple(windows)

    def detention_intervals(self, reticle_id: str) -> tuple[tuple[int, int | None], ...]:
        """Every cleaning detention, including the still-open one.

        Closed detentions are ``[held_at, released_at)`` half-open intervals;
        an open detention ends with ``None`` (no release recorded yet).
        """

        state = self._state(reticle_id)
        intervals: list[tuple[int, int | None]] = [
            (window.start, window.end) for window in self.cleaning_windows(reticle_id)
        ]
        if state.open_cycle is not None:
            intervals.append((state.open_cycle.held_at, None))
        return tuple(sorted(intervals))

    def cycles(self, reticle_id: str) -> tuple[HoldCycle, ...]:
        state = self._state(reticle_id)
        cycles = sorted(state.closed_cycles, key=lambda item: item.cycle)
        if state.open_cycle is not None:
            cycles.append(state.open_cycle)
        return tuple(cycles)

    def release_basis(self, reticle_id: str, released_at: int | None = None) -> ReleaseBasis:
        """Return the recorded evidence that justified a release."""

        events = self.events(reticle_id)
        releases = [event for event in events if event.kind == "released_after_clean"]
        if not releases:
            raise LookupError("reticle has never been released")
        release = releases[-1] if released_at is None else next(
            (event for event in releases if event.occurred_at == released_at), None
        )
        if release is None:
            raise LookupError("no release at that time")
        cycle_no = release.data["cycle"]
        # The released cycle is the last hold/send/return/passed-inspection
        # sequence present before the release event (a failed inspection opens
        # a fresh hold, so the latest event of each kind is the right one).
        basis: dict[str, Event] = {}
        for event in events:
            if event.sequence > release.sequence:
                break
            if event.kind in {"cleaning_held", "sent_to_wash", "returned_from_clean"}:
                basis[event.kind] = event
            elif event.kind == "inspected" and event.data["passed"]:
                basis[event.kind] = event
        held_event = basis["cleaning_held"]
        sent_event = basis["sent_to_wash"]
        returned_event = basis["returned_from_clean"]
        inspected_event = basis["inspected"]
        registration = events[0]
        return ReleaseBasis(
            cycle=cycle_no,
            held_at=held_event.occurred_at,
            reasons=frozenset(held_event.data["reasons"]),
            sent_at=sent_event.occurred_at,
            returned_at=returned_event.occurred_at,
            inspected_at=inspected_event.occurred_at,
            inspection_passed=True,
            measurements=dict(inspected_event.data["measurements"]),
            metrics=tuple(inspected_event.data["metrics"]),
            inspection_event_id=inspected_event.event_id,
            released_at=release.occurred_at,
            release_event_id=release.event_id,
            exposure_limit=registration.data["exposure_limit"],
            policy_revision=registration.data["policy_revision"],
        )


# ----------------------------------------------------------------- decisions


def reservation_decision(
    service: ReticleCleaningService,
    reticle_id: str,
    window: TimeBox,
    at: int,
) -> Decision:
    """Fail-closed gate: a reticle cannot be reserved while detained.

    Overlap is checked against every recorded detention: completed cleaning
    windows (historical planning must still see them) and any open hold.  An
    open hold extends to the end of time, so a window that merely starts before
    the hold but reaches into it is blocked too.
    """

    try:
        snapshot = service.snapshot_at(reticle_id, at)
    except LookupError:
        return deny("reticle_not_registered", reticle_id=reticle_id)
    blocked: list[tuple[int, int | None]] = []
    for held_at, released_at in service.detention_intervals(reticle_id):
        end = released_at if released_at is not None else window.end + 1
        if window.start < end and held_at < window.end:
            blocked.append((held_at, released_at))
    if blocked:
        return deny("reticle_cleaning_hold", reticle_id=reticle_id, status=snapshot.status.value,
                    cycle=snapshot.cycle, blocked_windows=tuple(blocked))
    return allow(reticle_id=reticle_id, status=snapshot.status.value, cycle=snapshot.cycle)
