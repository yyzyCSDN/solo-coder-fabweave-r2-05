from __future__ import annotations

import pytest

from fabweave.core.events import EventLog
from fabweave.core.timebox import TimeBox
from fabweave.reticle.cleaning import (
    CleaningPolicy,
    InspectionCriteria,
    ReticleCleaningService,
    ReticleStatus,
    reservation_decision,
)
from fabweave.reticle.reservation import Reservation, ReservationBook
from fabweave.workflows.control import reserve_available_reticles


CRITERIA = InspectionCriteria(
    {"defect_density": (0.0, 1.0), "transmission": (0.9, 1.0)},
    frozenset({"defect_density", "transmission"}),
)


def _service() -> ReticleCleaningService:
    service = ReticleCleaningService(EventLog())
    assert service.register("CMD-REG", "RET-1", CleaningPolicy(1, 100), 0).allowed
    return service


def _complete_clean_cycle(service: ReticleCleaningService, prefix: str, held_at: int = 10) -> None:
    assert service.send_to_wash(f"{prefix}-SEND", "RET-1", "VENDOR-A", held_at + 1).allowed
    assert service.return_from_clean(f"{prefix}-RET", "RET-1", held_at + 2).allowed
    decision = service.inspect(
        f"{prefix}-INSP", "RET-1", held_at + 3, {"defect_density": 0.2, "transmission": 0.97}, CRITERIA, "QA-1"
    )
    assert decision.allowed and decision.evidence["passed"]
    assert service.release(f"{prefix}-REL", "RET-1", held_at + 4).allowed


def test_reticle_auto_enters_hold_at_exposure_limit():
    service = _service()
    assert not service.add_exposures("CMD-E1", "RET-1", 60, 1).evidence["auto_held"]
    decision = service.add_exposures("CMD-E2", "RET-1", 40, 2)
    assert decision.allowed and decision.evidence["auto_held"]
    snapshot = service.snapshot_at("RET-1", 2)
    assert snapshot.status is ReticleStatus.CLEANING_HOLD
    assert snapshot.hold_reasons == frozenset({"exposure_limit_reached"})
    assert snapshot.total_exposures == 100


def test_exposures_are_blocked_for_the_entire_cleaning_detention():
    service = _service()
    service.add_exposures("CMD-E1", "RET-1", 100, 1)
    assert service.add_exposures("CMD-E2", "RET-1", 1, 3).reasons == ("reticle_cleaning_hold",)
    service.send_to_wash("CMD-S", "RET-1", "V", 4)
    service.return_from_clean("CMD-R", "RET-1", 5)
    service.inspect("CMD-I", "RET-1", 6, {"defect_density": 0.1, "transmission": 0.99}, CRITERIA, "QA")
    # Still detained after inspection: only the explicit release reopens exposure.
    assert service.add_exposures("CMD-E3", "RET-1", 1, 7).reasons == ("reticle_cleaning_hold",)
    service.release("CMD-L", "RET-1", 8)
    reopened = service.add_exposures("CMD-E4", "RET-1", 5, 9)
    assert reopened.allowed and reopened.evidence["total_exposures"] == 5


def test_contamination_report_opens_cleaning_hold():
    service = _service()
    decision = service.report_contamination("CMD-C", "RET-1", "CU", 5)
    assert decision.allowed and decision.evidence["reason"] == "contamination:CU"
    snapshot = service.snapshot_at("RET-1", 5)
    assert snapshot.status is ReticleStatus.CLEANING_HOLD
    assert snapshot.open_hold_since == 5
    # Duplicate or extra contamination reports cannot advance an open hold.
    assert service.report_contamination("CMD-C", "RET-1", "CU", 5).evidence.get("replay")
    assert service.report_contamination("CMD-C2", "RET-1", "FE", 6).reasons == (
        "reticle_already_on_cleaning_hold",
    )
    events_before = len(service.events("RET-1"))
    service.report_contamination("CMD-C3", "RET-1", "NI", 7)
    assert len(service.events("RET-1")) == events_before


def test_cleaning_events_must_arrive_in_order_and_never_skip_stages():
    service = _service()
    service.report_contamination("CMD-C", "RET-1", "CU", 10)
    # Returning before sending to wash is rejected.
    assert service.return_from_clean("CMD-R-EARLY", "RET-1", 11).reasons == ("return_out_of_order",)
    # Inspecting before the reticle is returned is rejected.
    bad = service.inspect(
        "CMD-I-EARLY", "RET-1", 12, {"defect_density": 0.1, "transmission": 0.99}, CRITERIA, "QA"
    )
    assert bad.reasons == ("inspection_out_of_order",)
    # Releasing mid-cycle is rejected.
    assert service.release("CMD-L-EARLY", "RET-1", 13).reasons == ("cleaning_sequence_incomplete",)
    # Rejected commands append nothing.
    kinds = [event.kind for event in service.events("RET-1")]
    assert kinds == ["reticle_registered", "cleaning_held"]
    # Proper order works.
    _complete_clean_cycle(service, "CMD", held_at=10)
    assert service.snapshot_at("RET-1", 14).status is ReticleStatus.AVAILABLE


def test_duplicate_command_replays_without_appending_or_advancing():
    service = _service()
    service.report_contamination("CMD-C", "RET-1", "CU", 10)
    service.send_to_wash("CMD-S", "RET-1", "VENDOR-A", 11)
    events_before = len(service.events("RET-1"))
    replay = service.send_to_wash("CMD-S", "RET-1", "VENDOR-A", 11)
    assert replay.allowed and replay.evidence["replay"]
    assert len(service.events("RET-1")) == events_before
    # Same command id with different content is an error.
    with pytest.raises(ValueError):
        service.return_from_clean("CMD-S", "RET-1", 12)


def test_only_a_valid_passed_inspection_can_release_a_reticle():
    service = _service()
    service.add_exposures("CMD-E", "RET-1", 100, 10)
    service.send_to_wash("CMD-S", "RET-1", "V", 11)
    service.return_from_clean("CMD-R", "RET-1", 12)
    # An inspection missing a required metric is recorded as a failed one and
    # opens a fresh cleaning hold instead of releasing.
    failed = service.inspect("CMD-I1", "RET-1", 13, {"defect_density": 0.2}, CRITERIA, "QA")
    assert failed.allowed and not failed.evidence["passed"] and failed.evidence["reheld"]
    assert service.snapshot_at("RET-1", 13).status is ReticleStatus.CLEANING_HOLD
    assert service.release("CMD-L1", "RET-1", 14).reasons == ("cleaning_sequence_incomplete",)
    # Out-of-tolerance measurement also fails and reholds.
    service.send_to_wash("CMD-S2", "RET-1", "V", 15)
    service.return_from_clean("CMD-R2", "RET-1", 16)
    failed2 = service.inspect(
        "CMD-I2", "RET-1", 17, {"defect_density": 2.5, "transmission": 0.99}, CRITERIA, "QA"
    )
    assert "metric_out_of_tolerance:defect_density" in failed2.evidence["failures"]
    # A full, passing cycle finally releases.
    service.send_to_wash("CMD-S3", "RET-1", "V", 18)
    service.return_from_clean("CMD-R3", "RET-1", 19)
    passed = service.inspect(
        "CMD-I3", "RET-1", 20, {"defect_density": 0.3, "transmission": 0.95}, CRITERIA, "QA"
    )
    assert passed.evidence["passed"]
    # Passed inspection alone still detains; release is its own gated step.
    assert service.release("CMD-L-BEFORE", "RET-1", 20).allowed  # same instant as inspection
    assert service.snapshot_at("RET-1", 20).status is ReticleStatus.AVAILABLE


def test_release_resets_exposure_counter_for_next_cycle():
    service = _service()
    service.add_exposures("CMD-E", "RET-1", 100, 1)
    _complete_clean_cycle(service, "CMD-1", held_at=1)
    assert service.snapshot_at("RET-1", 100).total_exposures == 0
    assert service.cycles("RET-1")[0].window() == TimeBox(1, 5)


def test_reservations_are_blocked_during_cleaning_and_open_at_the_boundary():
    service = _service()
    service.add_exposures("CMD-E", "RET-1", 100, 10)
    # Window entirely inside the open hold.
    assert not reservation_decision(service, "RET-1", TimeBox(20, 30), 15).allowed
    # Window starting in the future but overlapping the open hold is blocked.
    assert reservation_decision(service, "RET-1", TimeBox(5, 12), 9).reasons == ("reticle_cleaning_hold",)
    # Historical planning before the hold is fine.
    assert reservation_decision(service, "RET-1", TimeBox(0, 10), 9).allowed
    _complete_clean_cycle(service, "CMD", held_at=10)
    # After release the recorded window [10, 14) still blocks overlap ...
    assert reservation_decision(service, "RET-1", TimeBox(12, 20), 20).reasons == ("reticle_cleaning_hold",)
    # ... but half-open touching intervals do not.
    assert reservation_decision(service, "RET-1", TimeBox(14, 20), 20).allowed
    assert reservation_decision(service, "RET-1", TimeBox(0, 10), 20).allowed


def test_batch_reservation_is_fail_closed_and_leaves_no_partial_booking():
    service = ReticleCleaningService(EventLog())
    service.register("REG-1", "RET-1", CleaningPolicy(1, 100), 0)
    service.register("REG-2", "RET-2", CleaningPolicy(1, 100), 0)
    service.add_exposures("E-1", "RET-2", 100, 5)
    book = ReservationBook()
    requested = [
        Reservation("RET-1", "JOB-1", TimeBox(10, 20), 1),
        Reservation("RET-2", "JOB-1", TimeBox(10, 20), 1),
    ]
    decision = reserve_available_reticles(service, book, "REQ-1", requested, 8)
    assert decision.reasons == ("reticle_cleaning_hold",)
    assert book.reservations("RET-1") == () and book.reservations("RET-2") == ()
    # Idempotent replay after nothing was booked.
    assert not reserve_available_reticles(service, book, "REQ-1", requested, 8).allowed
    # Once the detained reticle is released the same batch books atomically.
    service.send_to_wash("S", "RET-2", "V", 6)
    service.return_from_clean("R", "RET-2", 7)
    service.inspect("I", "RET-2", 8, {"defect_density": 0.1, "transmission": 0.99}, CRITERIA, "QA")
    service.release("L", "RET-2", 9)
    booked = reserve_available_reticles(service, book, "REQ-2", requested, 9)
    assert booked.allowed and booked.evidence["reservation_count"] == 2


def test_historical_snapshots_and_release_basis_are_reconstructable():
    service = _service()
    service.add_exposures("E-1", "RET-1", 100, 10)
    _complete_clean_cycle(service, "C1", held_at=10)
    # Second cycle from contamination, released later.
    service.report_contamination("C-2", "RET-1", "CU", 40)
    _complete_clean_cycle(service, "C2", held_at=40)

    # Point-in-time status reflects the past even though the reticle is free now.
    assert service.snapshot_at("RET-1", 11).status is ReticleStatus.SENT_TO_WASH
    assert service.snapshot_at("RET-1", 12).status is ReticleStatus.RETURNED
    assert service.snapshot_at("RET-1", 13).status is ReticleStatus.INSPECTED
    assert service.snapshot_at("RET-1", 14).status is ReticleStatus.AVAILABLE
    assert service.snapshot_at("RET-1", 41).hold_reasons == frozenset({"contamination:CU"})

    # The basis for the latest release names every recorded predecessor.
    basis = service.release_basis("RET-1")
    assert basis.cycle == 2
    assert basis.reasons == frozenset({"contamination:CU"})
    assert (basis.held_at, basis.sent_at, basis.returned_at, basis.inspected_at, basis.released_at) == (
        40, 41, 42, 43, 44,
    )
    assert basis.measurements == {"defect_density": 0.2, "transmission": 0.97}
    assert basis.metrics == ("defect_density", "transmission")
    assert basis.policy_revision == 1 and basis.exposure_limit == 100
    # The first release's basis is also retrievable by its historical time.
    first = service.release_basis("RET-1", released_at=14)
    assert first.cycle == 1 and first.reasons == frozenset({"exposure_limit_reached"})


def test_event_log_get_returns_stored_event_or_none_isolated():
    log = EventLog()
    log.append("S", 0, "EVT-1", 1, "kind", {"v": 1})
    fetched = log.get("EVT-1")
    assert fetched is not None and fetched.data == {"v": 1}
    assert log.get("MISSING") is None


def test_backdated_lifecycle_event_is_rejected_by_the_log():
    service = _service()
    service.add_exposures("E-1", "RET-1", 10, 5)
    with pytest.raises(ValueError):
        service.report_contamination("C-OLD", "RET-1", "CU", 4)


def test_unregistered_reticle_queries_fail_closed():
    service = ReticleCleaningService(EventLog())
    assert reservation_decision(service, "RET-X", TimeBox(0, 10), 0).reasons == ("reticle_not_registered",)
    with pytest.raises(LookupError):
        service.snapshot_at("RET-X", 0)
