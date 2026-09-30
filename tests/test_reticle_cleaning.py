from __future__ import annotations

import pytest

from fabweave.core.events import EventLog
from fabweave.core.timebox import TimeBox
from fabweave.core.versioning import RevisionHistory
from fabweave.recipe.qualification import Qualification
from fabweave.reticle.cleaning import (
    CleaningPolicy,
    CleaningState,
    ReticleCleaningLog,
    cleaning_windows,
    release_decision,
    valid_inspection,
)
from fabweave.reticle.reservation import Reservation, ReservationBook
from fabweave.workflows.control import reserve_cleanable

RET = "RET-1"


def policies() -> RevisionHistory[CleaningPolicy]:
    history: RevisionHistory[CleaningPolicy] = RevisionHistory()
    history.publish(1, 0, CleaningPolicy(1, 100))
    history.publish(2, 500, CleaningPolicy(2, 50))
    return history


def full_cycle(cleaning: ReticleCleaningLog, start: int = 10) -> None:
    assert cleaning.detain_if_required(RET, "EVT-D", start, 101, False, policies()).kind == "cleaning_detained"
    cleaning.send_to_clean(RET, "EVT-S", start + 1, "VENDOR-X")
    cleaning.return_from_clean(RET, "EVT-R", start + 2, "VENDOR-X")
    cleaning.record_inspection(RET, "EVT-I", start + 3, "INSP-1", True)
    cleaning.release(RET, "EVT-L", start + 4)


def test_limit_or_contamination_auto_detains_and_keeps_basis():
    cleaning = ReticleCleaningLog()
    by_limit = cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    assert by_limit is not None and by_limit.state is CleaningState.DETAINED
    snap = cleaning.snapshot(RET)
    assert snap.held_reasons == ("exposure_limit_exceeded",) and snap.policy_revision == 1

    contaminated = ReticleCleaningLog()
    record = contaminated.detain_if_required("RET-2", "EVT-D2", 10, 0, True, policies())
    assert record is not None
    assert contaminated.snapshot("RET-2").held_reasons == ("contaminated",)

    below_limit = ReticleCleaningLog()
    assert below_limit.detain_if_required("RET-3", "EVT-D3", 10, 50, False, policies()) is None
    at_limit = ReticleCleaningLog()
    assert at_limit.detain_if_required("RET-5", "EVT-D5", 10, 100, False, policies()) is not None
    with pytest.raises(ValueError):
        # fail closed: no policy effective at that time
        ReticleCleaningLog().detain_if_required("RET-4", "EVT-D4", 10, 999, True, RevisionHistory())


def test_full_sequence_records_send_return_inspect_release():
    cleaning = ReticleCleaningLog()
    full_cycle(cleaning)
    snap = cleaning.snapshot(RET)
    assert snap.state is CleaningState.AVAILABLE and snap.cycle == 1 and snap.held_reasons == ()
    kinds = tuple(event.kind for event in cleaning.history(RET))
    assert kinds == (
        "cleaning_detained",
        "sent_for_clean",
        "returned_from_clean",
        "inspection_passed",
        "released_from_clean",
    )


def test_duplicate_events_replay_and_do_not_advance_state():
    cleaning = ReticleCleaningLog()
    cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    again = cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    assert again is None  # already detained, not a new hold
    first = cleaning.send_to_clean(RET, "EVT-S", 11, "VENDOR-X")
    replay = cleaning.send_to_clean(RET, "EVT-S", 11, "VENDOR-X")
    assert replay.replay and replay.sequence == first.sequence
    assert len(cleaning.history(RET)) == 2
    assert cleaning.snapshot(RET).state is CleaningState.SENT_FOR_CLEAN


def test_out_of_order_and_illegal_transitions_are_rejected():
    cleaning = ReticleCleaningLog()
    cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    with pytest.raises(ValueError):
        cleaning.return_from_clean(RET, "EVT-BAD", 11, "VENDOR-X")
    cleaning.send_to_clean(RET, "EVT-S", 11, "VENDOR-X")
    with pytest.raises(ValueError):
        # event time must not move backwards
        cleaning.return_from_clean(RET, "EVT-R", 5, "VENDOR-X")
    with pytest.raises(ValueError):
        cleaning.release(RET, "EVT-L", 12)
    assert cleaning.snapshot(RET).state is CleaningState.SENT_FOR_CLEAN


def test_reservations_are_blocked_during_cleaning_and_open_after_release():
    cleaning = ReticleCleaningLog()
    full_cycle(cleaning)
    second = ReticleCleaningLog()
    second.detain_if_required(RET, "EVT-D2", 100, 0, True, policies())
    assert cleaning.blocked_windows(RET, 1_000) == (TimeBox(10, 14),)
    assert second.blocked_windows(RET, 1_000) == (TimeBox(100, 1_000),)
    assert cleaning.snapshot(RET).state is CleaningState.AVAILABLE
    assert second.is_held(RET)
    # the detained interval covers the closed window [10,14); touching is free
    assert not any(window.overlaps(TimeBox(14, 20)) for window in cleaning.blocked_windows(RET, 1_000))


def test_only_valid_inspection_can_release():
    cleaning = ReticleCleaningLog()
    cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    awaiting = cleaning.snapshot(RET)
    assert release_decision(awaiting).reasons == ("cleaning_stage:detained",)

    cleaning.send_to_clean(RET, "EVT-S", 11, "VENDOR-X")
    cleaning.return_from_clean(RET, "EVT-R", 12, "VENDOR-X")
    assert release_decision(cleaning.snapshot(RET)).reasons == ("awaiting_inspection",)

    cleaning.record_inspection(RET, "EVT-I", 13, "INSP-F", False, {"defect": "haze"})
    failed = cleaning.snapshot(RET)
    assert failed.state is CleaningState.INSPECTION_REJECTED and not valid_inspection(failed)
    with pytest.raises(ValueError):
        cleaning.release(RET, "EVT-L", 14)
    # a rejected inspection forces a repeat cleaning cycle
    cleaning.send_to_clean(RET, "EVT-S2", 15, "VENDOR-X")
    cleaning.return_from_clean(RET, "EVT-R2", 16, "VENDOR-X")
    cleaning.record_inspection(RET, "EVT-I2", 17, "INSP-2", True)
    passed = cleaning.snapshot(RET)
    assert valid_inspection(passed) and release_decision(passed).allowed
    released = cleaning.release(RET, "EVT-L", 18)
    assert released.state is CleaningState.AVAILABLE and released.cycle == 1


def test_historical_point_in_time_shows_then_state_and_basis():
    cleaning = ReticleCleaningLog()
    full_cycle(cleaning)
    # policy revision 2 tightens the limit to 50 starting at 500
    second = cleaning.detain_if_required(RET, "EVT-D2", 600, 60, False, policies())
    assert second is not None
    assert cleaning.snapshot(RET).policy_revision == 2

    at_release = cleaning.snapshot(RET, as_of=14)
    assert at_release.state is CleaningState.AVAILABLE and at_release.cycle == 1
    while_held = cleaning.snapshot(RET, as_of=11)
    assert while_held.state is CleaningState.SENT_FOR_CLEAN
    assert while_held.held_reasons == ("exposure_limit_exceeded",)
    assert while_held.policy_revision == 1 and while_held.as_of == 11
    later = cleaning.snapshot(RET, as_of=600)
    assert later.state is CleaningState.DETAINED and later.cycle == 2 and later.policy_revision == 2
    assert cleaning_windows(RET, cleaning.history(RET), 1_000) == (TimeBox(10, 14), TimeBox(600, 1_000))


def test_workflow_blocks_reservation_for_held_reticle_without_booking():
    book = ReservationBook()
    cleaning = ReticleCleaningLog()
    cleaning.detain_if_required(RET, "EVT-D", 10, 101, False, policies())
    reservation = Reservation(RET, "JOB-1", TimeBox(0, 100), 1)
    qualifications = [Qualification("TOOL-1", "CH-1", 1, 0, 100)]
    decision = reserve_cleanable(book, cleaning, "REQ-1", [reservation], {"JOB-1": ("TOOL-1", "CH-1")}, qualifications, 0, 1_000)
    assert not decision.allowed and "reticle_cleaning_hold" in decision.reasons
    assert book.reservations(RET) == ()

    cleaning.send_to_clean(RET, "EVT-S", 11, "VENDOR-X")
    cleaning.return_from_clean(RET, "EVT-R", 12, "VENDOR-X")
    cleaning.record_inspection(RET, "EVT-I", 13, "INSP-1", True)
    cleaning.release(RET, "EVT-L", 14)
    after = Reservation(RET, "JOB-2", TimeBox(20, 30), 1)
    decision = reserve_cleanable(book, cleaning, "REQ-2", [after], {"JOB-2": ("TOOL-1", "CH-1")}, qualifications, 20, 1_000)
    assert decision.allowed and len(book.reservations(RET)) == 1
