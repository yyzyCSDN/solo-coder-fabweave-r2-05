# FabWeave Operations Lab

FabWeave is a dependency-light Python reference project for semiconductor-fab
operations.  Its production modules model queue-time protection, dispatch,
reticle reservations and the reticle cleaning-hold lifecycle, recipe
qualification, SPC, metrology sampling, maintenance, chamber matching, holds,
genealogy, chemical aging, cleanroom excursions, demand control, rework, lot
merging, equipment dedication, contamination, takt, route yield and shipment
commitments.

Reticle cleaning is an event-sourced state machine: reaching the exposure
limit or a contamination report automatically detains the reticle, after which
send-to-wash, return, inspection and release must occur in that order.
Duplicate commands replay without effect, out-of-order or invalid commands are
rejected with a reason and append nothing, only a passed inspection followed
by an explicit release returns the reticle to service, reservations
overlapping a detention are blocked fail-closed, and the state at any instant
plus the exact evidence behind any release can be reconstructed historically.

The cross-domain workflow layer composes these services for material
readiness, dispatch-after-hold, qualified and cleaning-aware reservations and
assignments, metrology/SPC gates, containment, powered scheduling, rework
release, merge-to-commit and performance reporting.  It preserves domain
rejection reasons instead of replacing them with a generic success or failure.

Shared infrastructure supplies half-open time intervals, versioned policy
history, immutable decisions, quantity conservation and an append-only event
log with optimistic concurrency.  Domain services use these primitives to keep
event-time, idempotency and fail-closed behavior consistent.

Run the tests with `python -m pytest -q`.
