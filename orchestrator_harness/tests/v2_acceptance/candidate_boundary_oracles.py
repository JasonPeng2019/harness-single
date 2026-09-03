"""Synthetic candidate-boundary oracles for resume and review observations.

The helpers deliberately observe only records/events a candidate exposes at its
boundary. They do not model or prescribe the candidate's implementation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _index(observations: Sequence[Mapping[str, Any]], **required: str) -> int:
    for index, observation in enumerate(observations):
        if all(observation.get(key) == value for key, value in required.items()):
            return index
    raise AssertionError(f"candidate did not expose required observation: {required!r}")


def assert_resume_order(observations: Sequence[Mapping[str, Any]]) -> None:
    """Require durable RESUMING before replacement and validation before RUNNING."""
    resuming = _index(observations, kind="lane-record-persisted", lifecycle="resuming")
    replaced = _index(observations, kind="current-run-artifacts-replaced")
    validated = _index(observations, kind="fresh-invocation-validated")
    running = _index(observations, kind="lane-record-persisted", lifecycle="running")
    if resuming >= replaced:
        raise AssertionError("RESUMING was not persisted before current-run replacement")
    if running <= validated:
        raise AssertionError("RUNNING was persisted before fresh-invocation validation")


def assert_rejected_review_resume_boundary(candidate: Any) -> None:
    """Require exactly one review-owned resume-required event across resume."""
    candidate.reject_review()
    candidate.resume()
    signals = [event for event in candidate.events if event.get("type") == "LANE_RESUME_REQUIRED"]
    if len(signals) != 1:
        raise AssertionError(f"expected one LANE_RESUME_REQUIRED event, found {len(signals)}")
    if signals[0].get("producer") != "completion-review":
        raise AssertionError("LANE_RESUME_REQUIRED must be owned by completion-review")
