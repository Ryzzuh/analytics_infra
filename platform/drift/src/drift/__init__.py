"""Schema-drift detection (SPEC.md §7)."""

from .detect import DriftFinding, detect_drift, observe_shapes, required_fields

__all__ = ["DriftFinding", "detect_drift", "observe_shapes", "required_fields"]
