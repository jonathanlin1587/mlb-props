"""Calendar-based weights for blending prior-season actuals vs current-season inputs."""

from __future__ import annotations

from datetime import date

# Month/day thresholds (year-agnostic; interpreted in the active baseball calendar).
_EARLY_END = (4, 9)  # before April 10
_MID_END = (4, 25)  # April 10–25 inclusive

STABILIZATION_BF_2026 = 50


def get_current_weights(as_of: date | None = None) -> tuple[float, float, str]:
    """Return ``(w_2025, w_2026, status_label)`` for stat blending.

    - Before April 10: 80% / 20%
    - April 10 through April 25: 40% / 60%
    - After April 25: 0% / 100%

    ``status_label`` is meant for UI badges (e.g. "Early Season Blend 80/20").
    """
    d = as_of or date.today()
    md = (d.month, d.day)
    if md <= _EARLY_END:
        return 0.8, 0.2, "Early Season Blend 80/20"
    if md <= _MID_END:
        return 0.4, 0.6, "Mid April Blend 40/60"
    return 0.0, 1.0, "Late Season 100% 2026"


def apply_stabilization_override(
    w25: float,
    w26: float,
    *,
    bf_2026: int,
    base_label: str,
) -> tuple[float, float, str]:
    """If 2026 sample is tiny (< ``STABILIZATION_BF_2026`` BF), pin 80/20 regardless of date."""
    if bf_2026 >= STABILIZATION_BF_2026:
        return w25, w26, base_label
    return (
        0.8,
        0.2,
        f"{base_label} · Stabilization <{STABILIZATION_BF_2026} BF → 80/20",
    )
