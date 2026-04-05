"""Live pitcher season stats from MLB-StatsAPI (statsapi) + optional 2026 projections JSON."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import statsapi

from data.pitcher_projections import resolve_pitcher_projection
from logic.stat_weights import apply_stabilization_override, get_current_weights

PITCHER_STATS_SEASON = 2025
SEASON_PRIOR = 2025
SEASON_CURRENT = 2026

# |2025 actual K% − 2026 projection K%| at or below → High Confidence in UI.
CONFIDENCE_K_DIFF_THRESHOLD_PCT = 3.0

_LATE_SEASON_START = (4, 26)  # after April 25


@dataclass(frozen=True)
class PitcherSeasonStats:
    season: int
    strikeouts: int
    total_batters_faced: int
    games_started: int
    k_pct: float
    bf_per_start: float | None


# Backward-compatible aliases
Pitcher2025Stats = PitcherSeasonStats


def get_pitcher_stats(player_id: int, *, season: int | None = None) -> PitcherSeasonStats | None:
    """Fetch one season of pitching stats; compute ``k_pct`` and ``bf_per_start``."""
    year = PITCHER_STATS_SEASON if season is None else season
    data = statsapi.player_stat_data(
        player_id,
        group="pitching",
        type="season",
        season=year,
    )
    rows = data.get("stats") or []
    if not rows:
        return None
    raw = rows[0].get("stats") or {}
    bf = int(raw.get("battersFaced") or 0)
    so = int(raw.get("strikeOuts") or 0)
    gs = int(raw.get("gamesStarted") or 0)
    if bf <= 0:
        return None
    k_pct = (so / bf) * 100.0
    bf_per: float | None = (bf / gs) if gs > 0 else None
    return PitcherSeasonStats(
        season=year,
        strikeouts=so,
        total_batters_faced=bf,
        games_started=gs,
        k_pct=k_pct,
        bf_per_start=bf_per,
    )


def _no_2026_sample(s26: PitcherSeasonStats | None) -> bool:
    """True when the API has no usable 2026 workload (safety valve → 100% 2025)."""
    if s26 is None:
        return True
    return int(s26.total_batters_faced) <= 0


def _blend_optional(
    a: float | None,
    b: float | None,
    w_a: float,
    w_b: float,
) -> float | None:
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return w_a * a + w_b * b


def _confidence_label(k_2025_actual: float | None, k_2026_projection: float | None) -> str:
    if k_2025_actual is None or k_2026_projection is None:
        return "n/a"
    if abs(float(k_2025_actual) - float(k_2026_projection)) <= CONFIDENCE_K_DIFF_THRESHOLD_PCT:
        return "High Confidence"
    return "Volatility Warning"


def _k_six_for_blend(
    *,
    as_of: date,
    s26: PitcherSeasonStats,
    k_proj: float | None,
) -> float:
    """Value on the '2026' side of the calendar blend (projection early, actual after Apr 25)."""
    md = (as_of.month, as_of.day)
    if md >= _LATE_SEASON_START:
        return float(s26.k_pct)
    if k_proj is not None:
        return float(k_proj)
    return float(s26.k_pct)


def _bf_per_start_dynamic(
    *,
    s25: PitcherSeasonStats | None,
    s26: PitcherSeasonStats | None,
    gs_2026: int,
    w25: float,
    w26: float,
    bf_six: float | None,
) -> tuple[float | None, str]:
    """Workload: 2026 GS≥1 → actual 2026 BF/start; else calendar blend (2025 vs projection arm)."""
    if (
        s26 is not None
        and gs_2026 >= 1
        and s26.bf_per_start is not None
    ):
        return (
            float(s26.bf_per_start),
            "BF/start: **2026 actual** (≥1 GS — dynamic leash baseline).",
        )
    blended = _blend_optional(
        s25.bf_per_start if s25 is not None else None,
        bf_six,
        w25,
        w26,
    )
    return (
        blended,
        "BF/start: **blended** 2025 actual × projection/2026 arm (no 2026 start yet — no haircut).",
    )


@dataclass(frozen=True)
class WeightedPitcherStats:
    """Blended pitcher inputs for projection (K% and BF/start)."""

    k_pct: float
    bf_per_start: float | None
    gs_2026: int
    bf_2026: int
    weight_2025: float
    weight_2026: float
    blend_summary: str
    model_status_label: str
    stats_2025: PitcherSeasonStats | None
    stats_2026: PitcherSeasonStats | None
    k_pct_2025_actual: float | None
    k_pct_2026_projection: float | None
    confidence_label: str


def get_weighted_stats(
    player_id: int,
    *,
    as_of: date | None = None,
) -> WeightedPitcherStats | None:
    """Blend 2025 actuals with 2026 projections / actuals using calendar + stabilization rules.

    * Calendar weights from :func:`logic.stat_weights.get_current_weights`.
    * If 2026 BF < 50, pin **80% / 20%** (stabilization override).
    * If there is no 2026 sample (no API row or zero activity), **100%** 2025.
    * **BF/start:** if pitcher has **≥1 GS in 2026**, use **2026 actual** BF/start; else calendar
      blend of 2025 vs projection/2026 arm (no global volume haircut).
    """
    today = as_of or date.today()
    pid = int(player_id)
    s25 = get_pitcher_stats(pid, season=SEASON_PRIOR)
    s26 = get_pitcher_stats(pid, season=SEASON_CURRENT)
    proj = resolve_pitcher_projection(pid)
    k_proj = proj.k_pct if proj is not None else None

    if _no_2026_sample(s26):
        if s25 is None:
            return None
        # When the API has no usable 2026 workload yet, we still have a
        # preseason workload projection (CSV/JSON). BF/start from pure 2025
        # can look stale for pitchers whose role/usage has changed.
        bf_proj = proj.bf_per_start if proj is not None else None
        bf_out = bf_proj if bf_proj is not None else s25.bf_per_start
        return WeightedPitcherStats(
            k_pct=float(s25.k_pct),
            bf_per_start=bf_out,
            gs_2026=0,
            bf_2026=0,
            weight_2025=1.0,
            weight_2026=0.0,
            blend_summary=(
                "No 2026 sample — **100%** 2025 K% (safety valve). "
                "BF/start uses 2026 projection when available (fallback: 2025)."
            ),
            model_status_label="2026 unavailable — 100% 2025",
            stats_2025=s25,
            stats_2026=None,
            k_pct_2025_actual=float(s25.k_pct),
            k_pct_2026_projection=k_proj,
            confidence_label=_confidence_label(float(s25.k_pct), k_proj),
        )

    assert s26 is not None
    gs_2026 = int(s26.games_started)
    bf_2026 = int(s26.total_batters_faced)

    w25_cal, w26_cal, base_lbl = get_current_weights(today)
    w25, w26, model_status = apply_stabilization_override(
        w25_cal,
        w26_cal,
        bf_2026=bf_2026,
        base_label=base_lbl,
    )

    bf_six = proj.bf_per_start if (proj and proj.bf_per_start is not None) else s26.bf_per_start

    if s25 is None:
        k_six = _k_six_for_blend(as_of=today, s26=s26, k_proj=k_proj)
        bf_out, bf_note = _bf_per_start_dynamic(
            s25=None,
            s26=s26,
            gs_2026=gs_2026,
            w25=0.0,
            w26=1.0,
            bf_six=bf_six,
        )
        if bf_out is None:
            bf_out = bf_six
        return WeightedPitcherStats(
            k_pct=float(k_six),
            bf_per_start=bf_out,
            gs_2026=gs_2026,
            bf_2026=bf_2026,
            weight_2025=0.0,
            weight_2026=1.0,
            blend_summary=f"No 2025 API stats — **100%** 2026 inputs. {bf_note}",
            model_status_label="Late Season 100% 2026"
            if (today.month, today.day) >= _LATE_SEASON_START
            else "No 2025 actuals — 100% 2026",
            stats_2025=None,
            stats_2026=s26,
            k_pct_2025_actual=None,
            k_pct_2026_projection=k_proj,
            confidence_label=_confidence_label(None, k_proj),
        )

    k_six = _k_six_for_blend(as_of=today, s26=s26, k_proj=k_proj)
    k_blended = _blend_optional(s25.k_pct, k_six, w25, w26)
    assert k_blended is not None

    bf_out, bf_note = _bf_per_start_dynamic(
        s25=s25,
        s26=s26,
        gs_2026=gs_2026,
        w25=w25,
        w26=w26,
        bf_six=bf_six,
    )

    arm = "2026 actual K%" if (today.month, today.day) >= _LATE_SEASON_START else (
        "2026 projection K%" if k_proj is not None else "2026 actual K%"
    )
    blend_summary = (
        f"Calendar blend **{int(round(w25 * 100))}%** / **{int(round(w26 * 100))}%** "
        f"({SEASON_PRIOR} actual vs {arm}). {bf_note}"
    )

    return WeightedPitcherStats(
        k_pct=float(k_blended),
        bf_per_start=bf_out,
        gs_2026=gs_2026,
        bf_2026=bf_2026,
        weight_2025=w25,
        weight_2026=w26,
        blend_summary=blend_summary,
        model_status_label=model_status,
        stats_2025=s25,
        stats_2026=s26,
        k_pct_2025_actual=float(s25.k_pct),
        k_pct_2026_projection=k_proj,
        confidence_label=_confidence_label(float(s25.k_pct), k_proj),
    )


# Backward-compatible name
get_pitcher_2025_stats = get_pitcher_stats
