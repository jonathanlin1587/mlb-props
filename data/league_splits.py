"""League-wide offensive K%% splits (vs LHP / vs RHP) from MLB StatsAPI.

Run once (or weekly) to refresh local JSON so the Streamlit app does not
hammer the API on every rerun::

    python data/league_splits.py
    # or
    python -c \"from data.league_splits import fetch_league_splits; fetch_league_splits()\"

API notes: ``sitCodes`` ``vl`` = plate appearances vs left-handed pitchers,
``vr`` = vs right-handed pitchers (hitting team perspective).
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import statsapi

from data.team_k_2025 import get_team_k_pct
from logic.mlb_schedule import get_canonical_team_id_map

# MLB statSplits situation codes (hitting vs opponent pitcher hand).
_SIT_VS_LHP = "vl"
_SIT_VS_RHP = "vr"

TEAM_MAP: dict[str, int] = get_canonical_team_id_map()


def league_data_json_path(season: int) -> Path:
    """Set ``MLB_DATA_DIR`` for a persistent directory (Docker volume, etc.)."""
    raw = os.environ.get("MLB_DATA_DIR", "").strip()
    base = Path(raw).expanduser().resolve() if raw else Path(__file__).resolve().parent
    return base / f"league_data_{season}.json"


def _k_pct_from_team_stats_payload(payload: dict[str, Any]) -> float | None:
    stats = payload.get("stats") or []
    if not stats:
        return None
    splits = stats[0].get("splits") or []
    if not splits:
        return None
    raw = splits[0].get("stat") or {}
    try:
        pa = int(raw.get("plateAppearances") or 0)
        so = int(raw.get("strikeOuts") or 0)
    except (TypeError, ValueError):
        return None
    if pa <= 0:
        return None
    return round(100.0 * so / pa, 3)


def _fetch_team_hitting_split(team_id: int, season: int, sit_codes: str) -> float | None:
    try:
        data = statsapi.get(
            "team_stats",
            {
                "teamId": team_id,
                "season": season,
                "group": "hitting",
                "stats": "statSplits",
                "sitCodes": sit_codes,
            },
        )
    except Exception:
        return None
    return _k_pct_from_team_stats_payload(data)


def _fetch_team_hitting_season(team_id: int, season: int) -> float | None:
    try:
        data = statsapi.get(
            "team_stats",
            {
                "teamId": team_id,
                "season": season,
                "group": "hitting",
                "stats": "season",
            },
        )
    except Exception:
        return None
    return _k_pct_from_team_stats_payload(data)


def fetch_league_splits(season: int = 2025, *, path: Path | None = None) -> Path:
    """Pull vs-LHP / vs-RHP / season K%% for every team in :data:`TEAM_MAP` and write JSON.

    Uses ``statsapi.get('team_stats', ...)`` (hitting + ``statSplits`` / ``season``).
    """
    out = path or league_data_json_path(season)
    teams_out: dict[str, Any] = {}
    for name, tid in sorted(TEAM_MAP.items(), key=lambda x: x[0]):
        vs_l = _fetch_team_hitting_split(tid, season, _SIT_VS_LHP)
        vs_r = _fetch_team_hitting_split(tid, season, _SIT_VS_RHP)
        overall = _fetch_team_hitting_season(tid, season)
        teams_out[name] = {
            "team_id": tid,
            "overall_k_pct": overall,
            "vs_lhp_k_pct": vs_l,
            "vs_rhp_k_pct": vs_r,
        }

    doc = {
        "season": season,
        "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "splits": {"vs_lhp_sit_code": _SIT_VS_LHP, "vs_rhp_sit_code": _SIT_VS_RHP},
        "teams": teams_out,
    }
    out.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return out


def load_league_data(season: int = 2025, *, path: Path | None = None) -> dict[str, Any] | None:
    """Parse league JSON if present; otherwise return None."""
    p = path or league_data_json_path(season)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _split_k_from_row(row: dict[str, Any], throws: str) -> tuple[float | None, str]:
    overall = row.get("overall_k_pct")
    vs_l = row.get("vs_lhp_k_pct")
    vs_r = row.get("vs_rhp_k_pct")
    if throws == "R" and vs_r is not None:
        return float(vs_r), "vs RHP"
    if throws == "L" and vs_l is not None:
        return float(vs_l), "vs LHP"
    if overall is not None:
        return float(overall), "overall"
    return None, "none"


def calendar_season_blend_weights(as_of: date) -> tuple[float, float, str]:
    """Calendar-based weights ``(w_2025, w_2026)`` for opponent K%% (sums to 1).

    - Before April 15: favor 2025 (90% / 10%).
    - On/after May 1: 100% 2026.
    - Apr 15–Apr 30: linear ramp from 90/10 to 0/100.
    """
    y = as_of.year
    apr15 = date(y, 4, 15)
    may1 = date(y, 5, 1)
    if as_of < apr15:
        return 0.90, 0.10, f"before {apr15:%b %d}: **90%** 2025 / **10%** 2026"
    if as_of >= may1:
        return 0.0, 1.0, f"on/after {may1:%b %d}: **100%** 2026"
    span = (may1 - apr15).days
    if span <= 0:
        return 0.0, 1.0, "calendar blend → **100%** 2026"
    t = (as_of - apr15).days / float(span)
    t = min(1.0, max(0.0, t))
    w26 = 0.1 + 0.9 * t
    w25 = 1.0 - w26
    return w25, w26, f"Apr 15–May 1 ramp: **{w25:.0%}** 2025 / **{w26:.0%}** 2026"


def opponent_k_pct_for_pitcher_hand(
    opponent_team: str,
    pitcher_throws: str | None,
    *,
    season: int = 2025,
    league_doc: dict[str, Any] | None = None,
) -> tuple[float | None, str]:
    """Opponent offensive K%% for projection input (single season / legacy).

    - RHP → opponent ``vs_rhp_k_pct`` (hitters facing right-handed pitching).
    - LHP → ``vs_lhp_k_pct``.
    - Unknown pitcher hand → ``overall_k_pct`` from JSON, else :func:`data.team_k_2025.get_team_k_pct`.
    """
    doc = league_doc if league_doc is not None else load_league_data(season)
    throws = (pitcher_throws or "").strip().upper()

    if doc:
        row = (doc.get("teams") or {}).get(opponent_team)
        if isinstance(row, dict):
            k, kind = _split_k_from_row(row, throws)
            if k is not None:
                return k, f"{kind} (league JSON {season})"
    fb = get_team_k_pct(opponent_team)
    label = "overall (team_k_2025 fallback)"
    if throws in ("L", "R") and doc:
        label += " — run fetch_league_splits() for platoon splits"
    return fb, label


def opponent_k_pct_blended(
    opponent_team: str,
    pitcher_throws: str | None,
    *,
    as_of: date,
    league_doc_2025: dict[str, Any] | None = None,
    league_doc_2026: dict[str, Any] | None = None,
) -> tuple[float | None, str, str]:
    """Blended opponent K%% using calendar weights between 2025 and 2026 league JSON.

    Returns ``(k_pct, split_label, calendar_blend_md)``.
    """
    throws = (pitcher_throws or "").strip().upper()
    w25, w26, cal_label = calendar_season_blend_weights(as_of)

    d25 = league_doc_2025 if league_doc_2025 is not None else load_league_data(2025)
    d26 = league_doc_2026 if league_doc_2026 is not None else load_league_data(2026)

    v25: float | None = None
    v26: float | None = None
    split_note = "overall"

    for doc, target in ((d25, "v25"), (d26, "v26")):
        if not doc:
            continue
        row = (doc.get("teams") or {}).get(opponent_team)
        if not isinstance(row, dict):
            continue
        k, kind = _split_k_from_row(row, throws)
        if k is not None:
            split_note = kind
            if target == "v25":
                v25 = k
            else:
                v26 = k

    if v25 is not None and v26 is not None:
        blended = w25 * v25 + w26 * v26
        return blended, split_note, cal_label
    if v26 is not None:
        return v26, split_note, cal_label + " (2025 JSON missing — **100%** 2026 sample)"
    if v25 is not None:
        return v25, split_note, cal_label + " (2026 JSON missing — **100%** 2025 sample)"

    fb = get_team_k_pct(opponent_team)
    return fb, "overall", "no league JSON — **team_k_2025** table only"


if __name__ == "__main__":
    p25 = fetch_league_splits(2025)
    p26 = fetch_league_splits(2026)
    print("Wrote", p25, "and", p26)
