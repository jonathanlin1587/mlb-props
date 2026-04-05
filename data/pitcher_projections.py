"""Pitcher projections: optional JSON overrides + ``2026 Projections.csv`` (Steamer/ZiPS-style)."""

from __future__ import annotations

import csv
import json
import os
import unicodedata
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path

import statsapi

_PKG_DATA = Path(__file__).resolve().parent


def _optional_data_dir() -> Path | None:
    raw = os.environ.get("MLB_DATA_DIR", "").strip()
    return Path(raw).expanduser().resolve() if raw else None


def _default_json_path() -> Path:
    od = _optional_data_dir()
    return (od / "pitcher_projections_2026.json") if od else _PKG_DATA / "pitcher_projections_2026.json"


def _default_csv_path() -> Path:
    od = _optional_data_dir()
    return (od / "2026 Projections.csv") if od else _PKG_DATA / "2026 Projections.csv"

# Implied batters faced per inning when deriving K% from K/9 (typical SP range ~4.2–4.35).
_BF_PER_INNING_FROM_K9 = 4.28

# Instead of deriving BF/start from the CSV's IP/GS (which can be noisy and
# produce extreme BF/start for some pitchers), use a league-average innings
# per start and convert via BF/inning.
#
# This keeps the K% signal (from K/9) pitcher-specific while normalizing
# workload (BF/start) to a single representative starter usage.
_AVG_IP_PER_START_FOR_BF = 5.5

# CSV team codes that differ from MLB Stats API ``abbreviation``.
_CSV_TEAM_TO_API: dict[str, str] = {
    "ARI": "AZ",
    "ATH": "ATH",
    "CHW": "CWS",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WSN": "WSH",
}


@dataclass(frozen=True)
class PitcherProjection2026:
    """Preseason or roto projections for a pitcher."""

    k_pct: float | None
    bf_per_start: float | None


def _strip_bom(s: str) -> str:
    return str(s).replace("\ufeff", "").strip()


def _fold_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", s.strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


def _api_abbrev_from_csv_team(csv_team: str) -> str:
    t = _strip_bom(csv_team).upper()
    return _CSV_TEAM_TO_API.get(t, t)


def _parse_ip(ip_raw: str) -> float:
    """Baseball string innings, e.g. ``199.2`` → 199⅔."""
    s = _strip_bom(ip_raw)
    if not s:
        return 0.0
    if "." not in s:
        return float(s)
    whole, frac = s.split(".", 1)
    w = int(whole) if whole else 0
    if not frac:
        return float(w)
    thirds = int(frac[0])
    return float(w) + thirds / 3.0


def _row_to_projection(row: dict[str, str]) -> PitcherProjection2026 | None:
    try:
        k9 = float(_strip_bom(row.get("K/9") or ""))
        gs = int(float(_strip_bom(row.get("GS") or "0")))
        ip = _parse_ip(row.get("IP") or "0")
    except (TypeError, ValueError):
        return None
    if ip <= 0 or gs <= 0:
        return None
    # K% consistent with K/9 and assumed BF/inning.
    k_pct = 100.0 * k9 / (9.0 * _BF_PER_INNING_FROM_K9)
    # BF/start derived from a constant average IP/start.
    bf_per_start = float(_AVG_IP_PER_START_FOR_BF) * _BF_PER_INNING_FROM_K9
    return PitcherProjection2026(k_pct=k_pct, bf_per_start=float(bf_per_start))


def _clamp_bf_per_start(bf_per_start: float | None) -> float | None:
    """Clamp projection-derived BF/start to a conservative maximum.

    Kept as a safety valve for JSON overrides (or other sources) that might
    provide extreme BF/start values.
    """
    if bf_per_start is None:
        return None
    # Conservative maximum: allow up to ~6.5 IP/start worth of BF.
    max_bf = 6.5 * float(_BF_PER_INNING_FROM_K9)
    return min(float(bf_per_start), max_bf)


def _normalize_csv_row_keys(row: dict[str, str]) -> dict[str, str]:
    return {_strip_bom(k): v for k, v in row.items()}


@lru_cache(maxsize=8)
def _csv_projection_tables(csv_mtime_ns: int) -> tuple[
    dict[tuple[str, str], PitcherProjection2026],
    dict[str, tuple[PitcherProjection2026, str]],
]:
    """( (name, api_abbrev) -> proj, name_only -> (proj, team) for singles )."""
    path = _default_csv_path()
    by_pair: dict[tuple[str, str], PitcherProjection2026] = {}
    name_groups: dict[str, list[tuple[str, PitcherProjection2026]]] = {}
    if not path.is_file():
        return {}, {}
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return {}, {}
    reader = csv.DictReader(text.splitlines())
    for raw in reader:
        row = _normalize_csv_row_keys(raw)
        name = _strip_bom(row.get("Name") or "")
        team_csv = _strip_bom(row.get("Team") or "")
        if not name:
            continue
        proj = _row_to_projection(row)
        if proj is None:
            continue
        key_name = _fold_name(name)
        api_ab = _api_abbrev_from_csv_team(team_csv)
        by_pair[(key_name, api_ab)] = proj
        name_groups.setdefault(key_name, []).append((api_ab, proj))
    singles: dict[str, tuple[PitcherProjection2026, str]] = {}
    for nm, lst in name_groups.items():
        if len(lst) == 1:
            singles[nm] = (lst[0][1], lst[0][0])
    return by_pair, singles


def _current_csv_mtime_ns() -> int:
    p = _default_csv_path()
    if not p.is_file():
        return 0
    try:
        return p.stat().st_mtime_ns
    except OSError:
        return 0


def _json_path_resolved(path: Path | None) -> Path:
    return path or _default_json_path()


@lru_cache(maxsize=16)
def _load_json_map_cached(path_str: str, mtime_ns: int) -> dict[int, PitcherProjection2026]:
    p = Path(path_str)
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[int, PitcherProjection2026] = {}
    for key, val in raw.items():
        try:
            pid = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(val, dict):
            continue
        k_raw = val.get("k_pct")
        bf_raw = val.get("bf_per_start")
        k_pct = float(k_raw) if k_raw is not None else None
        bf_ps = float(bf_raw) if bf_raw is not None else None
        out[pid] = PitcherProjection2026(k_pct=k_pct, bf_per_start=bf_ps)
    return out


def _load_json_map(path: Path | None) -> dict[int, PitcherProjection2026]:
    p = _json_path_resolved(path)
    try:
        mt = p.stat().st_mtime_ns if p.is_file() else -1
    except OSError:
        mt = -1
    return dict(_load_json_map_cached(str(p.resolve()), mt))


def load_pitcher_projections_map(path: Path | None = None) -> dict[int, PitcherProjection2026]:
    """JSON-only map ``player_id -> projection`` (manual overrides).

    For CSV-backed projections keyed by MLBAM id, use :func:`resolve_pitcher_projection`.
    """
    return _load_json_map(path)


@lru_cache(maxsize=4096)
def _player_fold_name_and_abbrev(player_id: int) -> tuple[str, str] | None:
    try:
        data = statsapi.get("people", {"personIds": int(player_id)})
    except Exception:
        return None
    people = data.get("people") or []
    if not people:
        return None
    p = people[0]
    name = str(p.get("fullName") or "").strip()
    if not name:
        return None
    key_name = _fold_name(name)
    team = p.get("currentTeam") or {}
    tid = team.get("id")
    if tid is None:
        return (key_name, "")
    try:
        from logic.mlb_schedule import get_id_to_abbrev

        ab = str(get_id_to_abbrev().get(int(tid), "") or "")
    except Exception:
        ab = ""
    return (key_name, ab)


def resolve_pitcher_projection(
    player_id: int,
    *,
    json_path: Path | None = None,
) -> PitcherProjection2026 | None:
    """Resolve projection: optional JSON per field overrides CSV (``2026 Projections.csv``)."""
    pid = int(player_id)
    jm = _load_json_map(json_path)
    jp = jm.get(pid)

    csvp: PitcherProjection2026 | None = None
    mtime = _current_csv_mtime_ns()
    by_pair, singles = _csv_projection_tables(mtime)
    if by_pair or singles:
        idinfo = _player_fold_name_and_abbrev(pid)
        if idinfo is not None:
            key_name, api_ab = idinfo
            csvp = by_pair.get((key_name, api_ab))
            if csvp is None and key_name in singles:
                proj, team = singles[key_name]
                if not api_ab or team == api_ab:
                    csvp = proj

    if jp is not None and csvp is not None:
        return PitcherProjection2026(
            k_pct=jp.k_pct if jp.k_pct is not None else csvp.k_pct,
            bf_per_start=_clamp_bf_per_start(
                jp.bf_per_start
                if jp.bf_per_start is not None
                else csvp.bf_per_start
            ),
        )
    if jp is not None and (jp.k_pct is not None or jp.bf_per_start is not None):
        return PitcherProjection2026(
            k_pct=jp.k_pct,
            bf_per_start=_clamp_bf_per_start(jp.bf_per_start),
        )
    return PitcherProjection2026(
        k_pct=csvp.k_pct if csvp is not None else None,
        bf_per_start=_clamp_bf_per_start(csvp.bf_per_start if csvp is not None else None),
    )


def projection_for_player(
    player_id: int,
    path: Path | None = None,
) -> PitcherProjection2026 | None:
    return resolve_pitcher_projection(player_id, json_path=path)
