"""MLB schedule and team-id helpers via MLB-StatsAPI (statsapi).

One /teams fetch builds maps used with TEAM_K_RATES (full club names) and abbreviations, e.g.:
  147 -> "New York Yankees" -> TEAM_K_RATES key; abbrev "NYY"
  137 -> "San Francisco Giants"; abbrev "SF"
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

import statsapi

from data.mlb_teams import MLB_TEAMS

# Schedule uses "Athletics"; app dropdown uses "Oakland Athletics".
_API_NAME_TO_CANONICAL: dict[str, str] = {"Athletics": "Oakland Athletics"}

UPCOMING_GAME_STATUSES = frozenset({"Scheduled", "Pre-Game"})

_canonical_to_id: dict[str, int] | None = None
_id_to_canonical: dict[int, str] | None = None
_id_to_abbrev: dict[int, str] | None = None


@dataclass(frozen=True)
class ScheduledMatchup:
    """Next Scheduled/Pre-Game matchup from team schedule search."""

    opponent_canonical: str
    game_date_iso: str
    game_date_display: str
    game_pk: int | None
    """MLB ``game_id`` for statsapi box score / tracker (upcoming game)."""
    probable_starter: str | None
    """Probable pitcher name for this team from the schedule entry, if any."""
    pitcher_is_probable: bool
    """True iff `pitcher_full_name` matches the listed probable starter."""
    probable_not_listed: bool
    """True if API has no probable starter string yet (TBD)."""


def _normalize_person_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _ensure_team_maps() -> None:
    global _canonical_to_id, _id_to_canonical, _id_to_abbrev
    if _canonical_to_id is not None:
        return
    raw = statsapi.get("teams", {"sportId": 1})["teams"]
    c2i: dict[str, int] = {}
    i2c: dict[int, str] = {}
    i2a: dict[int, str] = {}
    for row in raw:
        api_name = row["name"]
        canonical = _API_NAME_TO_CANONICAL.get(api_name, api_name)
        if canonical not in MLB_TEAMS:
            continue
        tid = row["id"]
        c2i[canonical] = tid
        i2c[tid] = canonical
        i2a[tid] = row["abbreviation"]
    missing = [t for t in MLB_TEAMS if t not in c2i]
    if missing:
        raise RuntimeError(f"Could not resolve MLB API ids for: {missing}")
    _canonical_to_id = c2i
    _id_to_canonical = i2c
    _id_to_abbrev = i2a


def _get_canonical_to_id() -> dict[str, int]:
    _ensure_team_maps()
    assert _canonical_to_id is not None
    return _canonical_to_id


def get_canonical_team_id_map() -> dict[str, int]:
    """``MLB_TEAMS`` club name -> MLB API team id (same keys as the app dropdown)."""
    return dict(_get_canonical_to_id())


def get_id_to_canonical() -> dict[int, str]:
    """MLB API team id -> TEAM_K_RATES / dropdown key (full club name)."""
    _ensure_team_maps()
    assert _id_to_canonical is not None
    return dict(_id_to_canonical)


def get_id_to_abbrev() -> dict[int, str]:
    """MLB API team id -> standard abbreviation (e.g. 147 -> NYY, 137 -> SF)."""
    _ensure_team_maps()
    assert _id_to_abbrev is not None
    return dict(_id_to_abbrev)


def canonical_team_for_id(team_id: int) -> str | None:
    _ensure_team_maps()
    assert _id_to_canonical is not None
    return _id_to_canonical.get(team_id)


def team_display_for_id(team_id: int) -> str:
    """Full name plus abbrev when available (for messages)."""
    name = canonical_team_for_id(team_id)
    if name is None:
        return f"team {team_id}"
    _ensure_team_maps()
    assert _id_to_abbrev is not None
    ab = _id_to_abbrev.get(team_id)
    return f"{name} ({ab})" if ab else name


def _schedule_name_to_canonical(name: str | None) -> str | None:
    if not name:
        return None
    if name in MLB_TEAMS:
        return name
    mapped = _API_NAME_TO_CANONICAL.get(name)
    if mapped and mapped in MLB_TEAMS:
        return mapped
    return None


def _game_date_display(iso_date: str) -> str:
    try:
        d = datetime.strptime(iso_date, "%Y-%m-%d")
        return d.strftime("%B %d, %Y")
    except ValueError:
        return iso_date


def _probable_pitcher_for_team(game: dict, team_id: int) -> str | None:
    if game.get("away_id") == team_id:
        raw = game.get("away_probable_pitcher")
    elif game.get("home_id") == team_id:
        raw = game.get("home_probable_pitcher")
    else:
        return None
    if raw is None:
        return None
    s = str(raw).strip()
    return s if s else None


def _opponent_canonical_from_game(team_id: int, game: dict) -> str | None:
    aid, hid = game.get("away_id"), game.get("home_id")
    if aid == team_id:
        return _schedule_name_to_canonical(game.get("home_name"))
    if hid == team_id:
        return _schedule_name_to_canonical(game.get("away_name"))
    return None


def find_next_scheduled_game(team_id: int) -> dict | None:
    """
    Games from today forward via statsapi.schedule(team=..., start_date=today);
    first entry with status Scheduled or Pre-Game.
    """
    today = date.today().strftime("%Y-%m-%d")
    end = (date.today() + timedelta(days=45)).strftime("%Y-%m-%d")
    games = statsapi.schedule(team=team_id, start_date=today, end_date=end) or []
    games = sorted(
        games,
        key=lambda g: (g.get("game_date") or "", g.get("game_datetime") or ""),
    )
    for g in games:
        if g.get("status") in UPCOMING_GAME_STATUSES:
            return g
    return None


def get_scheduled_matchup_for_team(
    team_id: int,
    pitcher_full_name: str | None = None,
) -> ScheduledMatchup | None:
    """
    Next Scheduled/Pre-Game opponent + probable starter info for this team.
    """
    game = find_next_scheduled_game(team_id)
    if game is None:
        return None
    opp = _opponent_canonical_from_game(team_id, game)
    if opp is None or opp not in MLB_TEAMS:
        return None
    iso = str(game.get("game_date") or "")
    probable = _probable_pitcher_for_team(game, team_id)
    probable_not_listed = probable is None
    p_is_prob = False
    if pitcher_full_name and probable:
        p_is_prob = _normalize_person_name(pitcher_full_name) == _normalize_person_name(
            probable
        )
    gpk = game.get("game_id")
    try:
        game_pk = int(gpk) if gpk is not None else None
    except (TypeError, ValueError):
        game_pk = None
    return ScheduledMatchup(
        opponent_canonical=opp,
        game_date_iso=iso,
        game_date_display=_game_date_display(iso) if iso else "",
        game_pk=game_pk,
        probable_starter=probable,
        pitcher_is_probable=p_is_prob,
        probable_not_listed=probable_not_listed,
    )


def schedule_club_name_to_canonical(name: str | None) -> str | None:
    """Map API schedule team label (e.g. ``Athletics``) to ``MLB_TEAMS`` key."""
    return _schedule_name_to_canonical(name)


def get_next_opponent_by_team_id(team_id: int) -> str | None:
    """Return canonical opponent for the next Scheduled/Pre-Game game, or None."""
    m = get_scheduled_matchup_for_team(team_id, pitcher_full_name=None)
    return m.opponent_canonical if m else None


def get_next_opponent(team_name: str) -> str | None:
    """
    Resolve a dropdown team name to MLB API id, then return the next opponent
    as a canonical MLB_TEAMS string.
    """
    team_id = _get_canonical_to_id().get(team_name)
    if team_id is None:
        return None
    return get_next_opponent_by_team_id(team_id)


def get_daily_schedule(
    d: date | None = None,
    *,
    sport_id: int = 1,
    regular_season_only: bool = True,
) -> list[dict]:
    """
    All games on a calendar day via ``statsapi.schedule(date=..., sportId=...)``.
    """
    target = (d or date.today()).strftime("%Y-%m-%d")
    games = statsapi.schedule(date=target, sportId=sport_id) or []
    if regular_season_only:
        games = [g for g in games if g.get("game_type") == "R"]
    games.sort(
        key=lambda g: (
            str(g.get("game_datetime") or ""),
            int(g.get("game_id") or 0),
        )
    )
    return games


PitcherSide = Literal["away", "home"]


def canonical_opponent_for_pitcher_side(game: dict, side: PitcherSide) -> str | None:
    """Batting team (``MLB_TEAMS`` key) faced by the away or home probable starter."""
    if side == "away":
        return _schedule_name_to_canonical(game.get("home_name"))
    return _schedule_name_to_canonical(game.get("away_name"))


def scheduled_matchup_from_game_for_team(
    game: dict,
    team_id: int,
    pitcher_full_name: str | None = None,
) -> ScheduledMatchup | None:
    """Build :class:`ScheduledMatchup` from a schedule row for a given club."""
    opp = _opponent_canonical_from_game(team_id, game)
    if opp is None or opp not in MLB_TEAMS:
        return None
    iso = str(game.get("game_date") or "")
    probable = _probable_pitcher_for_team(game, team_id)
    probable_not_listed = probable is None
    p_is_prob = False
    if pitcher_full_name and probable:
        p_is_prob = _normalize_person_name(pitcher_full_name) == _normalize_person_name(
            probable
        )
    gpk = game.get("game_id")
    try:
        game_pk = int(gpk) if gpk is not None else None
    except (TypeError, ValueError):
        game_pk = None
    return ScheduledMatchup(
        opponent_canonical=opp,
        game_date_iso=iso,
        game_date_display=_game_date_display(iso) if iso else "",
        game_pk=game_pk,
        probable_starter=probable,
        pitcher_is_probable=p_is_prob,
        probable_not_listed=probable_not_listed,
    )


@dataclass(frozen=True)
class LiveStarterPitchingLine:
    """Box score pitching stats for the starting pitcher on one side of a game."""

    player_id: int | None
    strikeouts: int
    batters_faced: int
    innings_pitched: str | None
    hits: int | None
    runs: int | None
    earned_runs: int | None
    base_on_balls: int | None


def _pit_int(pit: dict, key: str) -> int | None:
    v = pit.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _parse_live_starter_for_side(
    box: dict,
    side: str,
    *,
    person_id: int | None = None,
) -> LiveStarterPitchingLine | None:
    team = (box.get("teams") or {}).get(side) or {}
    players = team.get("players") or {}
    candidates: list[tuple[int | None, dict]] = []
    for pdata in players.values():
        pit = pdata.get("stats", {}).get("pitching", {})
        if not pit:
            continue
        if int(pit.get("gamesStarted", 0) or 0) < 1:
            continue
        raw_id = pdata.get("person", {}).get("id")
        try:
            ipid = int(raw_id) if raw_id is not None else None
        except (TypeError, ValueError):
            ipid = None
        candidates.append((ipid, pit))
    if not candidates:
        return None
    chosen_pit: dict | None = None
    chosen_pid: int | None = None
    if person_id is not None:
        for ipid, pit in candidates:
            if ipid == int(person_id):
                chosen_pit = pit
                chosen_pid = ipid
                break
    if chosen_pit is None:
        chosen_pid, chosen_pit = candidates[0]
    ip_raw = chosen_pit.get("inningsPitched")
    ip_str = str(ip_raw).strip() if ip_raw not in (None, "") else None
    return LiveStarterPitchingLine(
        player_id=chosen_pid,
        strikeouts=int(chosen_pit.get("strikeOuts", 0) or 0),
        batters_faced=int(chosen_pit.get("battersFaced", 0) or 0),
        innings_pitched=ip_str,
        hits=_pit_int(chosen_pit, "hits"),
        runs=_pit_int(chosen_pit, "runs"),
        earned_runs=_pit_int(chosen_pit, "earnedRuns"),
        base_on_balls=_pit_int(chosen_pit, "baseOnBalls"),
    )


def live_game_starter_pitching_lines(
    game_pk: int,
    *,
    away_pitcher_id: int | None = None,
    home_pitcher_id: int | None = None,
) -> tuple[LiveStarterPitchingLine | None, LiveStarterPitchingLine | None]:
    """
    Live or final box score lines for each starting pitcher (one MLB StatsAPI call).
    Optional ``*_pitcher_id`` disambiguates when multiple ``gamesStarted`` rows exist.
    """
    try:
        feed = statsapi.get("game", {"gamePk": int(game_pk)})
    except Exception:
        return None, None
    box = feed.get("liveData", {}).get("boxscore", {})
    if not box:
        return None, None
    away = _parse_live_starter_for_side(box, "away", person_id=away_pitcher_id)
    home = _parse_live_starter_for_side(box, "home", person_id=home_pitcher_id)
    return away, home
