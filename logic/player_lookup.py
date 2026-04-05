"""Player search via MLB-StatsAPI."""

from __future__ import annotations

import statsapi


def lookup_player(name: str) -> list[dict]:
    """Raw statsapi.lookup_player result (may include hitters and inactive)."""
    name = name.strip()
    if not name:
        return []
    return statsapi.lookup_player(name) or []


def filter_pitchers(players: list[dict]) -> list[dict]:
    return [
        p
        for p in players
        if p.get("primaryPosition", {}).get("abbreviation") == "P"
    ]


def pitcher_team_id(player: dict) -> int | None:
    ct = player.get("currentTeam")
    if not ct:
        return None
    tid = ct.get("id")
    return int(tid) if tid is not None else None


def format_player_option(p: dict) -> str:
    tid = pitcher_team_id(p)
    team_bit = f" — team id {tid}" if tid is not None else " — no team"
    return f"{p.get('fullName', '?')} ({p.get('id')}){team_bit}"


def get_pitcher_throws(player_id: int) -> str | None:
    """Return ``\"L\"`` or ``\"R\"`` from MLB ``people`` endpoint, or None."""
    data = statsapi.get("people", {"personIds": player_id})
    people = data.get("people") or []
    if not people:
        return None
    ph = people[0].get("pitchHand") or {}
    code = str(ph.get("code") or "").strip().upper()
    if code in ("L", "R"):
        return code
    return None
