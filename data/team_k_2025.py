"""2025 team offensive K% (source table used MLB abbreviations; mapped to dropdown names)."""

from __future__ import annotations

from data.mlb_teams import MLB_TEAMS

# Values from user-provided 2025 team K% table.
TEAM_K_RATES: dict[str, float] = {
    "Arizona Diamondbacks": 21.20,
    "Atlanta Braves": 22.20,
    "Baltimore Orioles": 24.20,
    "Boston Red Sox": 22.90,
    "Chicago Cubs": 21.10,
    "Chicago White Sox": 22.80,
    "Cincinnati Reds": 23.40,
    "Cleveland Guardians": 22.60,
    "Colorado Rockies": 25.90,
    "Detroit Tigers": 24.00,
    "Houston Astros": 21.40,
    "Kansas City Royals": 18.30,
    "Los Angeles Angels": 27.10,
    "Los Angeles Dodgers": 22.10,
    "Miami Marlins": 20.30,
    "Milwaukee Brewers": 20.50,
    "Minnesota Twins": 22.60,
    "New York Mets": 21.50,
    "New York Yankees": 23.60,
    "Oakland Athletics": 22.90,
    "Philadelphia Phillies": 21.70,
    "Pittsburgh Pirates": 23.70,
    "San Diego Padres": 19.10,
    "San Francisco Giants": 22.70,
    "Seattle Mariners": 23.60,
    "St. Louis Cardinals": 21.80,
    "Tampa Bay Rays": 23.10,
    "Texas Rangers": 22.00,
    "Toronto Blue Jays": 17.70,
    "Washington Nationals": 22.60,
}

# Backward-compatible alias
TEAM_K_PCT_2025: dict[str, float] = TEAM_K_RATES


def _assert_coverage() -> None:
    missing = [t for t in MLB_TEAMS if t not in TEAM_K_RATES]
    extra = [t for t in TEAM_K_RATES if t not in MLB_TEAMS]
    if missing or extra:
        raise ValueError(f"TEAM_K_RATES out of sync with MLB_TEAMS: missing={missing!r} extra={extra!r}")


_assert_coverage()


def get_team_k_pct(team: str) -> float | None:
    """Return configured K% for team name, or None if unknown."""
    return TEAM_K_RATES.get(team)
