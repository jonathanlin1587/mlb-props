"""
The Odds API integration for MLB pitcher strikeout props.

Quota-aware: all per-event calls are batched once per day and cached to
daily_best_odds.json.  The Refresh button forces a new fetch.

Endpoints used:
  GET /v4/sports/baseball_mlb/events                        (1 call)
  GET /v4/sports/baseball_mlb/events/{id}/odds?markets=...  (1 call per game)
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ODDS_API_KEY: str = os.environ.get("ODDS_API_KEY", "").strip()
ODDS_API_BASE = "https://api.the-odds-api.com"


def _odds_cache_path() -> Path:
    """Set ``MLB_ODDS_CACHE_PATH`` for persistent cache (Docker volume)."""
    raw = os.environ.get("MLB_ODDS_CACHE_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path(__file__).resolve().parent.parent / "daily_best_odds.json"

REQUEST_TIMEOUT = 15  # seconds per call
INTER_REQUEST_SLEEP = 0.15  # small throttle between per-event calls

# Only surface odds from these two books everywhere in the UI.
TARGET_BOOKMAKERS = {"draftkings", "fanduel"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BookOdds:
    """One bookmaker's line and American-odds prices for a single pitcher."""
    bookmaker: str
    line: float
    over_price: int    # American odds (e.g. -115)
    under_price: int   # American odds (e.g. +100)


@dataclass
class PitcherOddsEntry:
    """All bookmaker offers for one pitcher on one event."""
    pitcher_name: str
    event_id: str
    home_team: str
    away_team: str
    books: list[BookOdds] = field(default_factory=list)


@dataclass
class BestOddsResult:
    """The single best book/line/side combo given a Poisson projection."""
    bookmaker: str
    line: float
    over_price: int
    under_price: int
    side: str          # "over" or "under"
    kelly_edge_pct: float
    all_books: list[BookOdds]


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def fetch_mlb_odds_data(*, force_refresh: bool = False) -> dict[str, PitcherOddsEntry]:
    """
    Return pitcher strikeout odds keyed by lower-cased pitcher name.

    Uses ``daily_best_odds.json`` as a same-day cache to protect the
    500-call/month quota.  Pass ``force_refresh=True`` to bypass the cache.
    """
    if not force_refresh:
        cached = _load_cache()
        if cached is not None and _is_cache_valid(cached):
            return _deserialize(cached)

    # --- Step 1: pull today's event list --------------------------------
    events_resp = requests.get(
        f"{ODDS_API_BASE}/v4/sports/baseball_mlb/events",
        params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"},
        timeout=REQUEST_TIMEOUT,
    )
    events_resp.raise_for_status()
    events: list[dict] = events_resp.json()

    pitchers: dict[str, PitcherOddsEntry] = {}

    # --- Step 2: per-event pitcher_strikeouts odds ----------------------
    for event in events:
        event_id = event.get("id", "")
        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")
        if not event_id:
            continue

        try:
            resp = requests.get(
                f"{ODDS_API_BASE}/v4/sports/baseball_mlb/events/{event_id}/odds",
                params={
                    "apiKey": ODDS_API_KEY,
                    "markets": "pitcher_strikeouts",
                    "regions": "us",
                    "oddsFormat": "american",
                },
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 404:
                continue  # market not posted yet for this game
            resp.raise_for_status()
            odds_data: dict = resp.json()
        except requests.RequestException:
            continue

        _parse_event_odds(odds_data, event_id, home_team, away_team, pitchers)
        time.sleep(INTER_REQUEST_SLEEP)

    _save_cache(_serialize(pitchers))
    return pitchers


def _parse_event_odds(
    odds_data: dict,
    event_id: str,
    home_team: str,
    away_team: str,
    out: dict[str, PitcherOddsEntry],
) -> None:
    """Extract per-pitcher BookOdds from one event's API response."""
    for bookmaker in odds_data.get("bookmakers", []):
        bk_key = str(bookmaker.get("key", ""))
        if bk_key not in TARGET_BOOKMAKERS:
            continue
        for market in bookmaker.get("markets", []):
            if market.get("key") != "pitcher_strikeouts":
                continue

            # Group Over/Under outcomes by pitcher name (description field).
            pitcher_rows: dict[str, dict] = {}
            for outcome in market.get("outcomes", []):
                pname = str(outcome.get("description", "")).strip()
                side = str(outcome.get("name", ""))   # "Over" or "Under"
                point = float(outcome.get("point") or 0.0)
                price = int(outcome.get("price") or 0)
                if not pname:
                    continue
                if pname not in pitcher_rows:
                    pitcher_rows[pname] = {"line": point, "over": 0, "under": 0}
                if side == "Over":
                    pitcher_rows[pname]["line"] = point
                    pitcher_rows[pname]["over"] = price
                elif side == "Under":
                    pitcher_rows[pname]["under"] = price

            for pname, row in pitcher_rows.items():
                key = pname.lower()
                if key not in out:
                    out[key] = PitcherOddsEntry(
                        pitcher_name=pname,
                        event_id=event_id,
                        home_team=home_team,
                        away_team=away_team,
                    )
                out[key].books.append(
                    BookOdds(
                        bookmaker=bk_key,
                        line=row["line"],
                        over_price=row["over"],
                        under_price=row["under"],
                    )
                )


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def lookup_pitcher_in_odds(
    name: str,
    odds_data: dict[str, PitcherOddsEntry],
) -> Optional[PitcherOddsEntry]:
    """
    Find a pitcher in the odds dict.  Tries exact lower-case match first,
    then falls back to last-name-only match.
    """
    if not name or not odds_data:
        return None
    key = name.strip().lower()
    if key in odds_data:
        return odds_data[key]
    # Last-name fallback
    last = key.split()[-1] if key.split() else ""
    for k, v in odds_data.items():
        if k.split()[-1] == last if k.split() else False:
            return v
    return None


def find_best_kelly_odds(
    entry: PitcherOddsEntry,
    projection: float,
    *,
    side: str = "over",
) -> Optional[BestOddsResult]:
    """
    Given a Poisson projection λ, scan all bookmakers for this pitcher and
    return the book/line combo with the highest Kelly Criterion edge.

    ``side`` should be ``"over"`` or ``"under"``.
    """
    # Import here to avoid circular dependency (odds_api -> projection -> odds_api)
    from logic.projection import (
        american_odds_to_implied_probability_pct,
        poisson_over_probability_pct,
        poisson_under_probability_pct,
    )

    if not entry.books:
        return None

    best: Optional[BestOddsResult] = None
    best_edge = float("-inf")

    for book in entry.books:
        price = book.over_price if side == "over" else book.under_price
        if price == 0:
            continue
        try:
            implied_pct = american_odds_to_implied_probability_pct(price)
        except ValueError:
            continue

        if side == "over":
            our_pct = poisson_over_probability_pct(projection, book.line)
        else:
            our_pct = poisson_under_probability_pct(projection, book.line)

        edge = our_pct - implied_pct

        if edge > best_edge:
            best_edge = edge
            best = BestOddsResult(
                bookmaker=book.bookmaker,
                line=book.line,
                over_price=book.over_price,
                under_price=book.under_price,
                side=side,
                kelly_edge_pct=edge,
                all_books=entry.books,
            )

    return best


def best_over_price_book(entry: PitcherOddsEntry) -> Optional[BookOdds]:
    """
    Return the bookmaker offering the highest (best) American Over price.
    Useful for a quick tile preview before a projection is available.
    """
    valid = [b for b in entry.books if b.over_price != 0]
    if not valid:
        return None
    return max(valid, key=lambda b: _american_to_decimal(b.over_price))


def modal_line(entry: PitcherOddsEntry) -> Optional[float]:
    """Return the most common line across bookmakers (simple mode)."""
    lines = [b.line for b in entry.books if b.line > 0]
    if not lines:
        return None
    return max(set(lines), key=lines.count)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def get_cached_fetch_time() -> Optional[str]:
    """Return a human-readable fetch timestamp, or None if no cache."""
    data = _load_cache()
    if data is None:
        return None
    raw = data.get("fetched_at")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%b %d %Y %I:%M %p")
    except ValueError:
        return raw


def _load_cache() -> Optional[dict]:
    p = _odds_cache_path()
    if not p.exists():
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _save_cache(data: dict) -> None:
    try:
        p = _odds_cache_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass  # non-fatal; cache just won't persist


def _is_cache_valid(cache: dict) -> bool:
    try:
        return cache.get("date", "") == date.today().isoformat()
    except Exception:
        return False


def _serialize(pitchers: dict[str, PitcherOddsEntry]) -> dict:
    return {
        "fetched_at": datetime.now().isoformat(),
        "date": date.today().isoformat(),
        "pitchers": {
            k: {
                "pitcher_name": v.pitcher_name,
                "event_id": v.event_id,
                "home_team": v.home_team,
                "away_team": v.away_team,
                "books": [
                    {
                        "bookmaker": b.bookmaker,
                        "line": b.line,
                        "over_price": b.over_price,
                        "under_price": b.under_price,
                    }
                    for b in v.books
                ],
            }
            for k, v in pitchers.items()
        },
    }


def _deserialize(cache: dict) -> dict[str, PitcherOddsEntry]:
    result: dict[str, PitcherOddsEntry] = {}
    for k, v in cache.get("pitchers", {}).items():
        books = [
            BookOdds(
                bookmaker=b["bookmaker"],
                line=float(b["line"]),
                over_price=int(b.get("over_price", 0)),
                under_price=int(b.get("under_price", 0)),
            )
            for b in v.get("books", [])
            if b.get("bookmaker") in TARGET_BOOKMAKERS
        ]
        result[k] = PitcherOddsEntry(
            pitcher_name=v["pitcher_name"],
            event_id=v.get("event_id", ""),
            home_team=v.get("home_team", ""),
            away_team=v.get("away_team", ""),
            books=books,
        )
    return result


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _american_to_decimal(american: int) -> float:
    if american > 0:
        return 1.0 + american / 100.0
    if american < 0:
        return 1.0 + 100.0 / abs(american)
    return 1.0


def fmt_american(price: int) -> str:
    """Format an American odds integer as a signed string, e.g. +120 or -115."""
    if price == 0:
        return "—"
    return f"{price:+d}"
