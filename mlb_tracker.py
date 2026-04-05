#!/usr/bin/env python3
"""Historical log for strikeout projections vs results (SQLite + statsapi).

Log picks with :func:`log_prediction`, then (next day or after games end) run
``python mlb_tracker.py update`` to pull starter K from box scores. Use
``python mlb_tracker.py summary`` or :func:`summary` for totals.

Columns stored: date, pitcher, opponent, our_projected_k, betting_line, actual_k,
result (Win / Loss / No-Bet). Optional: ``game_id`` for reliable matching;
``recommended`` (model signal), ``placed_bet``, ``bet_side`` (over/under),
``american_odds``, ``stake``, settled ``profit``, optional ``tracker_tag``,
``projected_bf``, ``actual_bf``, and ``confidence_flag`` (beta / dashboard).

Default DB file is ``mlb_tracker.db`` in this directory; if only the legacy
``mlb_predictions.sqlite`` exists, that path is used until you migrate.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import statsapi

from logic.mlb_schedule import (
    live_game_starter_pitching_lines,
    schedule_club_name_to_canonical,
)
from logic.player_lookup import filter_pitchers, lookup_player

from mlb_accounts import create_user, list_user_emails, user_id_for_email

_ROOT = Path(__file__).resolve().parent
_DB_NEW = _ROOT / "mlb_tracker.db"
_DB_LEGACY = _ROOT / "mlb_predictions.sqlite"


def default_tracker_db_path() -> Path:
    """Pick the tracker SQLite file.

    If both ``mlb_tracker.db`` and legacy ``mlb_predictions.sqlite`` exist, we
    prefer the new name **unless** it is empty and the legacy file still has
    rows — so upgrading does not strand historical bets in the old file.

    Set ``MLB_TRACKER_DB_PATH`` to force the DB file (e.g. on a Docker volume).
    """
    env_path = os.environ.get("MLB_TRACKER_DB_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()

    new_exists = _DB_NEW.exists()
    leg_exists = _DB_LEGACY.exists()

    def _prediction_count(path: Path) -> int:
        try:
            conn = sqlite3.connect(str(path))
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='predictions'"
                ).fetchone()
                if not row or int(row[0]) == 0:
                    return 0
                r2 = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
                return int(r2[0]) if r2 else 0
            finally:
                conn.close()
        except sqlite3.Error:
            return 0

    if new_exists and leg_exists:
        n_new = _prediction_count(_DB_NEW)
        n_leg = _prediction_count(_DB_LEGACY)
        if n_leg > 0 and n_new == 0:
            return _DB_LEGACY
        return _DB_NEW
    if new_exists:
        return _DB_NEW
    if leg_exists:
        return _DB_LEGACY
    return _DB_NEW



BETA_2026_TEST_TAG = "BETA_2026_TEST"


def _connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or default_tracker_db_path()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create schema. ``actual_k`` / ``result`` / ``profit`` are written only by
    :func:`update_results` (CLI or app **Fetch Results**), after MLB reports the game
    as **Final** / **Game Over** — not by triggers or background SQL jobs.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    _user_cols = {
        str(r[1]) for r in conn.execute("PRAGMA table_info(users)").fetchall()
    }
    if "display_name" not in _user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            pitcher TEXT NOT NULL,
            pitcher_team TEXT,
            opponent TEXT NOT NULL,
            our_projected_k REAL NOT NULL,
            betting_line REAL NOT NULL,
            actual_k INTEGER,
            result TEXT,
            game_id INTEGER,
            recommended INTEGER NOT NULL DEFAULT 0,
            placed_bet INTEGER NOT NULL DEFAULT 0,
            bet_side TEXT,
            american_odds REAL,
            closing_american_odds REAL,
            stake REAL,
            profit REAL,
            created_at TEXT NOT NULL
        )
        """
    )
    cols = {
        str(r[1])
        for r in conn.execute("PRAGMA table_info(predictions)").fetchall()
    }
    if "placed_bet" not in cols:
        conn.execute(
            "ALTER TABLE predictions ADD COLUMN placed_bet INTEGER NOT NULL DEFAULT 0"
        )
    if "pitcher_team" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN pitcher_team TEXT")
    if "stake" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN stake REAL")
    if "profit" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN profit REAL")
    if "closing_american_odds" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN closing_american_odds REAL")
    if "tracker_tag" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN tracker_tag TEXT")
    if "projected_bf" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN projected_bf REAL")
    if "actual_bf" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN actual_bf INTEGER")
    if "confidence_flag" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN confidence_flag TEXT")
    cols = {
        str(r[1])
        for r in conn.execute("PRAGMA table_info(predictions)").fetchall()
    }
    if "user_id" not in cols:
        conn.execute(
            "ALTER TABLE predictions ADD COLUMN user_id INTEGER REFERENCES users(id)"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_user ON predictions(user_id)"
    )
    conn.execute("DROP VIEW IF EXISTS v_predictions_pending_settlement")
    conn.execute(
        """
        CREATE VIEW v_predictions_pending_settlement AS
        SELECT
            id,
            user_id,
            date,
            pitcher,
            opponent,
            game_id,
            betting_line,
            bet_side,
            placed_bet,
            american_odds,
            stake,
            created_at
        FROM predictions
        WHERE actual_k IS NULL
        """
    )
    conn.commit()


def _utc_stamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def would_duplicate_open_log(
    game_date: str,
    pitcher: str,
    opponent: str,
    betting_line: float,
    *,
    user_id: int,
    db_path: Path | None = None,
) -> bool:
    """True if an unsettled *manual* row (no tracker tag) already exists for this key."""
    conn = _connect(db_path)
    try:
        init_db(conn)
        row = conn.execute(
            """
            SELECT 1 FROM predictions
            WHERE user_id = ? AND date = ? AND pitcher = ? AND opponent = ? AND betting_line = ?
              AND actual_k IS NULL
              AND COALESCE(tracker_tag, '') = ''
            LIMIT 1
            """,
            (
                int(user_id),
                game_date,
                pitcher.strip(),
                opponent.strip(),
                float(betting_line),
            ),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def would_duplicate_tagged_open_log(
    game_date: str,
    pitcher: str,
    opponent: str,
    betting_line: float,
    *,
    user_id: int,
    tracker_tag: str,
    db_path: Path | None = None,
) -> bool:
    """True if an unsettled row with this ``tracker_tag`` already exists for this key."""
    conn = _connect(db_path)
    try:
        init_db(conn)
        row = conn.execute(
            """
            SELECT 1 FROM predictions
            WHERE user_id = ? AND date = ? AND pitcher = ? AND opponent = ? AND betting_line = ?
              AND actual_k IS NULL
              AND tracker_tag = ?
            LIMIT 1
            """,
            (
                int(user_id),
                game_date,
                pitcher.strip(),
                opponent.strip(),
                float(betting_line),
                str(tracker_tag).strip(),
            ),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def log_prediction(
    game_date: str,
    pitcher: str,
    pitcher_team: str | None,
    opponent: str,
    our_projected_k: float,
    betting_line: float,
    *,
    user_id: int,
    recommended: bool = False,
    placed_bet: bool = False,
    bet_side: str | None = None,
    american_odds: float | None = None,
    stake: float | None = None,
    game_id: int | None = None,
    tracker_tag: str | None = None,
    projected_bf: float | None = None,
    confidence_flag: str | None = None,
    db_path: Path | None = None,
) -> int:
    """Insert a prediction row. Returns new row id.

    ``bet_side`` is ``\"over\"`` or ``\"under\"`` when you logged a priced side;
    ``recommended`` should match whether the model recommended that side.
    ``placed_bet`` marks if you actually bet it; set stake for ROI tracking.
    """
    conn = _connect(db_path)
    try:
        init_db(conn)
        side = (bet_side or "").strip().lower() or None
        if side not in (None, "over", "under"):
            raise ValueError("bet_side must be 'over', 'under', or omitted")
        amt = float(stake) if stake is not None else None
        if amt is not None and amt < 0:
            raise ValueError("stake must be >= 0")
        tag = (tracker_tag or "").strip() or None
        conf = (confidence_flag or "").strip() or None
        pbf = float(projected_bf) if projected_bf is not None else None
        cur = conn.execute(
            """
            INSERT INTO predictions (
                user_id, date, pitcher, pitcher_team, opponent, our_projected_k, betting_line,
                actual_k, result, game_id, recommended, placed_bet, bet_side,
                american_odds, stake, profit, created_at,
                tracker_tag, projected_bf, confidence_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                game_date,
                pitcher.strip(),
                (pitcher_team or "").strip() or None,
                opponent.strip(),
                float(our_projected_k),
                float(betting_line),
                game_id,
                1 if recommended else 0,
                1 if placed_bet else 0,
                side,
                float(american_odds) if american_odds is not None else None,
                amt,
                _utc_stamp(),
                tag,
                pbf,
                conf,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _first_starter_k(box: dict, side: str) -> tuple[int | None, int | None]:
    key = "awayPitchers" if side == "away" else "homePitchers"
    rows = box.get(key) or []
    for row in rows[1:]:
        pid = row.get("personId")
        if pid is None:
            continue
        try:
            ipid = int(pid)
        except (TypeError, ValueError):
            continue
        if ipid == 0:
            continue
        try:
            k = int(row.get("k") or 0)
        except (TypeError, ValueError):
            k = 0
        return ipid, k
    return None, None


def _feed_starter_k_bf(
    game_pk: int,
    side: str,
    person_id: int | None,
) -> tuple[int | None, int | None]:
    """Strikeouts and batters faced for the starting pitcher on ``side`` (from game feed)."""
    away_pid = int(person_id) if side == "away" and person_id is not None else None
    home_pid = int(person_id) if side == "home" and person_id is not None else None
    away, home = live_game_starter_pitching_lines(
        game_pk, away_pitcher_id=away_pid, home_pitcher_id=home_pid
    )
    line = away if side == "away" else home
    if line is None:
        return None, None
    return line.strikeouts, line.batters_faced


def _resolve_pitcher_id(name: str) -> int | None:
    pitchers = filter_pitchers(lookup_player(name.strip()))
    if len(pitchers) == 1:
        return int(pitchers[0]["id"])
    if not pitchers:
        return None
    # Prefer exact fullName match
    target = name.strip().lower()
    for p in pitchers:
        if str(p.get("fullName", "")).strip().lower() == target:
            return int(p["id"])
    return int(pitchers[0]["id"])


def _norm_team(s: str) -> str:
    return " ".join(s.strip().lower().split())


def _opponent_matches_schedule(opp: str, home_name: str, away_name: str) -> tuple[str | None, str | None]:
    """Return (pitcher_side, _) or (None, None).

    If opponent is home team, pitcher is away starter; vice versa.
    """
    o = _norm_team(opp)
    hc = schedule_club_name_to_canonical(home_name)
    ac = schedule_club_name_to_canonical(away_name)
    h = _norm_team(hc) if hc else _norm_team(home_name)
    a = _norm_team(ac) if ac else _norm_team(away_name)
    if o == h or h.startswith(o) or o.startswith(h):
        return "away", "home"
    if o == a or a.startswith(o) or o.startswith(a):
        return "home", "away"
    return None, None


def _bet_won(*, side: str, actual_k: int, line: float) -> bool | None:
    """None = push (no win/loss for typical book rules on integer line)."""
    if actual_k > line:
        over_wins = True
    elif actual_k < line:
        over_wins = False
    else:
        return None
    if side == "over":
        return over_wins
    return not over_wins


def _settle_row_result(actual_k: int, line: float, placed_bet: bool, bet_side: str | None) -> str:
    if not placed_bet or not bet_side:
        return "No-Bet"
    outcome = _bet_won(side=bet_side, actual_k=actual_k, line=line)
    if outcome is None:
        return "No-Bet"
    return "Win" if outcome else "Loss"


def _profit_from_settled_result(
    *,
    result: str,
    american_odds: float | None,
    stake: float | None,
) -> float | None:
    if result not in ("Win", "Loss"):
        return None
    if american_odds is None or stake is None or stake <= 0:
        return None
    if result == "Loss":
        return -stake
    if american_odds > 0:
        return stake * (american_odds / 100.0)
    if american_odds < 0:
        return stake * (100.0 / abs(american_odds))
    return None


def _algorithm_close(projected: float, actual: int) -> bool:
    return abs(float(actual) - float(projected)) <= 1.0


# MLB schedule ``status`` strings that mean the game is finished (see statsapi schedule).
_SCHEDULE_FINAL_STATUSES = frozenset({"Final", "Game Over"})


def _is_schedule_game_final(game_pk: int) -> bool:
    """True when ``statsapi.schedule(game_id=...)`` reports a terminal completed game."""
    try:
        sg = statsapi.schedule(game_id=int(game_pk)) or []
    except Exception:
        return False
    if not sg:
        return False
    st = str(sg[0].get("status") or "").strip()
    return st in _SCHEDULE_FINAL_STATUSES


def _final_games_on_schedule_date(
    iso_date: str,
    *,
    regular_season_only: bool,
) -> list[dict]:
    """Regular-season (optional) games on ``YYYY-MM-DD`` with Final / Game Over status."""
    games = statsapi.schedule(date=iso_date, sportId=1) or []
    if regular_season_only:
        games = [g for g in games if g.get("game_type") == "R"]
    return [
        g for g in games if str(g.get("status") or "").strip() in _SCHEDULE_FINAL_STATUSES
    ]


def update_results(
    *,
    target_date: str | None = None,
    settle_all_pending: bool = False,
    db_path: Path | None = None,
    regular_season_only: bool = True,
) -> int:
    """Fill ``actual_k`` and ``result`` for open tracker rows whose games are finished.

    Only **completed** games are settled: schedule status must be **Final** or **Game Over**
    before reading strikeouts from a stored ``game_id``. Date-wide fallback still uses only
    final games from ``statsapi.schedule``.

    - Default: rows on ``target_date`` only (default **yesterday**).
    - ``settle_all_pending=True``: every row with ``actual_k IS NULL``; for schedule fallback
      uses that row’s ``date`` column as the slate day.

    Returns number of rows updated.
    """
    if not settle_all_pending:
        if target_date is None:
            d = date.today() - timedelta(days=1)
            target_date = d.strftime("%Y-%m-%d")

    conn = _connect(db_path)
    try:
        init_db(conn)
        if settle_all_pending:
            pending = conn.execute(
                """
                SELECT * FROM predictions
                WHERE actual_k IS NULL
                ORDER BY id
                """
            ).fetchall()
        else:
            pending = conn.execute(
                """
                SELECT * FROM predictions
                WHERE date = ? AND actual_k IS NULL
                """,
                (target_date,),
            ).fetchall()
        if not pending:
            return 0

        finals_cache: dict[str, list[dict]] = {}
        if not settle_all_pending:
            assert target_date is not None
            finals_cache[target_date] = _final_games_on_schedule_date(
                target_date,
                regular_season_only=regular_season_only,
            )
        updated = 0

        for row in pending:
            rid = int(row["id"])
            gid_hint = row["game_id"]
            pitcher = str(row["pitcher"])
            opponent = str(row["opponent"])
            line = float(row["betting_line"])
            placed_bet = bool(row["placed_bet"])
            bet_side = row["bet_side"]
            bs_lower = bet_side.lower() if bet_side else None
            odds = float(row["american_odds"]) if row["american_odds"] is not None else None
            stake = float(row["stake"]) if row["stake"] is not None else None

            actual_k: int | None = None
            settle_gid: int | None = None
            settle_side: str | None = None
            pid = _resolve_pitcher_id(pitcher)

            if gid_hint is not None and _is_schedule_game_final(int(gid_hint)):
                try:
                    bx = statsapi.boxscore_data(int(gid_hint))
                except Exception:
                    bx = {}
                else:
                    for side in ("away", "home"):
                        spid, k = _first_starter_k(bx, side)
                        if spid is None:
                            continue
                        if pid is not None and spid == pid:
                            actual_k = k
                            settle_gid = int(gid_hint)
                            settle_side = side
                            break
                    if actual_k is None and bx:
                        sg = statsapi.schedule(game_id=int(gid_hint)) or []
                        g0 = sg[0] if sg else None
                        if g0 and str(g0.get("status") or "").strip() in _SCHEDULE_FINAL_STATUSES:
                            hn = str(g0.get("home_name") or "")
                            an = str(g0.get("away_name") or "")
                            p_side, _ = _opponent_matches_schedule(opponent, hn, an)
                            if p_side:
                                _, k2 = _first_starter_k(bx, p_side)
                                actual_k = k2
                                settle_gid = int(gid_hint)
                                settle_side = p_side

            if actual_k is None:
                row_day = str(row["date"] or "").strip()[:10]
                finals = []
                if row_day:
                    if row_day not in finals_cache:
                        finals_cache[row_day] = _final_games_on_schedule_date(
                            row_day,
                            regular_season_only=regular_season_only,
                        )
                    finals = finals_cache[row_day]
                for g in finals:
                    gid = int(g["game_id"])
                    hn = str(g.get("home_name") or "")
                    an = str(g.get("away_name") or "")
                    p_side, _ = _opponent_matches_schedule(opponent, hn, an)
                    if p_side is None:
                        continue
                    try:
                        bx = statsapi.boxscore_data(gid)
                    except Exception:
                        continue
                    spid, k = _first_starter_k(bx, p_side)
                    if spid is None:
                        continue
                    if pid is not None and spid != pid:
                        continue
                    if pid is None:
                        # keep only if single candidate game for this opponent date
                        pass
                    actual_k = k
                    settle_gid = gid
                    settle_side = p_side
                    conn.execute(
                        "UPDATE predictions SET game_id = ? WHERE id = ?",
                        (gid, rid),
                    )
                    break

            if actual_k is None:
                continue

            actual_bf: int | None = None
            if settle_gid is not None and settle_side is not None:
                _, bf_fb = _feed_starter_k_bf(settle_gid, settle_side, pid)
                actual_bf = bf_fb

            result = _settle_row_result(actual_k, line, placed_bet, bs_lower)
            profit = _profit_from_settled_result(
                result=result,
                american_odds=odds,
                stake=stake,
            )
            conn.execute(
                """
                UPDATE predictions
                SET actual_k = ?, actual_bf = ?, result = ?, profit = ?
                WHERE id = ?
                """,
                (actual_k, actual_bf, result, profit, rid),
            )
            updated += 1

        conn.commit()
        return updated
    finally:
        conn.close()


@dataclass
class SummaryStats:
    total_with_actual: int
    algorithm_hits: int
    algorithm_accuracy_pct: float | None
    recommended_count: int
    recommended_wins: int
    recommended_losses: int
    no_bet_count: int
    total_profit_units: float
    total_staked: float
    roi_pct: float | None


def _profit_on_100_stake(american_odds: float, won: bool) -> float:
    if not won:
        return -100.0
    if american_odds > 0:
        return 100.0 * (american_odds / 100.0)
    return 100.0 * (100.0 / abs(american_odds))


def compute_summary(
    db_path: Path | None = None,
    *,
    user_id: int | None = None,
) -> SummaryStats:
    conn = _connect(db_path)
    try:
        init_db(conn)
        if user_id is not None:
            rows = conn.execute(
                """
                SELECT * FROM predictions
                WHERE actual_k IS NOT NULL AND user_id = ?
                """,
                (int(user_id),),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM predictions WHERE actual_k IS NOT NULL"
            ).fetchall()
        total = len(rows)
        hits = sum(
            1
            for r in rows
            if _algorithm_close(float(r["our_projected_k"]), int(r["actual_k"]))
        )
        acc = (100.0 * hits / total) if total else None

        stake_each = 100.0
        profit = 0.0
        staked = 0.0
        rw = rl = nb = 0
        rec_n = 0
        for r in rows:
            if not r["placed_bet"]:
                if r["result"] == "No-Bet":
                    nb += 1
                continue
            if r["result"] == "No-Bet":
                nb += 1
                continue
            if r["result"] not in ("Win", "Loss"):
                continue
            rec_n += 1
            won = r["result"] == "Win"
            if won:
                rw += 1
            else:
                rl += 1

            row_stake = float(r["stake"]) if r["stake"] is not None else None
            row_profit = float(r["profit"]) if r["profit"] is not None else None
            if row_stake is not None and row_stake > 0 and row_profit is not None:
                staked += row_stake
                profit += row_profit
                continue

            # Backward compatibility for older rows without stake/profit.
            odds = r["american_odds"]
            if odds is None or float(odds) == 0:
                continue
            staked += stake_each
            profit += _profit_on_100_stake(float(odds), won)

        roi = (100.0 * profit / staked) if staked > 0 else None
        return SummaryStats(
            total_with_actual=total,
            algorithm_hits=hits,
            algorithm_accuracy_pct=acc,
            recommended_count=rec_n,
            recommended_wins=rw,
            recommended_losses=rl,
            no_bet_count=nb,
            total_profit_units=profit,
            total_staked=staked,
            roi_pct=roi,
        )
    finally:
        conn.close()


def summary(db_path: Path | None = None, *, user_id: int | None = None) -> None:
    """Print aggregate stats (alias for :func:`print_summary`)."""
    print_summary(db_path, user_id=user_id)


def print_summary(db_path: Path | None = None, *, user_id: int | None = None) -> None:
    s = compute_summary(db_path, user_id=user_id)
    print(f"Total Games Tracked: {s.total_with_actual}")
    if s.algorithm_accuracy_pct is not None:
        print(
            f"Algorithm Accuracy (|Actual − Projected| ≤ 1.0): "
            f"{s.algorithm_hits}/{s.total_with_actual} = {s.algorithm_accuracy_pct:.1f}%"
        )
    else:
        print("Algorithm Accuracy: n/a (no completed games yet)")
    if s.recommended_count:
        print(
            f"Betting ROI (placed bets only; uses logged stake/profit when available): "
            f"${s.total_profit_units:+.2f} on ${s.total_staked:.0f} staked "
            f"({s.roi_pct:+.1f}% ROI) — W{s.recommended_wins} L{s.recommended_losses} NB{s.no_bet_count}"
        )
    else:
        print(
            "Betting ROI: n/a (no settled placed bets, or missing stake/odds)"
        )


def _resolve_cli_summary_user_id(conn: sqlite3.Connection, email: str | None) -> int | None:
    users = list_user_emails(conn)
    if not users:
        return None
    if email:
        uid = user_id_for_email(conn, email.strip())
        if uid is None:
            raise SystemExit(f"No user with email: {email.strip()}")
        return uid
    if len(users) == 1:
        return int(users[0][0])
    raise SystemExit("Multiple users exist; pass --user-email for summary.")


def _read_password_from_env_or_prompt(env_key: str = "MLB_BOOTSTRAP_PASSWORD") -> str:
    pw = os.environ.get(env_key, "").strip()
    if pw:
        return pw
    p1 = getpass.getpass("Password: ")
    p2 = getpass.getpass("Confirm password: ")
    if p1 != p2:
        raise SystemExit("Passwords do not match.")
    return p1


def main() -> int:
    p = argparse.ArgumentParser(description="MLB strikeout prediction log")
    p.add_argument("--db", type=Path, default=None, help="SQLite path")
    sub = p.add_subparsers(dest="cmd", required=True)

    u = sub.add_parser(
        "update",
        help="Fill actual K from box scores for completed games only (default: yesterday)",
    )
    u.add_argument(
        "--all-pending",
        action="store_true",
        help="Settle every open row (actual_k NULL) when the game is Final / Game Over",
    )
    u.add_argument(
        "--date",
        default=None,
        help="YYYY-MM-DD: only rows with this date (ignored with --all-pending; default: yesterday)",
    )
    sp = sub.add_parser("summary", help="Print aggregate stats")
    sp.add_argument(
        "--user-email",
        default=None,
        help="User scope (required if multiple accounts exist)",
    )

    sub.add_parser("init", help="Create empty database")

    boot = sub.add_parser(
        "bootstrap-admin",
        help="Create admin user and assign predictions with NULL user_id to that user",
    )
    boot.add_argument(
        "--email",
        required=True,
        help="Admin email (password: MLB_BOOTSTRAP_PASSWORD or prompt)",
    )

    addu = sub.add_parser("add-user", help="Create a new login")
    addu.add_argument("--email", required=True)

    args = p.parse_args()
    db = args.db
    if args.cmd == "init":
        conn = _connect(db)
        init_db(conn)
        conn.close()
        print("OK:", (db or default_tracker_db_path()))
        return 0
    if args.cmd == "update":
        if getattr(args, "all_pending", False) and args.date:
            raise SystemExit("Use either --all-pending or --date, not both.")
        n = update_results(
            target_date=args.date,
            settle_all_pending=bool(getattr(args, "all_pending", False)),
            db_path=db,
        )
        print(f"Updated {n} row(s).")
        return 0
    if args.cmd == "summary":
        conn = _connect(db)
        try:
            init_db(conn)
            uid = _resolve_cli_summary_user_id(conn, getattr(args, "user_email", None))
        finally:
            conn.close()
        print_summary(db_path=db, user_id=uid)
        return 0
    if args.cmd == "bootstrap-admin":
        conn = _connect(db)
        try:
            init_db(conn)
            uid = user_id_for_email(conn, args.email)
            if uid is None:
                pw = _read_password_from_env_or_prompt()
                uid = create_user(conn, args.email, pw)
            else:
                print(
                    "User already exists; assigning orphan predictions only.",
                    file=sys.stderr,
                )
            conn.execute(
                "UPDATE predictions SET user_id = ? WHERE user_id IS NULL",
                (int(uid),),
            )
            conn.commit()
            print("OK: user id", uid, "— migrated rows with NULL user_id.")
        finally:
            conn.close()
        return 0
    if args.cmd == "add-user":
        pw = _read_password_from_env_or_prompt("MLB_NEW_USER_PASSWORD")
        if not pw:
            raise SystemExit("Empty password.")
        conn = _connect(db)
        try:
            init_db(conn)
            uid = create_user(conn, args.email, pw)
            print("OK: created user id", uid, args.email)
        except sqlite3.IntegrityError:
            raise SystemExit("Email already registered.") from None
        finally:
            conn.close()
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
