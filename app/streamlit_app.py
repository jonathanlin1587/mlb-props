# Run from repo root: streamlit run app/streamlit_app.py

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from datetime import date, datetime

import os
import shutil
import sqlite3
import math
import html as _html
import textwrap as _textwrap
import altair as alt
import pandas as pd
import statsapi
import streamlit as st
try:
    from streamlit_autorefresh import st_autorefresh as _st_autorefresh
except Exception:
    _st_autorefresh = None


def _sync_cloud_secrets_into_environ() -> None:
    """Copy selected Streamlit Secrets into os.environ before other imports read them."""
    try:
        sec = st.secrets
    except Exception:
        return
    for key in (
        "ODDS_API_KEY",
        "MLB_ALLOW_REGISTRATION",
        "MLB_BOOTSTRAP_EMAIL",
        "MLB_BOOTSTRAP_PASSWORD",
    ):
        if os.environ.get(key, "").strip():
            continue
        try:
            if key in sec:
                val = str(sec[key]).strip()
                if val:
                    os.environ[key] = val
        except Exception:
            pass


_sync_cloud_secrets_into_environ()

from data.league_splits import (
    fetch_league_splits,
    league_data_json_path,
    load_league_data,
    opponent_k_pct_blended,
)
from data.mlb_teams import MLB_TEAMS
from data.pitcher_api_stats import (
    CONFIDENCE_K_DIFF_THRESHOLD_PCT,
    SEASON_CURRENT,
    SEASON_PRIOR,
    WeightedPitcherStats,
    get_weighted_stats,
)
from logic.mlb_schedule import (
    LiveStarterPitchingLine,
    ScheduledMatchup,
    UPCOMING_GAME_STATUSES,
    canonical_team_for_id,
    get_canonical_team_id_map,
    get_daily_schedule,
    get_id_to_abbrev,
    get_scheduled_matchup_for_team,
    live_game_starter_pitching_lines,
    schedule_club_name_to_canonical,
    scheduled_matchup_from_game_for_team,
    team_display_for_id,
)
from logic.player_lookup import (
    filter_pitchers,
    format_player_option,
    get_pitcher_throws,
    lookup_player,
    pitcher_team_id,
)
from logic.projection import (
    BF_PROJECTED,
    EDGE_MIN_RELAXED_PCT,
    EDGE_MIN_STRICT_PCT,
    PropSide,
    american_odds_to_implied_probability_pct,
    poisson_over_probability_pct,
    poisson_under_probability_pct,
    compute_projection,
    calculate_kelly_bet,
    compute_value_verdict,
)
from logic.odds_api import (
    PitcherOddsEntry,
    best_over_price_book,
    fetch_mlb_odds_data,
    find_best_kelly_odds,
    fmt_american,
    get_cached_fetch_time,
    lookup_pitcher_in_odds,
    modal_line,
)
from logic.stat_weights import get_current_weights
from app.accounts import (
    authenticate,
    create_user,
    get_user_profile,
    normalize_email,
    update_display_name,
    update_password,
)
from app.tracker import (
    BETA_2026_TEST_TAG,
    compute_summary,
    default_tracker_db_path,
    init_db,
    log_prediction,
    update_results,
    would_duplicate_open_log,
)

_PENDING_SIDEBAR_FROM_DASH = "_pending_sidebar_from_dash"
_DASH_SIDEBAR_SYNC_DONE = "dash_sidebar_sync_done"
_SIDEBAR_KEY_LINE = "prop_line"
_SIDEBAR_KEY_AMERICAN_ODDS = "prop_american_odds"
_SIDEBAR_KEY_BET_SIDE = "prop_bet_side"
_NEXT_MU_DASH_OVERRIDE = "next_mu_dash_override"
_MAIN_SCREEN = "main_screen"
_DETAIL_GID = "detail_gid"
_SLATE_DATE_ISO = "slate_date_iso"
_FETCH_RESULTS_COUNT_KEY = "_fetch_results_updated_count"
_AUTO_BETA_LOGGING_KEY = "auto_beta_logging_enabled"
_TOP_NAV_VIEW = "top_nav_view"
_LIVE_CENTER_DETAIL_GID = "_live_center_detail_gid"
_AUTO_SETTLE_LAST_RUN_TS = "_auto_settle_last_run_ts"
_AUTO_SETTLE_UPDATED_KEY = "_auto_settle_updated_count"
_AUTH_USER_ID = "auth_user_id"
_AUTH_EMAIL = "auth_email"
_AUTH_DISPLAY_NAME = "auth_display_name"
_BETA_DEFAULT_BANKROLL = 500.0
_PENDING_BANKROLL_RESET = "_pending_bankroll_reset"

import json as _json
import pathlib as _pathlib
import time as _time

_LEGACY_LOCKS_FILE = _REPO_ROOT / "locked_lines.json"


def _user_settings_root() -> _pathlib.Path:
    raw = os.environ.get("MLB_USER_DATA_DIR", "").strip()
    if raw:
        return _pathlib.Path(raw).expanduser().resolve()
    return _REPO_ROOT / "user_settings"


def _locks_file_for_user(user_id: int) -> _pathlib.Path:
    return _user_settings_root() / str(int(user_id)) / "locked_lines.json"


def _migrate_legacy_locks_if_needed(user_id: int) -> None:
    dest = _locks_file_for_user(user_id)
    if dest.exists():
        return
    if not _LEGACY_LOCKS_FILE.is_file():
        return
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_LEGACY_LOCKS_FILE, dest)
    except OSError:
        return


def _clear_lock_keys_from_session() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and (
            k.startswith("locked_line_") or k.startswith("locked_odds_")
        ):
            st.session_state.pop(k, None)


def _perform_logout() -> None:
    st.session_state.pop(_AUTH_USER_ID, None)
    st.session_state.pop(_AUTH_EMAIL, None)
    st.session_state.pop(_AUTH_DISPLAY_NAME, None)
    st.session_state.pop("_locks_loaded", None)
    st.session_state.pop(_PENDING_BANKROLL_RESET, None)
    _clear_lock_keys_from_session()


def _sync_auth_display_name_from_db(user_id: int) -> None:
    dbp = default_tracker_db_path()
    if not dbp.is_file():
        st.session_state[_AUTH_DISPLAY_NAME] = None
        return
    conn = sqlite3.connect(str(dbp))
    try:
        init_db(conn)
        prof = get_user_profile(conn, int(user_id))
        st.session_state[_AUTH_DISPLAY_NAME] = (
            prof.get("display_name") if prof is not None else None
        )
    finally:
        conn.close()


def _account_menu_trigger_label() -> str:
    """Short label for circular popover control (full name shown inside the menu)."""
    dn = (st.session_state.get(_AUTH_DISPLAY_NAME) or "").strip()
    if dn:
        return dn[0].upper()
    em = str(st.session_state.get(_AUTH_EMAIL) or "").strip()
    if em:
        local = em.split("@", 1)[0]
        return (local[0].upper() if local else "👤")
    return "👤"


@st.dialog("Edit profile")
def _edit_profile_dialog() -> None:
    uid = int(st.session_state[_AUTH_USER_ID])
    dbp = default_tracker_db_path()
    conn = sqlite3.connect(str(dbp))
    try:
        init_db(conn)
        prof = get_user_profile(conn, uid) or {}
    finally:
        conn.close()
    default_dn = (st.session_state.get(_AUTH_DISPLAY_NAME) or prof.get("display_name") or "")
    st.caption(
        "Your **email** is your login and cannot be changed here. "
        "Update your display name and/or password below."
    )
    st.text_input(
        "Email (read-only)",
        value=str(prof.get("email") or st.session_state.get(_AUTH_EMAIL) or ""),
        disabled=True,
    )
    dn = st.text_input(
        "Display name",
        value=default_dn,
        key=f"dlg_profile_display_name_{uid}",
        placeholder="How you appear in the app",
    )
    st.divider()
    st.markdown("**Change password** _(optional)_")
    old_pw = st.text_input("Current password", type="password", key="dlg_profile_old_pw")
    new_pw = st.text_input("New password", type="password", key="dlg_profile_new_pw")
    new_pw2 = st.text_input("Confirm new password", type="password", key="dlg_profile_new_pw2")
    if st.button("Save changes", type="primary", key="dlg_profile_save"):
        errs: list[str] = []
        conn = sqlite3.connect(str(dbp))
        try:
            init_db(conn)
            update_display_name(conn, uid, (dn or "").strip() or None)
            st.session_state[_AUTH_DISPLAY_NAME] = (dn or "").strip() or None
            if (old_pw or new_pw or new_pw2):
                if not old_pw:
                    errs.append("Enter your current password to change it.")
                elif new_pw != new_pw2:
                    errs.append("New passwords do not match.")
                else:
                    ok, err = update_password(conn, uid, old_pw, new_pw)
                    if not ok:
                        errs.append(err)
        finally:
            conn.close()
        if errs:
            for e in errs:
                st.error(e)
        else:
            st.success("Profile saved.")
            st.rerun()


def _render_account_menu_popover() -> None:
    """User profile popover: edit profile, bankroll reset, logout."""
    with st.popover(_account_menu_trigger_label()):
        st.markdown("**Account Settings**")
        st.divider()
        if st.button("Edit profile", key="acct_menu_edit_profile", use_container_width=True):
            _edit_profile_dialog()
        if st.button(
            "Bankroll management",
            key="acct_menu_bankroll",
            use_container_width=True,
        ):
            st.session_state[_PENDING_BANKROLL_RESET] = True
            st.rerun()
        st.divider()
        if st.button(
            "🚪  Log out",
            key="account_menu_logout",
            type="primary",
            use_container_width=True,
        ):
            _perform_logout()
            st.rerun()


def _registration_allowed() -> bool:
    if os.environ.get("MLB_ALLOW_REGISTRATION", "").strip() == "1":
        return True
    try:
        sec = st.secrets
        if "MLB_ALLOW_REGISTRATION" in sec:
            v = str(sec["MLB_ALLOW_REGISTRATION"]).strip().lower()
            return v in ("1", "true", "yes")
    except Exception:
        pass
    return False


def _get_bootstrap_credentials() -> tuple[str, str] | None:
    """Email/password for first user on empty DB (Streamlit Secrets or env).

    Streamlit Community Cloud: add to **App settings → Secrets**, for example::

        MLB_BOOTSTRAP_EMAIL = "you@example.com"
        MLB_BOOTSTRAP_PASSWORD = "your-secure-password"

    Or a nested block::

        [mlb_bootstrap]
        email = "you@example.com"
        password = "your-secure-password"
    """
    e = os.environ.get("MLB_BOOTSTRAP_EMAIL", "").strip()
    p = os.environ.get("MLB_BOOTSTRAP_PASSWORD", "").strip()
    if e and p:
        return (e, p)
    try:
        sec = st.secrets
    except Exception:
        return None
    try:
        if "MLB_BOOTSTRAP_EMAIL" in sec and "MLB_BOOTSTRAP_PASSWORD" in sec:
            e2 = str(sec["MLB_BOOTSTRAP_EMAIL"]).strip()
            p2 = str(sec["MLB_BOOTSTRAP_PASSWORD"]).strip()
            if e2 and p2:
                return (e2, p2)
    except Exception:
        pass
    try:
        if "mlb_bootstrap" in sec:
            block = sec["mlb_bootstrap"]
            e2 = str(block["email"]).strip()
            p2 = str(block["password"]).strip()
            if e2 and p2:
                return (e2, p2)
    except Exception:
        pass
    return None


def _ensure_bootstrap_user_if_needed() -> None:
    """Create the first user when the DB is empty (needed for Streamlit Cloud)."""
    if st.session_state.get("_mlb_bootstrap_tried"):
        return
    creds = _get_bootstrap_credentials()
    if creds is None:
        return
    st.session_state._mlb_bootstrap_tried = True
    email, password = creds
    dbp = default_tracker_db_path()
    try:
        dbp.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    conn = sqlite3.connect(str(dbp))
    try:
        init_db(conn)
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        n = int(row[0]) if row else 0
        if n > 0:
            return
        create_user(conn, email, password)
    except ValueError as ex:
        st.session_state._mlb_bootstrap_error = str(ex)
    except sqlite3.IntegrityError:
        pass
    except OSError as ex:
        st.session_state._mlb_bootstrap_error = str(ex)
    finally:
        conn.close()


def _ensure_authenticated() -> None:
    if _AUTH_USER_ID not in st.session_state:
        st.session_state[_AUTH_USER_ID] = None
    if st.session_state.get(_AUTH_USER_ID) is not None:
        return
    _ensure_bootstrap_user_if_needed()
    err = st.session_state.pop("_mlb_bootstrap_error", None)
    if err:
        st.error(f"Could not create bootstrap account: {err}")
    st.markdown("## Sign in")
    dbp = default_tracker_db_path()
    with st.form("login_form"):
        email_in = st.text_input("Email")
        pw_in = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in")
    if submitted:
        if not dbp.is_file():
            st.error(
                "No tracker database yet. **Streamlit Cloud:** add "
                "`MLB_BOOTSTRAP_EMAIL` and `MLB_BOOTSTRAP_PASSWORD` under App settings → Secrets, "
                "then redeploy. **Local:** run "
                "`python mlb_tracker.py bootstrap-admin --email you@example.com` "
                "(set `MLB_BOOTSTRAP_PASSWORD` or enter password when prompted)."
            )
        else:
            conn = sqlite3.connect(str(dbp))
            try:
                init_db(conn)
                uid = authenticate(conn, email_in, pw_in)
                prof = get_user_profile(conn, int(uid)) if uid is not None else None
            finally:
                conn.close()
            if uid is None:
                st.error("Invalid email or password.")
            else:
                _clear_lock_keys_from_session()
                st.session_state.pop("_locks_loaded", None)
                st.session_state[_AUTH_USER_ID] = int(uid)
                st.session_state[_AUTH_EMAIL] = str(
                    (prof or {}).get("email") or normalize_email(email_in)
                )
                st.session_state[_AUTH_DISPLAY_NAME] = (prof or {}).get("display_name")
                st.rerun()
    if not _registration_allowed():
        st.caption(
            "No **Create account** section yet — registration is off. "
            "To allow it: set **MLB_ALLOW_REGISTRATION** to **1** in "
            "Streamlit **App settings → Secrets** (then redeploy), or "
            "`export MLB_ALLOW_REGISTRATION=1` locally. "
            "Otherwise sign in with your bootstrap admin account."
        )
    if _registration_allowed():
        with st.expander("Create account", expanded=False):
            e2 = st.text_input("New account email", key="reg_email")
            p2 = st.text_input("Choose password", type="password", key="reg_pw")
            p3 = st.text_input("Confirm password", type="password", key="reg_pw2")
            if st.button("Register", key="reg_submit"):
                if not (p2 and p2 == p3):
                    st.error("Passwords must match and be non-empty.")
                elif not dbp.is_file():
                    st.error("Initialize the database first (`mlb_tracker.py init`).")
                else:
                    conn = sqlite3.connect(str(dbp))
                    try:
                        init_db(conn)
                        create_user(conn, e2, p2)
                        st.success("Account created — sign in above.")
                    except sqlite3.IntegrityError:
                        st.error("That email is already registered.")
                    except ValueError as ex:
                        st.error(str(ex))
                    finally:
                        conn.close()
    st.stop()


def _refresh_league_json_if_stale(
    season: int,
    *,
    max_age_seconds: float = 86_400,
) -> bool:
    """Rewrite ``league_data_{season}.json`` from the API if missing or older than ``max_age_seconds``."""
    out = league_data_json_path(season)
    need = not out.is_file()
    if not need:
        try:
            need = _time.time() - out.stat().st_mtime > float(max_age_seconds)
        except OSError:
            need = True
    if not need:
        return False
    try:
        fetch_league_splits(season, path=out)
    except Exception:
        return False
    return True


@st.cache_data(ttl=1800, show_spinner=False)
def _resolve_pitcher_projection_for_card(
    pitcher_name: str, opposing_team: str
) -> float | None:
    """Best-effort projection for slate quick-log (same model inputs as main app)."""
    raw = _cached_lookup_player(pitcher_name.strip())
    pitchers = filter_pitchers(raw)
    if not pitchers:
        return None
    p = pitchers[0]
    pid = p.get("id")
    if pid is None:
        return None
    weighted = _cached_weighted_pitcher(int(pid))
    if weighted is None or weighted.bf_per_start is None:
        return None
    pitcher_throws = _cached_pitcher_throws(int(pid))
    lg_2025 = _cached_league_doc(SEASON_PRIOR)
    lg_2026 = _cached_league_doc(SEASON_CURRENT)
    opp_k_pct, _, _ = opponent_k_pct_blended(
        opposing_team,
        pitcher_throws,
        as_of=date.today(),
        league_doc_2025=lg_2025,
        league_doc_2026=lg_2026,
    )
    if opp_k_pct is None:
        return None
    return float(compute_projection(weighted.k_pct, opp_k_pct, weighted.bf_per_start))


def _load_predictions_df(*, user_id: int) -> pd.DataFrame:
    """Load all prediction rows from the tracker DB for one user."""
    dbp = default_tracker_db_path()
    if not dbp.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(dbp))
    try:
        init_db(conn)
        return pd.read_sql_query(
            """
            SELECT
                id,
                date,
                pitcher,
                pitcher_team,
                opponent,
                our_projected_k,
                betting_line,
                american_odds,
                closing_american_odds,
                stake,
                actual_k,
                actual_bf,
                result,
                profit,
                created_at,
                game_id,
                bet_side,
                placed_bet,
                recommended,
                tracker_tag,
                projected_bf,
                confidence_flag
            FROM predictions
            WHERE user_id = ?
            ORDER BY id DESC
            """,
            conn,
            params=(int(user_id),),
        )
    except sqlite3.OperationalError:
        return pd.DataFrame()
    finally:
        conn.close()


def _norm_person_name(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def _placed_bets_for_game(game_pk: int, *, user_id: int) -> list[dict]:
    """Tracker rows with ``placed_bet`` and this ``game_id`` (actually placed wagers)."""
    dbp = default_tracker_db_path()
    if not dbp.exists():
        return []
    conn = sqlite3.connect(str(dbp))
    try:
        init_db(conn)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT
                id,
                date,
                pitcher,
                opponent,
                our_projected_k,
                betting_line,
                bet_side,
                american_odds,
                stake,
                actual_k,
                result
            FROM predictions
            WHERE user_id = ? AND game_id = ? AND COALESCE(placed_bet, 0) = 1
            ORDER BY id DESC
            """,
            (int(user_id), int(game_pk)),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _fmt_tracker_bet_side(raw: str | None) -> str:
    if not raw:
        return "—"
    s = str(raw).strip().lower()
    if s == "over":
        return "Over"
    if s == "under":
        return "Under"
    return str(raw).strip()


def _pitcher_home_away_tag(pitcher: str, away_p: str, home_p: str) -> str:
    pn = _norm_person_name(pitcher)
    if away_p != "TBD" and pn == _norm_person_name(away_p):
        return " _(Away)_"
    if home_p != "TBD" and pn == _norm_person_name(home_p):
        return " _(Home)_"
    return ""


def _format_placed_bet_line(
    row: dict, away_p: str, home_p: str
) -> str:
    pitcher = str(row.get("pitcher") or "").strip()
    opp = str(row.get("opponent") or "").strip()
    line_v = float(row["betting_line"])
    side = _fmt_tracker_bet_side(row.get("bet_side"))
    odds_raw = row.get("american_odds")
    try:
        odds_i = int(round(float(odds_raw))) if odds_raw is not None else None
    except (TypeError, ValueError):
        odds_i = None
    odds_s = fmt_american(odds_i) if odds_i is not None else "—"
    lam = float(row["our_projected_k"])
    stake_raw = row.get("stake")
    try:
        stake_f = float(stake_raw) if stake_raw is not None else None
    except (TypeError, ValueError):
        stake_f = None
    stake_part = f" · stake **${stake_f:.2f}**" if stake_f is not None else ""
    ak = row.get("actual_k")
    res = row.get("result")
    tag = ""
    if ak is not None:
        try:
            ki = int(ak)
        except (TypeError, ValueError):
            ki = None
        if ki is not None:
            tag = f" · **{ki} K**"
            if res:
                tag += f" → _{res}_"
    else:
        tag = " · actual K: _pending_"
    who = _pitcher_home_away_tag(pitcher, away_p, home_p)
    return (
        f"- **{pitcher}**{who} vs {opp} — **{side}** **{line_v:.1f}** @ **{odds_s}**"
        f" · λ {lam:.2f}{stake_part}{tag}"
    )


def _history_metrics(*, user_id: int) -> tuple[float, float | None, float | None]:
    s = compute_summary(db_path=default_tracker_db_path(), user_id=int(user_id))
    total_profit = float(s.total_profit_units)
    roi_pct = s.roi_pct
    total_settled = int(s.recommended_wins + s.recommended_losses)
    win_rate = (100.0 * float(s.recommended_wins) / float(total_settled)) if total_settled > 0 else None
    return total_profit, roi_pct, win_rate


_MAX_HISTORY_DAY_TABS = 16


def _history_game_dates_descending(hist_df: pd.DataFrame) -> list[str]:
    if hist_df.empty or "date" not in hist_df.columns:
        return []
    raw = hist_df["date"].dropna().astype(str).str.strip()
    raw = raw[raw != ""]
    return sorted(set(raw), reverse=True)


def _history_tab_label(date_iso: str) -> str:
    s = date_iso.strip()[:10]
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return d.strftime("%a %b %d · %Y")
    except ValueError:
        return date_iso


def _hist_filter_by_game_date(hist_df: pd.DataFrame, date_iso: str) -> pd.DataFrame:
    key = date_iso.strip()[:10]
    ds = hist_df["date"].astype(str).str.strip().str[:10]
    return hist_df.loc[ds == key].copy()


def _pending_bets_df(hist_df: pd.DataFrame) -> pd.DataFrame:
    return hist_df[
        hist_df["actual_k"].isna()
        & (hist_df["placed_bet"].fillna(0).astype(int) == 1)
    ].copy()


def _totals_wagered_profit_for_rows(rows_df: pd.DataFrame) -> tuple[float, float, int]:
    """Sum of stake (placed bets with stake > 0) and sum of profit (non-null) for a slice."""
    if rows_df.empty:
        return 0.0, 0.0, 0
    placed = rows_df["placed_bet"].fillna(0).astype(int) == 1
    stake = pd.to_numeric(rows_df["stake"], errors="coerce")
    profit = pd.to_numeric(rows_df["profit"], errors="coerce")
    mask = placed & stake.notna() & (stake > 0)
    wagered = float(stake[mask].sum())
    profit_sum = float(profit.dropna().sum()) if profit.notna().any() else 0.0
    n_stake = int(mask.sum())
    return wagered, profit_sum, n_stake


def _history_totals_by_game_date(hist_df: pd.DataFrame) -> pd.DataFrame:
    """One row per game ``date`` with wagered, profit, and count of staked bets."""
    if hist_df.empty or "date" not in hist_df.columns:
        return pd.DataFrame(
            columns=["Game date", "Wagered", "Profit", "Bets (stake)"]
        )
    df = hist_df.copy()
    df["_day"] = df["date"].astype(str).str.strip().str[:10]
    placed = df["placed_bet"].fillna(0).astype(int) == 1
    stake = pd.to_numeric(df["stake"], errors="coerce")
    profit = pd.to_numeric(df["profit"], errors="coerce")
    df["_wag"] = 0.0
    m = placed & stake.notna() & (stake > 0)
    df.loc[m, "_wag"] = stake[m]

    def _profit_sum(s: pd.Series) -> float:
        p = pd.to_numeric(s, errors="coerce")
        return float(p.dropna().sum()) if p.notna().any() else 0.0

    g = (
        df.groupby("_day", as_index=False)
        .agg(
            wagered=("_wag", "sum"),
            profit=("profit", _profit_sum),
            bets_stake=("_wag", lambda s: int((s > 0).sum())),
        )
        .sort_values("_day", ascending=False)
    )
    g["wagered"] = g["wagered"].round(2)
    g["profit"] = g["profit"].round(2)
    return g.rename(
        columns={
            "_day": "Game date",
            "wagered": "Wagered",
            "profit": "Profit",
            "bets_stake": "Bets (stake)",
        }
    )


def _render_history_cards(rows_df: pd.DataFrame, *, empty_text: str, max_rows: int = 40) -> None:
    """Render history rows as compact visual cards instead of only raw grids."""
    if rows_df.empty:
        st.caption(empty_text)
        return
    show_df = rows_df.copy().head(max_rows)
    for _, r in show_df.iterrows():
        pitcher = str(r.get("pitcher") or "Unknown")
        opp = str(r.get("opponent") or "Unknown")
        side = str(r.get("bet_side") or "").strip().lower()
        side_label = side.title() if side in ("over", "under") else "Lean"
        line_v = pd.to_numeric(pd.Series([r.get("betting_line")]), errors="coerce").iloc[0]
        odds_v = pd.to_numeric(pd.Series([r.get("american_odds")]), errors="coerce").iloc[0]
        stake_v = pd.to_numeric(pd.Series([r.get("stake")]), errors="coerce").iloc[0]
        result = str(r.get("result") or "Pending")
        lam = pd.to_numeric(pd.Series([r.get("our_projected_k")]), errors="coerce").iloc[0]
        actual = pd.to_numeric(pd.Series([r.get("actual_k")]), errors="coerce").iloc[0]
        profit_v = pd.to_numeric(pd.Series([r.get("profit")]), errors="coerce").iloc[0]

        line_s = f"{float(line_v):.1f}" if pd.notna(line_v) else "?"
        odds_s = fmt_american(int(round(float(odds_v)))) if pd.notna(odds_v) else "n/a"
        stake_s = f"${float(stake_v):.2f}" if pd.notna(stake_v) and float(stake_v) > 0 else "—"
        lam_s = f"{float(lam):.2f}" if pd.notna(lam) else "—"
        actual_s = f"{int(actual)}" if pd.notna(actual) else "pending"
        profit_class = "pos" if pd.notna(profit_v) and float(profit_v) > 0 else ("neg" if pd.notna(profit_v) and float(profit_v) < 0 else "neu")
        profit_s = f"${float(profit_v):+,.2f}" if pd.notna(profit_v) else "—"
        date_s = str(r.get("date") or "")[:10]
        rid = int(r.get("id")) if pd.notna(r.get("id")) else 0

        st.markdown(
            (
                f'<div class="history-row-card">'
                f'<div class="history-row-head"><span class="history-matchup">{_html.escape(pitcher)} vs {_html.escape(opp)}</span>'
                f'<span class="history-pill">{_html.escape(result)}</span></div>'
                f'<div class="history-row-grid">'
                f'<span><b>{_html.escape(side_label)}</b> {line_s} @ {odds_s}</span>'
                f'<span>Stake: <b>{stake_s}</b></span>'
                f'<span>Model λ: <b>{lam_s}</b></span>'
                f'<span>Actual K: <b>{actual_s}</b></span>'
                f'<span class="history-profit {profit_class}">P/L: <b>{profit_s}</b></span>'
                f'<span class="history-meta">#{rid} · {date_s}</span>'
                f"</div>"
                f"</div>"
            ),
            unsafe_allow_html=True,
        )

    if len(rows_df) > max_rows:
        st.caption(f"Showing first {max_rows} rows of {len(rows_df)}. Use the table view below for all rows.")


def _render_history_by_day(
    hist_df: pd.DataFrame,
    pending_df: pd.DataFrame,
    *,
    date_iso: str | None,
) -> None:
    """Pending + full log for one game date, or everything when ``date_iso`` is None."""
    if date_iso is None:
        p_df = pending_df.copy()
        rows_df = hist_df.copy()
        st.subheader(f"Pending bets ({len(p_df)})")
    else:
        p_df = _hist_filter_by_game_date(pending_df, date_iso)
        rows_df = _hist_filter_by_game_date(hist_df, date_iso)
        label = _history_tab_label(date_iso)
        st.subheader(f"Pending — {label} ({len(p_df)})")

    if rows_df.empty:
        st.caption("No rows for this filter.")
    else:
        _w, _p, _n = _totals_wagered_profit_for_rows(rows_df)
        _m1, _m2 = st.columns(2)
        with _m1:
            st.metric(
                "Wagered (this filter)",
                f"${_w:,.2f}",
                help=f"Sum of stake where **Placed bet** is on and stake > 0 ({_n} row(s)).",
            )
        with _m2:
            st.metric(
                "Profit / loss (this filter)",
                f"${_p:+,.2f}",
                help="Sum of the **profit** column (typically settled rows).",
            )
        st.divider()

    if p_df.empty:
        st.caption("No pending bets for this filter.")
    else:
        _render_history_cards(p_df, empty_text="No pending bets for this filter.", max_rows=20)
        with st.expander("View pending as table", expanded=False):
            st.dataframe(p_df, use_container_width=True, hide_index=True)

    if date_iso is None:
        st.subheader(f"All bet history ({len(rows_df)})")
    else:
        st.subheader(f"Bet history — {_history_tab_label(date_iso)} ({len(rows_df)})")

    if rows_df.empty:
        st.caption("No rows for this filter.")
    else:
        _render_history_cards(rows_df, empty_text="No rows for this filter.", max_rows=40)
        with st.expander("View full bet history as table", expanded=False):
            st.dataframe(rows_df, use_container_width=True, hide_index=True)

    if date_iso is None and not hist_df.empty:
        st.subheader("Totals by game date")
        _by_day = _history_totals_by_game_date(hist_df)
        if _by_day.empty:
            st.caption("No rows to aggregate.")
        else:
            with st.expander("View totals by game date table", expanded=False):
                st.dataframe(_by_day, use_container_width=True, hide_index=True)
            _gw = float(_by_day["Wagered"].sum())
            _gp = float(_by_day["Profit"].sum())
            st.caption(
                f"**All dates:** wagered **${_gw:,.2f}** · profit/loss **${_gp:+,.2f}**"
            )


def _format_game_start_time(game: dict) -> str | None:
    raw = str(game.get("game_datetime") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        local_dt = dt.astimezone()
        return local_dt.strftime("%I:%M %p").lstrip("0")
    except ValueError:
        return None


def _pitcher_hand_label(pitcher_name: str) -> str:
    """Best-effort throwing-hand label for probable pitcher names."""
    name = str(pitcher_name or "").strip()
    if not name or name == "TBD":
        return "—"
    raw = _cached_lookup_player(name)
    pitchers = filter_pitchers(raw)
    if not pitchers:
        return "—"
    pick = pitchers[0]
    target = name.lower()
    for p in pitchers:
        if str(p.get("fullName") or "").strip().lower() == target:
            pick = p
            break
    pid = pick.get("id")
    if pid is None:
        return "—"
    h = _cached_pitcher_throws(int(pid))
    if h == "R":
        return "RHP"
    if h == "L":
        return "LHP"
    return "—"


def _overwrite_open_prediction(
    *,
    user_id: int,
    game_date: str,
    pitcher: str,
    pitcher_team: str | None,
    opponent: str,
    betting_line: float,
    our_projected_k: float,
    recommended: bool,
    placed_bet: bool,
    bet_side: str,
    american_odds: float | None,
    stake: float | None,
    game_id: int | None,
    projected_bf: float | None = None,
    confidence_flag: str | None = None,
    tracker_tag: str | None = None,
) -> int | None:
    """Overwrite latest open row for same date/pitcher/opponent/line and tracker tag."""
    conn = sqlite3.connect(str(default_tracker_db_path()))
    try:
        init_db(conn)
        tag_norm = (tracker_tag or "").strip()
        cur = conn.execute(
            """
            SELECT id
            FROM predictions
            WHERE user_id = ? AND date = ? AND pitcher = ? AND opponent = ? AND betting_line = ?
              AND actual_k IS NULL
              AND COALESCE(tracker_tag, '') = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                int(user_id),
                game_date,
                pitcher.strip(),
                opponent.strip(),
                float(betting_line),
                tag_norm,
            ),
        )
        row = cur.fetchone()
        if row is None:
            return None
        rid = int(row[0])
        conn.execute(
            """
            UPDATE predictions
            SET
                our_projected_k = ?,
                recommended = ?,
                placed_bet = ?,
                pitcher_team = ?,
                bet_side = ?,
                american_odds = ?,
                stake = ?,
                game_id = ?,
                projected_bf = ?,
                confidence_flag = ?,
                tracker_tag = ?
            WHERE id = ? AND user_id = ?
            """,
            (
                float(our_projected_k),
                1 if recommended else 0,
                1 if placed_bet else 0,
                (pitcher_team or "").strip() or None,
                str(bet_side).strip().lower() if bet_side else None,
                float(american_odds) if american_odds is not None else None,
                float(stake) if stake is not None else None,
                game_id,
                float(projected_bf) if projected_bf is not None else None,
                (confidence_flag or "").strip() or None,
                tag_norm or None,
                rid,
                int(user_id),
            ),
        )
        conn.commit()
        return rid
    finally:
        conn.close()


def _delete_prediction_row(row_id: int, *, user_id: int, require_placed_bet: bool = True) -> bool:
    """Delete a tracker row by id.

    By default this only deletes rows where ``placed_bet=1`` (i.e. actual bets), to
    reduce accidental removal of informational rows.
    """
    conn = sqlite3.connect(str(default_tracker_db_path()))
    try:
        init_db(conn)
        if require_placed_bet:
            cur = conn.execute(
                """
                DELETE FROM predictions
                WHERE id = ? AND user_id = ? AND COALESCE(placed_bet, 0) = 1
                """,
                (int(row_id), int(user_id)),
            )
        else:
            cur = conn.execute(
                "DELETE FROM predictions WHERE id = ? AND user_id = ?",
                (int(row_id), int(user_id)),
            )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def _load_persisted_locks(user_id: int) -> None:
    """Read per-user lock file and restore all lock keys into session state."""
    path = _locks_file_for_user(user_id)
    if not path.is_file():
        return
    try:
        data = _json.loads(path.read_text())
    except (ValueError, OSError):
        return
    for k, v in data.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _save_persisted_locks(user_id: int) -> None:
    """Write every ``locked_line_*`` / ``locked_odds_*`` key to the user's lock file."""
    locks = {
        k: v
        for k, v in st.session_state.items()
        if isinstance(k, str) and (k.startswith("locked_line_") or k.startswith("locked_odds_"))
    }
    try:
        path = _locks_file_for_user(user_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_json.dumps(locks, indent=2))
    except OSError:
        pass


def _save_persisted_locks_for_session() -> None:
    uid = st.session_state.get(_AUTH_USER_ID)
    if uid is None:
        return
    _save_persisted_locks(int(uid))


st.set_page_config(
    page_title="MLB Matchup Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def _inject_custom_theme_css() -> None:
    css_path = _REPO_ROOT / "assets" / "style.css"
    if not css_path.exists():
        return
    try:
        css = css_path.read_text(encoding="utf-8")
    except OSError:
        return
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def _render_grass_header() -> None:
    st.markdown(
        _textwrap.dedent(
            """
            <div class="mlb-header">
                <div class="mlb-header-left">
                    <div class="mlb-badge">MLB</div>
                    <div>
                        <p class="mlb-header-title">Scouting Dashboard</p>
                        <p class="mlb-header-sub">Live matchup intelligence and strikeout value board</p>
                    </div>
                </div>
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def _render_green_banner() -> None:
    st.markdown(
        _textwrap.dedent(
            """
            <div class="green-banner">
                <span class="mlb-mini">MLB</span>
                <span>Scouting edge model active: live matchup and value feed ready.</span>
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def _render_top_nav() -> str:
    if _TOP_NAV_VIEW not in st.session_state:
        st.session_state[_TOP_NAV_VIEW] = "LIVE MATCHUPS"
    with st.container():
        st.markdown('<div class="top-nav">', unsafe_allow_html=True)
        c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 0.5])
        with c1:
            if st.button("LIVE MATCHUPS", key="topnav_live", use_container_width=True):
                st.session_state[_TOP_NAV_VIEW] = "LIVE MATCHUPS"
        with c2:
            if st.button("LIVE GAME CENTER", key="topnav_live_center", use_container_width=True):
                st.session_state[_TOP_NAV_VIEW] = "LIVE GAME CENTER"
        with c3:
            if st.button("BET LOG", key="topnav_betlog", use_container_width=True):
                st.session_state[_TOP_NAV_VIEW] = "BET LOG"
        with c4:
            if st.button("ANALYTICS", key="topnav_analytics", use_container_width=True):
                st.session_state[_TOP_NAV_VIEW] = "ANALYTICS"
        with c5:
            _render_account_menu_popover()
        st.markdown("</div>", unsafe_allow_html=True)
    return str(st.session_state.get(_TOP_NAV_VIEW, "LIVE MATCHUPS"))


@st.cache_data(ttl=300, show_spinner="Loading schedule…")
def _cached_scheduled_matchup(
    team_id: int, pitcher_full_name: str
) -> ScheduledMatchup | None:
    return get_scheduled_matchup_for_team(team_id, pitcher_full_name or None)


@st.cache_data(ttl=1800, show_spinner="Searching players…")
def _cached_lookup_player(name: str) -> list[dict]:
    return lookup_player(name)


@st.cache_data(ttl=1800, show_spinner=False)
def _cached_weighted_pitcher(player_id: int) -> WeightedPitcherStats | None:
    return get_weighted_stats(player_id)


@st.cache_data(ttl=86_400, show_spinner=False)
def _cached_league_doc(season: int) -> dict | None:
    return load_league_data(season)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_pitcher_throws(player_id: int) -> str | None:
    return get_pitcher_throws(player_id)


@st.cache_data(ttl=3600, show_spinner="Fetching odds from The Odds API…")
def _cached_odds_data(_refresh_counter: int = 0) -> dict:
    """Load pitcher strikeout odds.  ``_refresh_counter`` is bumped to bust cache."""
    try:
        return fetch_mlb_odds_data()
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Odds API unavailable: {exc}")
        return {}


@st.cache_data(ttl=300, show_spinner="Loading slate…")
def _cached_daily_schedule(slate_iso: str) -> list[dict]:
    d = datetime.strptime(slate_iso, "%Y-%m-%d").date()
    return get_daily_schedule(d)


def _is_live_game_status(status: str | None) -> bool:
    s = str(status or "").strip().lower()
    if not s:
        return False
    non_live = {
        "scheduled",
        "pre-game",
        "warmup",
        "delayed start",
        "final",
        "game over",
        "postponed",
        "cancelled",
        "completed early",
        "suspended",
    }
    return s not in non_live


@st.cache_data(ttl=30, show_spinner=False)
def _cached_live_boxscore(game_pk: int) -> dict:
    try:
        return statsapi.boxscore_data(int(game_pk)) or {}
    except Exception:
        return {}


@st.cache_data(ttl=30, show_spinner=False)
def _cached_live_linescore(game_pk: int) -> dict:
    try:
        feed = statsapi.get("game", {"gamePk": int(game_pk)})
    except Exception:
        return {}
    return feed.get("liveData", {}).get("linescore", {}) or {}


@st.cache_data(ttl=30, show_spinner=False)
def _cached_live_game_feed(game_pk: int) -> dict:
    try:
        return statsapi.get("game", {"gamePk": int(game_pk)}) or {}
    except Exception:
        return {}


def _outs_from_ip(ip_raw: str | None) -> int:
    s = str(ip_raw or "").strip()
    if not s or "." not in s:
        return 0
    whole_s, frac_s = s.split(".", 1)
    try:
        whole = int(whole_s)
        frac = int(frac_s[:1] or "0")
    except ValueError:
        return 0
    frac = max(0, min(frac, 2))
    return (whole * 3) + frac


def _extract_live_pitcher_rows(box: dict) -> list[dict]:
    player_info = box.get("playerInfo") or {}
    out: list[dict] = []
    for side_key in ("awayPitchers", "homePitchers"):
        for row in (box.get(side_key) or [])[1:]:
            pid = row.get("personId")
            try:
                ipid = int(pid) if pid is not None else None
            except (TypeError, ValueError):
                ipid = None
            full_name = None
            if ipid is not None:
                p = player_info.get(f"ID{ipid}") or {}
                full_name = str(p.get("fullName") or "").strip() or None
            disp = (
                full_name
                or str(row.get("namefield") or "").strip()
                or str(row.get("name") or "").strip()
            )
            if not disp:
                continue
            try:
                k = int(float(row.get("k") or 0))
            except (TypeError, ValueError):
                k = 0
            outs = _outs_from_ip(str(row.get("ip") or "0.0"))
            # BF estimate from visible line: outs + hits + walks.
            try:
                h = int(float(row.get("h") or 0))
            except (TypeError, ValueError):
                h = 0
            try:
                er = int(float(row.get("er") or 0))
            except (TypeError, ValueError):
                er = 0
            try:
                bb = int(float(row.get("bb") or 0))
            except (TypeError, ValueError):
                bb = 0
            bf_est = outs + h + bb
            ip_raw = str(row.get("ip") or "").strip()
            ip_disp = ip_raw if ip_raw else "—"
            out.append(
                {
                    "pitcher_name": disp,
                    "pitcher_name_norm": _norm_person_name(disp),
                    "k": k,
                    "bf_est": bf_est,
                    "ip": ip_disp,
                    "h": h,
                    "er": er,
                }
            )
    return out


def _live_baserunners_text(linescore: dict) -> str:
    offense = linescore.get("offense") or {}
    occupied: list[str] = []
    if offense.get("first"):
        occupied.append("1B")
    if offense.get("second"):
        occupied.append("2B")
    if offense.get("third"):
        occupied.append("3B")
    return " ".join(occupied) if occupied else "Empty"


def _feed_person_name(node: dict | None) -> str:
    if not node or not isinstance(node, dict):
        return "—"
    return str(node.get("fullName") or "").strip() or "—"


def _svg_baseball_diamond_html(
    *,
    on_first: bool,
    on_second: bool,
    on_third: bool,
    outs: int,
    balls: int,
    strikes: int,
) -> str:
    """Compact SVG infield + runners + out dots + count (MLB-style layout)."""
    o = max(0, min(2, int(outs)))
    b = max(0, min(3, int(balls)))
    s = max(0, min(2, int(strikes)))

    def base_dot(filled: bool, cx: float, cy: float) -> str:
        fill = "#fbbf24" if filled else "rgba(255,255,255,0.12)"
        stroke = "#fcd34d" if filled else "rgba(255,255,255,0.35)"
        return (
            f'<circle cx="{cx}" cy="{cy}" r="11" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="1.2"/>'
        )

    # Bases: 2B top, 3B left, 1B right, home bottom (view from standard broadcast)
    bx2, by2 = 110.0, 28.0
    bx3, by3 = 38.0, 100.0
    bx1, by1 = 182.0, 100.0
    bh, hh = 110.0, 168.0

    runners = ""
    runners += base_dot(on_first, bx1, by1)
    runners += base_dot(on_second, bx2, by2)
    runners += base_dot(on_third, bx3, by3)

    out_circles = ""
    for i in range(3):
        active = i < o
        fill = "#ef4444" if active else "rgba(255,255,255,0.12)"
        ox = 158 + i * 14
        out_circles += (
            f'<circle cx="{ox}" cy="14" r="5" fill="{fill}" '
            f'stroke="rgba(255,255,255,0.4)" stroke-width="0.8"/>'
        )

    return f"""<svg class="game-tracker-diamond-svg" viewBox="0 0 220 200" xmlns="http://www.w3.org/2000/svg" aria-label="Bases">
  <defs>
    <linearGradient id="infieldGrass" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#14532d"/>
      <stop offset="100%" style="stop-color:#166534"/>
    </linearGradient>
  </defs>
  <rect x="0" y="0" width="220" height="200" rx="12" fill="url(#infieldGrass)"/>
  <polygon points="{bh},{hh} {bx1},{by1} {bx2},{by2} {bx3},{by3}" fill="rgba(203,171,120,0.45)" stroke="rgba(255,255,255,0.55)" stroke-width="1.4"/>
  <line x1="{bh}" y1="{hh}" x2="{bx1}" y1="{by1}" stroke="rgba(255,255,255,0.5)" stroke-width="1"/>
  <line x1="{bh}" y1="{hh}" x2="{bx3}" y1="{by3}" stroke="rgba(255,255,255,0.5)" stroke-width="1"/>
  {runners}
  <polygon points="{bh},{hh + 6} {bh - 5},{hh - 2} {bh + 5},{hh - 2}" fill="#e2e8f0" stroke="#94a3b8" stroke-width="0.6"/>
  {out_circles}
  <text x="110" y="192" text-anchor="middle" class="diamond-meta-text" font-size="10" fill="#e2e8f0">Outs: {o}</text>
  <text x="12" y="192" text-anchor="start" class="diamond-meta-text" font-size="11" fill="#fef08a">B {b} — S {s}</text>
</svg>"""


def _html_linescore_grid(
    linescore: dict,
    *,
    away_ab: str,
    home_ab: str,
) -> str:
    """R H E by inning from linescore.innings + totals."""
    innings = sorted(
        linescore.get("innings") or [],
        key=lambda x: int(x.get("num") or 0),
    )
    teams_blk = linescore.get("teams") or {}
    away_tot = teams_blk.get("away") or {}
    home_tot = teams_blk.get("home") or {}

    def inn_runs(side: str, inn: dict) -> str:
        blk = inn.get(side) or {}
        try:
            r = int(blk.get("runs", 0))
        except (TypeError, ValueError):
            return "—"
        return str(r)

    ths = '<th class="ls-corner"></th>'
    for inn in innings:
        n = inn.get("num") or ""
        ths += f'<th class="ls-inn">{_html.escape(str(n))}</th>'
    ths += '<th class="ls-tot">R</th><th class="ls-tot">H</th><th class="ls-tot">E</th>'

    away_tds = f'<td class="ls-team">{_html.escape(away_ab or "Away")}</td>'
    home_tds = f'<td class="ls-team">{_html.escape(home_ab or "Home")}</td>'
    for inn in innings:
        away_tds += f'<td class="ls-cell">{_html.escape(inn_runs("away", inn))}</td>'
        home_tds += f'<td class="ls-cell">{_html.escape(inn_runs("home", inn))}</td>'

    def tot(v: object) -> str:
        try:
            return str(int(v))
        except (TypeError, ValueError):
            return "—"

    away_tds += (
        f'<td class="ls-total">{_html.escape(tot(away_tot.get("runs")))}</td>'
        f'<td class="ls-total">{_html.escape(tot(away_tot.get("hits")))}</td>'
        f'<td class="ls-total">{_html.escape(tot(away_tot.get("errors")))}</td>'
    )
    home_tds += (
        f'<td class="ls-total">{_html.escape(tot(home_tot.get("runs")))}</td>'
        f'<td class="ls-total">{_html.escape(tot(home_tot.get("hits")))}</td>'
        f'<td class="ls-total">{_html.escape(tot(home_tot.get("errors")))}</td>'
    )

    return (
        '<table class="game-tracker-linescore">'
        "<thead><tr>"
        f"{ths}"
        "</tr></thead><tbody>"
        f"<tr>{away_tds}</tr>"
        f"<tr>{home_tds}</tr>"
        "</tbody></table>"
    )


def _render_live_game_tracker_detail(
    game_pk: int,
    schedule_game: dict,
    *,
    id_abb: dict[int, str],
) -> None:
    """MLB.com-style tracker: linescore, diamond, batter/pitcher, count."""
    feed = _cached_live_game_feed(game_pk)
    box = _cached_live_boxscore(game_pk)
    ls = feed.get("liveData", {}).get("linescore", {}) or _cached_live_linescore(
        game_pk
    )
    if not ls and not box:
        st.warning("Could not load live data for this game.")
        return

    try:
        away_id = int(schedule_game.get("away_id") or (box.get("away") or {}).get("team", {}).get("id") or 0)
    except (TypeError, ValueError):
        away_id = 0
    try:
        home_id = int(schedule_game.get("home_id") or (box.get("home") or {}).get("team", {}).get("id") or 0)
    except (TypeError, ValueError):
        home_id = 0

    away_ab = id_abb.get(away_id, "") or str(schedule_game.get("away_abbrev") or "")[:4]
    home_ab = id_abb.get(home_id, "") or str(schedule_game.get("home_abbrev") or "")[:4]
    away_name = str(schedule_game.get("away_name") or away_ab or "Away")
    home_name = str(schedule_game.get("home_name") or home_ab or "Home")

    away_runs = (box.get("away") or {}).get("runs")
    home_runs = (box.get("home") or {}).get("runs")
    if away_runs is None:
        away_runs = schedule_game.get("away_score")
    if home_runs is None:
        home_runs = schedule_game.get("home_score")

    inn_state = str(ls.get("inningState") or "").strip()
    top = bool(ls.get("isTopInning"))
    inn_half = "Top" if top else "Bottom"
    if not inn_state:
        inn_state = "Live"
    cur_inn = ls.get("currentInning")
    outs = int(ls.get("outs") or 0)
    try:
        balls = int(ls.get("balls") or 0)
    except (TypeError, ValueError):
        balls = 0
    try:
        strikes = int(ls.get("strikes") or 0)
    except (TypeError, ValueError):
        strikes = 0

    offense = ls.get("offense") or {}
    defense = ls.get("defense") or {}
    on1 = bool(offense.get("first"))
    on2 = bool(offense.get("second"))
    on3 = bool(offense.get("third"))

    bat_team = offense.get("team") or {}
    bat_team_name = str(bat_team.get("name") or "").strip() or "Batting team"
    batter = _feed_person_name(offense.get("batter"))
    on_deck = _feed_person_name(offense.get("onDeck"))
    in_hole = _feed_person_name(offense.get("inHole"))
    pitcher = _feed_person_name(defense.get("pitcher"))
    pit_team = defense.get("team") or {}
    pit_team_name = str(pit_team.get("name") or "").strip() or "Fielding team"

    status = str(schedule_game.get("status") or "")
    if cur_inn is not None:
        try:
            ci = int(cur_inn)
            if 11 <= ci <= 13:
                suf = "th"
            else:
                suf = {1: "st", 2: "nd", 3: "rd"}.get(ci % 10, "th")
            inning_line = f"{inn_half} {ci}{suf}"
        except (TypeError, ValueError):
            inning_line = f"{inn_half} {cur_inn}"
    else:
        inning_line = status or "—"

    last_play = _last_play_text(feed) or ""

    diamond = _svg_baseball_diamond_html(
        on_first=on1,
        on_second=on2,
        on_third=on3,
        outs=outs,
        balls=balls,
        strikes=strikes,
    )
    linescore_html = _html_linescore_grid(ls, away_ab=away_ab, home_ab=home_ab)

    _away_cap = (
        f'<img class="game-tracker-cap" src="{_team_logo_url(away_id)}" '
        f'alt="{_html.escape(away_ab)} logo"/>'
        if away_id > 0
        else '<span class="game-tracker-cap-fallback"></span>'
    )
    _home_cap = (
        f'<img class="game-tracker-cap" src="{_team_logo_url(home_id)}" '
        f'alt="{_html.escape(home_ab)} logo"/>'
        if home_id > 0
        else '<span class="game-tracker-cap-fallback"></span>'
    )
    header_html = f"""
    <div class="game-tracker-header">
      <div class="game-tracker-teams">
        <div class="game-tracker-team">
          {_away_cap}
          <span class="game-tracker-ab">{_html.escape(away_ab)}</span>
          <span class="game-tracker-score">{away_runs if away_runs is not None else "—"}</span>
        </div>
        <span class="game-tracker-at">@</span>
        <div class="game-tracker-team">
          {_home_cap}
          <span class="game-tracker-ab">{_html.escape(home_ab)}</span>
          <span class="game-tracker-score">{home_runs if home_runs is not None else "—"}</span>
        </div>
      </div>
      <div class="game-tracker-inning-pill">{_html.escape(inning_line)} · {_html.escape(inn_state)}</div>
      <p class="game-tracker-sub">{_html.escape(away_name)} at {_html.escape(home_name)}</p>
    </div>
    """

    matchup_html = f"""
    <div class="game-tracker-matchup">
      <div class="game-tracker-panel at-bat">
        <p class="game-tracker-panel-title">At bat · {_html.escape(bat_team_name)}</p>
        <p class="game-tracker-player">{_html.escape(batter)}</p>
        <p class="game-tracker-deck">On deck: {_html.escape(on_deck)}</p>
        <p class="game-tracker-deck">In the hole: {_html.escape(in_hole)}</p>
      </div>
      <div class="game-tracker-panel pitching">
        <p class="game-tracker-panel-title">Pitching · {_html.escape(pit_team_name)}</p>
        <p class="game-tracker-player">{_html.escape(pitcher)}</p>
      </div>
    </div>
    """

    st.markdown(header_html, unsafe_allow_html=True)
    c1, c2 = st.columns([1, 1.35])
    with c1:
        st.markdown(
            f'<div class="game-tracker-diamond-wrap">{diamond}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(matchup_html, unsafe_allow_html=True)
    with c2:
        st.markdown(
            '<p class="game-tracker-ls-title">Linescore</p>',
            unsafe_allow_html=True,
        )
        st.markdown(linescore_html, unsafe_allow_html=True)
    if last_play:
        st.markdown(
            f'<div class="game-tracker-last-play"><span class="game-tracker-last-label">Last play</span> {_html.escape(last_play)}</div>',
            unsafe_allow_html=True,
        )
    st.caption(
        "Live data from MLB StatsAPI. Count, outs, and baserunners update with the official feed."
    )


def _poisson_tail_prob(lam: float, need_at_least: int) -> float:
    if need_at_least <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    upto = need_at_least - 1
    cdf = 0.0
    for k in range(0, upto + 1):
        cdf += (math.exp(-lam) * (lam**k)) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _poisson_at_most_prob(lam: float, k_max: int) -> float:
    """P(X <= k_max) for X ~ Poisson(lam). Used for under (cap on remaining Ks)."""
    if k_max < 0:
        return 0.0
    if lam <= 0:
        return 1.0
    cdf = 0.0
    for k in range(0, k_max + 1):
        cdf += (math.exp(-lam) * (lam**k)) / math.factorial(k)
    return max(0.0, min(1.0, cdf))


def _min_strikeouts_to_win_over(line: float) -> int:
    """Smallest integer K that clears a standard over (K > line)."""
    return int(math.floor(float(line) + 1e-9)) + 1


def _max_strikeouts_to_win_under(line: float) -> int:
    """Largest integer K that clears a standard under (K < line)."""
    return int(math.ceil(float(line) - 1e-9)) - 1


def _sweat_card_bet_readout(row: dict) -> tuple[str, str, str, str]:
    """Odds text, wager text, total return if win, profit if win. Use — when unknown."""
    odds_s = wager_s = payout_s = profit_s = "—"
    odds_raw = row.get("american_odds")
    stake_raw = row.get("stake")
    oi: int | None = None
    stake_f: float | None = None
    if odds_raw is not None:
        try:
            oi = int(round(float(odds_raw)))
        except (TypeError, ValueError):
            oi = None
        if oi is not None and oi != 0:
            odds_s = fmt_american(oi)
    if stake_raw is not None:
        try:
            stake_f = float(stake_raw)
        except (TypeError, ValueError):
            stake_f = None
        if stake_f is not None and stake_f > 0:
            wager_s = f"${stake_f:,.2f}"
    if oi is not None and oi != 0 and stake_f is not None and stake_f > 0:
        dec = _american_to_decimal_odds(float(oi))
        total = stake_f * dec
        prof = total - stake_f
        payout_s = f"${total:,.2f}"
        profit_s = f"+${prof:,.2f}" if prof >= 0 else f"${prof:,.2f}"
    return odds_s, wager_s, payout_s, profit_s


def _game_progress_outs(linescore: dict) -> int:
    try:
        inn = int(linescore.get("currentInning") or 1)
    except (TypeError, ValueError):
        inn = 1
    try:
        outs = int(linescore.get("outs") or 0)
    except (TypeError, ValueError):
        outs = 0
    # Rough global game progress proxy: innings completed + current outs.
    return max(0, ((inn - 1) * 3) + outs)


def _live_win_probability_pct(
    *,
    cur_k: int,
    target_line: float,
    cur_bf_est: int,
    projected_bf: float | None,
    linescore: dict,
    bet_side: str = "over",
) -> tuple[float, int]:
    """Modeled win likelihood for the logged side (over = enough Ks; under = stay at/below cap)."""
    side = str(bet_side or "over").strip().lower()
    if side not in ("over", "under"):
        side = "over"

    game_outs_left = max(0, 27 - _game_progress_outs(linescore))
    if projected_bf is not None:
        rem_bf = max(0.0, float(projected_bf) - float(cur_bf_est))
        rem_outs_from_bf = int(round(rem_bf * 0.72))
        rem_outs = max(0, min(rem_outs_from_bf, game_outs_left))
    else:
        rem_outs = max(0, int(round(game_outs_left * 0.6)))

    k_per_out = float(cur_k) / float(max(cur_bf_est, 1))
    k_per_out = max(0.08, min(0.5, k_per_out))
    lam_remaining = rem_outs * k_per_out

    if side == "over":
        min_win = _min_strikeouts_to_win_over(float(target_line))
        need_k = max(0, min_win - int(cur_k))
        if need_k <= 0:
            return 100.0, rem_outs
        pct = 100.0 * _poisson_tail_prob(lam_remaining, need_k)
        return pct, rem_outs

    # Under: win if final K <= max_under (e.g. line 5.5 → K <= 5).
    max_under = _max_strikeouts_to_win_under(float(target_line))
    if int(cur_k) > max_under:
        return 0.0, rem_outs
    allowed_more = max_under - int(cur_k)
    pct = 100.0 * _poisson_at_most_prob(lam_remaining, allowed_more)
    return pct, rem_outs


def _status_is_final(status: str | None) -> bool:
    s = str(status or "").strip().lower()
    return s in {"final", "game over", "completed early"}


def _current_defensive_pitcher_name(feed: dict) -> str | None:
    try:
        p = (
            feed.get("liveData", {})
            .get("linescore", {})
            .get("defense", {})
            .get("pitcher", {})
        )
        name = str(p.get("fullName") or "").strip()
        return name or None
    except Exception:
        return None


def _last_play_text(feed: dict) -> str | None:
    try:
        s = (
            feed.get("liveData", {})
            .get("plays", {})
            .get("currentPlay", {})
            .get("result", {})
            .get("description")
        )
        txt = str(s or "").strip()
        return txt or None
    except Exception:
        return None


def _maybe_auto_settle_tracker(min_interval_sec: float = 60.0) -> int:
    now = float(_time.time())
    last = float(st.session_state.get(_AUTO_SETTLE_LAST_RUN_TS, 0.0) or 0.0)
    if (now - last) < float(min_interval_sec):
        return 0
    st.session_state[_AUTO_SETTLE_LAST_RUN_TS] = now
    try:
        updated = int(
            update_results(
                db_path=default_tracker_db_path(),
                settle_all_pending=True,
            )
        )
    except Exception:
        updated = 0
    if updated > 0:
        st.session_state[_AUTO_SETTLE_UPDATED_KEY] = int(
            st.session_state.get(_AUTO_SETTLE_UPDATED_KEY, 0)
        ) + updated
    return updated


def _pitcher_team_id_for_sweat_card(
    *,
    pitcher_team: str | None,
    pitcher_name: str,
    game_meta: dict | None,
) -> int | None:
    """Resolve MLB team id for cap logo: tracker ``pitcher_team``, else probable match."""
    pt = (pitcher_team or "").strip()
    if pt:
        tid = get_canonical_team_id_map().get(pt)
        if tid is not None:
            return int(tid)
    if not game_meta:
        return None
    pn = _norm_person_name(pitcher_name)
    ap_raw = str(game_meta.get("away_p") or "").strip()
    hp_raw = str(game_meta.get("home_p") or "").strip()
    ap = _norm_person_name(ap_raw) if ap_raw else ""
    hp = _norm_person_name(hp_raw) if hp_raw else ""
    try:
        aid = int(game_meta.get("away_id") or 0)
        hid = int(game_meta.get("home_id") or 0)
    except (TypeError, ValueError):
        return None
    if ap and pn == ap and aid > 0:
        return aid
    if hp and pn == hp and hid > 0:
        return hid
    return None


def _open_tracker_bets_for_live_games(game_ids: list[int], *, user_id: int) -> list[dict]:
    """Unsettled tracker rows (``actual_k`` still null) for the given MLB game PKs.

    Includes both paper logs and placed wagers (``placed_bet`` 0 or 1) so Live Game
    Center stays in sync when users mark bets as placed.
    """
    if not game_ids:
        return []
    dbp = default_tracker_db_path()
    if not dbp.exists():
        return []
    conn = sqlite3.connect(str(dbp))
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        placeholders = ",".join("?" for _ in game_ids)
        rows = conn.execute(
            f"""
            SELECT
                id,
                game_id,
                pitcher,
                pitcher_team,
                betting_line,
                projected_bf,
                created_at,
                placed_bet,
                bet_side,
                american_odds,
                stake
            FROM predictions
            WHERE user_id = ? AND game_id IN ({placeholders})
              AND actual_k IS NULL
            ORDER BY id DESC
            """,
            (int(user_id),) + tuple(int(g) for g in game_ids),
        ).fetchall()
        seen: set[tuple[int, str]] = set()
        uniq: list[dict] = []
        for r in rows:
            key = (int(r["game_id"]), _norm_person_name(str(r["pitcher"])))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(dict(r))
        return uniq
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _render_live_game_center() -> None:
    st.subheader("LIVE GAME CENTER")
    _auto_n = _maybe_auto_settle_tracker()
    if _auto_n > 0:
        st.success(f"Auto-settlement updated {_auto_n} completed bet(s).")
    if _st_autorefresh is not None:
        _st_autorefresh(interval=30_000, key="live_game_center_autorefresh")
        st.caption("Auto-refreshing every 30 seconds.")
    rc1, rc2 = st.columns([1, 1])
    with rc1:
        if st.button("Refresh Live Stats", key="live_refresh_button", use_container_width=True):
            _cached_live_boxscore.clear()
            _cached_live_linescore.clear()
            _cached_live_game_feed.clear()
            _cached_daily_schedule.clear()
            st.rerun()
    with rc2:
        st.caption("Live MLB feed: scores, inning state, and starter lines.")

    today_iso = date.today().strftime("%Y-%m-%d")
    all_games = _cached_daily_schedule(today_iso)
    live_games = [g for g in all_games if _is_live_game_status(g.get("status"))]
    id_abb_live = get_id_to_abbrev()

    detail_gid: int | None = None
    _dr = st.session_state.get(_LIVE_CENTER_DETAIL_GID)
    if _dr is not None:
        try:
            detail_gid = int(_dr)
        except (TypeError, ValueError):
            detail_gid = None
    g_detail: dict | None = None
    if detail_gid is not None and detail_gid > 0:
        g_detail = next(
            (g for g in all_games if int(g.get("game_id") or 0) == detail_gid),
            None,
        )
        if g_detail is None:
            st.session_state[_LIVE_CENTER_DETAIL_GID] = None
            detail_gid = None

    if not live_games and detail_gid is None:
        st.info("No games currently in progress.")
        return

    live_game_ids: list[int] = []
    live_pitcher_index: dict[int, list[dict]] = {}
    linescore_index: dict[int, dict] = {}
    feed_index: dict[int, dict] = {}
    status_index: dict[int, str] = {}
    game_meta_by_id: dict[int, dict[str, object]] = {}
    live_cards: list[dict[str, object]] = []

    for g in live_games:
        try:
            gid = int(g.get("game_id") or 0)
        except (TypeError, ValueError):
            continue
        if gid <= 0:
            continue
        live_game_ids.append(gid)
        status_index[gid] = str(g.get("status") or "")
        try:
            game_meta_by_id[gid] = {
                "away_id": int(g.get("away_id") or 0),
                "home_id": int(g.get("home_id") or 0),
                "away_p": str(g.get("away_probable_pitcher") or "").strip(),
                "home_p": str(g.get("home_probable_pitcher") or "").strip(),
            }
        except (TypeError, ValueError):
            game_meta_by_id[gid] = {
                "away_id": 0,
                "home_id": 0,
                "away_p": str(g.get("away_probable_pitcher") or "").strip(),
                "home_p": str(g.get("home_probable_pitcher") or "").strip(),
            }
        box = _cached_live_boxscore(gid)
        live_pitcher_index[gid] = _extract_live_pitcher_rows(box)
        feed = _cached_live_game_feed(gid)
        feed_index[gid] = feed
        linescore = (
            feed.get("liveData", {}).get("linescore", {}) or _cached_live_linescore(gid)
        )
        linescore_index[gid] = linescore
        away_ab = str(g.get("away_abbrev") or g.get("away_name") or "AWAY")
        home_ab = str(g.get("home_abbrev") or g.get("home_name") or "HOME")
        away_runs = (box.get("away") or {}).get("runs")
        home_runs = (box.get("home") or {}).get("runs")
        if away_runs is None:
            away_runs = g.get("away_score")
        if home_runs is None:
            home_runs = g.get("home_score")
        inning_state = str(linescore.get("inningState") or "Live").strip()
        inning_num = linescore.get("currentInning")
        outs = linescore.get("outs")
        base_txt = _live_baserunners_text(linescore)
        card_body = _textwrap.dedent(
            f"""
            <div class="live-mini-card">
                <div class="live-mini-head">
                    <span>{_html.escape(away_ab)} @ {_html.escape(home_ab)}</span>
                    <span class="live-badge-red"><span class="live-dot-red"></span>LIVE</span>
                </div>
                <div class="live-mini-score">{away_runs if away_runs is not None else "—"} - {home_runs if home_runs is not None else "—"}</div>
                <div class="live-mini-meta">
                    <span>Inning: {inning_state} {inning_num if inning_num is not None else "—"}</span>
                    <span>Outs: {outs if outs is not None else "—"}</span>
                    <span>Runners: {base_txt}</span>
                </div>
            </div>
            """
        ).strip()
        live_cards.append({"gid": gid, "html": card_body})

    if detail_gid is not None and g_detail is not None:
        st.markdown("### Game tracker")
        if st.button("← All live games", key="live_tracker_back", use_container_width=False):
            st.session_state[_LIVE_CENTER_DETAIL_GID] = None
            st.rerun()
        _render_live_game_tracker_detail(
            detail_gid, g_detail, id_abb=id_abb_live
        )
        st.divider()

    if live_cards and detail_gid is None:
        st.markdown("##### Live games")
        st.caption("Open the full tracker for linescore, field, batter, pitcher, and count.")
        _i = 0
        while _i < len(live_cards):
            _chunk = live_cards[_i : _i + 4]
            _cols = st.columns(len(_chunk))
            for _col, _lc in zip(_cols, _chunk):
                with _col:
                    st.markdown(str(_lc["html"]), unsafe_allow_html=True)
                    _gpk = int(_lc["gid"])
                    if st.button(
                        "Open tracker",
                        key=f"lc_open_{_gpk}",
                        use_container_width=True,
                    ):
                        st.session_state[_LIVE_CENTER_DETAIL_GID] = _gpk
                        st.rerun()
            _i += 4

    sweat_rows = _open_tracker_bets_for_live_games(
        live_game_ids, user_id=int(st.session_state[_AUTH_USER_ID])
    )
    if not sweat_rows:
        st.caption("No open tracker bets for today's live games.")
        return

    st.markdown("### Sweat Tracker")
    for row in sweat_rows:
        gid = int(row.get("game_id") or 0)
        pname = str(row.get("pitcher") or "").strip()
        target_line = float(row.get("betting_line") or 0.0)
        projected_bf = row.get("projected_bf")
        p_norm = _norm_person_name(pname)
        p_rows = live_pitcher_index.get(gid, [])
        p_match = next((r for r in p_rows if r["pitcher_name_norm"] == p_norm), None)
        cur_k = int(p_match["k"]) if p_match is not None else 0
        cur_bf_est = int(p_match["bf_est"]) if p_match is not None else 0
        ip_s = str(p_match["ip"]) if p_match is not None else "—"
        h_s = str(int(p_match["h"])) if p_match is not None else "—"
        er_s = str(int(p_match["er"])) if p_match is not None else "—"
        if projected_bf is None:
            rem_outs = "N/A"
        else:
            try:
                rem_bf = max(0.0, float(projected_bf) - float(cur_bf_est))
                rem_outs = f"{int(round(rem_bf * 0.72))}"
            except (TypeError, ValueError):
                rem_outs = "N/A"
        linescore = linescore_index.get(gid, {})
        feed = feed_index.get(gid, {})
        try:
            projected_bf_num = float(projected_bf) if projected_bf is not None else None
        except (TypeError, ValueError):
            projected_bf_num = None
        _prop_side = str(row.get("bet_side") or "over").strip().lower()
        if _prop_side not in ("over", "under"):
            _prop_side = "over"
        live_prob, rem_outs_v = _live_win_probability_pct(
            cur_k=cur_k,
            target_line=target_line,
            cur_bf_est=cur_bf_est,
            projected_bf=projected_bf_num,
            linescore=linescore,
            bet_side=_prop_side,
        )
        rem_outs = f"{rem_outs_v}" if rem_outs_v >= 0 else "N/A"
        defensive_pitcher = _current_defensive_pitcher_name(feed)
        is_final = _status_is_final(status_index.get(gid))
        pitcher_pulled = (
            defensive_pitcher is not None
            and _norm_person_name(defensive_pitcher) != p_norm
        )
        outing_done = is_final or pitcher_pulled
        _min_over = _min_strikeouts_to_win_over(target_line)
        _max_under = _max_strikeouts_to_win_under(target_line)
        if _prop_side == "over":
            is_won = cur_k >= _min_over
            is_lost = outing_done and cur_k < _min_over
        else:
            is_won = outing_done and cur_k <= _max_under
            is_lost = cur_k > _max_under
        card_class = "sweat-card"
        if is_won:
            card_class += " success"
        elif is_lost:
            card_class += " pulled"
        badge_class = "live-prob-badge high" if live_prob >= 60.0 else ("live-prob-badge low" if live_prob <= 40.0 else "live-prob-badge mid")
        last_play = _last_play_text(feed) or "Awaiting next play..."
        side_lbl = _fmt_tracker_bet_side(row.get("bet_side"))
        _line_disp = (
            f"O {target_line:.1f}"
            if _prop_side == "over"
            else f"U {target_line:.1f}"
        )
        odds_disp, wager_disp, payout_disp, profit_disp = _sweat_card_bet_readout(row)
        bet_bits: list[str] = []
        if odds_disp != "—":
            bet_bits.append(
                f'<span class="sweat-bet-item">Odds <strong>{_html.escape(odds_disp)}</strong></span>'
            )
        if wager_disp != "—":
            bet_bits.append(
                f'<span class="sweat-bet-item">Wager <strong>{_html.escape(wager_disp)}</strong></span>'
            )
        if payout_disp != "—":
            prof_chunk = (
                f' (<strong>{_html.escape(profit_disp)}</strong> profit)'
                if profit_disp != "—"
                else ""
            )
            bet_bits.append(
                f'<span class="sweat-bet-item">Payout if win <strong>{_html.escape(payout_disp)}</strong>{prof_chunk}</span>'
            )
        bet_strip_html = (
            " · ".join(bet_bits)
            if bet_bits
            else '<span class="sweat-bet-muted">Log American odds and stake on the bet to see payout.</span>'
        )
        sweat_tid = _pitcher_team_id_for_sweat_card(
            pitcher_team=row.get("pitcher_team"),
            pitcher_name=pname,
            game_meta=game_meta_by_id.get(gid),
        )
        if sweat_tid is not None and sweat_tid > 0:
            _ab = id_abb_live.get(sweat_tid, "")
            _club = canonical_team_for_id(sweat_tid) or ""
            _alt = _ab or _club or "Team"
            sweat_logo = (
                f'<div class="sweat-cap-wrap">'
                f'<img class="sweat-cap-img" src="{_team_logo_url(sweat_tid)}" '
                f'alt="{_html.escape(_alt)} logo">'
                f"</div>"
            )
        else:
            sweat_logo = '<div class="sweat-cap-wrap sweat-cap-missing" aria-hidden="true"></div>'
        st.markdown(
            _textwrap.dedent(
                f"""
                <div class="{card_class}">
                    <div class="sweat-title"><span class="live-dot-red"></span> SWEAT CARD</div>
                    <div class="{badge_class}">{live_prob:.0f}% Likelihood</div>
                    <div class="sweat-grid">
                        <div class="sweat-pitcher-cell">
                            {sweat_logo}
                            <div class="sweat-pitcher-names">
                                <p class="scout-label">Pitcher</p>
                                <p class="metric-digital-sm">{_html.escape(pname)}</p>
                            </div>
                        </div>
                        <div><p class="scout-label">Current Ks</p><p class="metric-digital-sm">{cur_k}</p></div>
                        <div><p class="scout-label">Prop line</p><p class="metric-digital-sm">{_html.escape(_line_disp)}</p></div>
                        <div><p class="scout-label">Remaining Outs</p><p class="metric-digital-sm">{rem_outs}</p></div>
                    </div>
                    <div class="sweat-grid sweat-grid-ip">
                        <div><p class="scout-label">IP</p><p class="metric-digital-sm">{_html.escape(ip_s)}</p></div>
                        <div><p class="scout-label">H</p><p class="metric-digital-sm">{_html.escape(h_s)}</p></div>
                        <div><p class="scout-label">ER</p><p class="metric-digital-sm">{_html.escape(er_s)}</p></div>
                        <div><p class="scout-label">Bet side</p><p class="metric-digital-sm">{_html.escape(side_lbl)}</p></div>
                    </div>
                    <div class="sweat-bet-strip">{bet_strip_html}</div>
                    <div class="sweat-last-play">{_html.escape(last_play)}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


@st.cache_data(ttl=25, show_spinner=False)
def _cached_live_starter_lines(
    game_pk: int,
) -> tuple[LiveStarterPitchingLine | None, LiveStarterPitchingLine | None]:
    """Short-TTL cache so the dashboard refreshes live K / BF without API spam."""
    return live_game_starter_pitching_lines(game_pk)


def _format_live_starter_line(line: LiveStarterPitchingLine) -> str:
    parts: list[str] = [f"**{line.strikeouts} K**", f"{line.batters_faced} BF"]
    if line.innings_pitched:
        parts.append(f"{line.innings_pitched} IP")
    if line.hits is not None:
        parts.append(f"{line.hits} H")
    if line.earned_runs is not None:
        parts.append(f"{line.earned_runs} ER")
    elif line.runs is not None:
        parts.append(f"{line.runs} R")
    if line.base_on_balls is not None:
        parts.append(f"{line.base_on_balls} BB")
    return " · ".join(parts)


def _opponent_canonical_for_starter_side(game: dict, side: str) -> str | None:
    """``MLB_TEAMS`` key for the lineup the away or home starter faces (same as schedule helpers)."""
    if side == "away":
        return schedule_club_name_to_canonical(game.get("home_name"))
    return schedule_club_name_to_canonical(game.get("away_name"))


def _render_value_verdict(
    headline: str, detail: str, *, recommend: bool, edge_pct: float
) -> None:
    body = f"**{headline}**\n\n{detail}"
    if recommend:
        st.success(body)
    elif edge_pct < 0:
        st.warning(body)
    else:
        st.info(body)


def _american_to_decimal_odds(american_odds: float) -> float:
    if american_odds > 0:
        return 1.0 + (american_odds / 100.0)
    if american_odds < 0:
        return 1.0 + (100.0 / abs(american_odds))
    return 1.0


def _render_performance_dashboard(hist_df: pd.DataFrame) -> None:
    """Core model diagnostics + beta analytics for settled bets."""
    req = {"our_projected_k", "actual_k"}
    if not req.issubset(hist_df.columns):
        st.caption("Tracker is missing expected columns.")
        return
    settled = hist_df[hist_df["actual_k"].notna()].copy()
    if settled.empty:
        st.caption("No settled games yet — use **Fetch Results** after games finish.")
        return
    lam = settled["our_projected_k"].astype(float)
    actk = settled["actual_k"].astype(float)
    mae_k = float((lam - actk).abs().mean())
    st.metric("Model accuracy (MAE): |λ − actual K|", f"{mae_k:.2f} strikeouts")

    placed_settled = settled[
        (settled["placed_bet"].fillna(0).astype(int) == 1)
        & settled["result"].isin(["Win", "Loss"])
    ].copy()
    if not placed_settled.empty:
        losses = placed_settled[placed_settled["result"] == "Loss"].copy()
        if not losses.empty:
            hook_losses = losses[
                (pd.to_numeric(losses["actual_k"], errors="coerce") - pd.to_numeric(losses["betting_line"], errors="coerce"))
                .abs()
                .sub(0.5)
                .abs()
                < 1e-9
            ]
            hook_n = int(len(hook_losses))
            hook_pct = (100.0 * hook_n / float(len(losses))) if len(losses) else 0.0
            st.metric(
                "Hook Losses (exactly 0.5 K)",
                f"{hook_n}",
                help=f"{hook_pct:.1f}% of settled losses.",
            )

        ev_rows = placed_settled.copy()
        ev_rows["stake_num"] = pd.to_numeric(ev_rows["stake"], errors="coerce")
        ev_rows = ev_rows[ev_rows["stake_num"].notna() & (ev_rows["stake_num"] > 0)].copy()
        if not ev_rows.empty:
            expected_profit = 0.0
            actual_profit = 0.0
            total_staked = float(ev_rows["stake_num"].sum())
            for _, r in ev_rows.iterrows():
                side = str(r.get("bet_side") or "").strip().lower()
                if side not in ("over", "under"):
                    continue
                try:
                    projection = float(r["our_projected_k"])
                    line = float(r["betting_line"])
                    odds = float(r["american_odds"])
                    stake = float(r["stake_num"])
                except (TypeError, ValueError, KeyError):
                    continue
                if odds == 0 or stake <= 0:
                    continue
                p_win = (
                    poisson_over_probability_pct(projection, line) / 100.0
                    if side == "over"
                    else poisson_under_probability_pct(projection, line) / 100.0
                )
                win_profit = _profit_from_result(
                    result="Win",
                    american_odds=odds,
                    stake=stake,
                )
                if win_profit is None:
                    continue
                expected_profit += (p_win * float(win_profit)) - ((1.0 - p_win) * stake)
                row_profit = r.get("profit")
                if pd.notna(row_profit):
                    actual_profit += float(row_profit)
                else:
                    realized = _profit_from_result(
                        result=str(r.get("result") or ""),
                        american_odds=odds,
                        stake=stake,
                    )
                    actual_profit += float(realized) if realized is not None else 0.0
            if total_staked > 0:
                expected_roi = 100.0 * expected_profit / total_staked
                actual_roi = 100.0 * actual_profit / total_staked
                st.caption(
                    f"Expected ROI (from model edge): **{expected_roi:+.2f}%** vs "
                    f"Actual ROI: **{actual_roi:+.2f}%** (stake-aligned sample)."
                )

        if "closing_american_odds" in placed_settled.columns:
            clv = placed_settled.copy()
            clv["bet_odds"] = pd.to_numeric(clv["american_odds"], errors="coerce")
            clv["close_odds"] = pd.to_numeric(clv["closing_american_odds"], errors="coerce")
            clv = clv[
                clv["bet_odds"].notna()
                & clv["close_odds"].notna()
                & (clv["bet_odds"] != 0)
                & (clv["close_odds"] != 0)
            ].copy()
            if not clv.empty:
                clv["implied_bet_pct"] = clv["bet_odds"].map(american_odds_to_implied_probability_pct)
                clv["implied_close_pct"] = clv["close_odds"].map(american_odds_to_implied_probability_pct)
                clv["clv_pct"] = clv["implied_close_pct"] - clv["implied_bet_pct"]
                clv_avg = float(clv["clv_pct"].mean())
                clv_hit = float((clv["clv_pct"] > 0).mean() * 100.0)
                st.metric(
                    "CLV (implied prob, avg)",
                    f"{clv_avg:+.2f}%",
                    help=f"Positive means your bet-time price beat close. CLV win rate: {clv_hit:.1f}%.",
                )
                clv["_dt"] = pd.to_datetime(clv["date"], errors="coerce")
                clv = clv[clv["_dt"].notna()].sort_values(["_dt", "id"])
                if not clv.empty:
                    clv_line = (
                        alt.Chart(clv)
                        .mark_line(point=True)
                        .encode(
                            x=alt.X("_dt:T", title="Date"),
                            y=alt.Y("clv_pct:Q", title="CLV (% implied prob)"),
                            tooltip=[
                                alt.Tooltip("date:N"),
                                alt.Tooltip("pitcher:N"),
                                alt.Tooltip("bet_odds:Q", format=".0f", title="Bet odds"),
                                alt.Tooltip("close_odds:Q", format=".0f", title="Close odds"),
                                alt.Tooltip("clv_pct:Q", format="+.2f", title="CLV %"),
                            ],
                        )
                        .properties(height=220, title="Market Movement (CLV) Over Time")
                    )
                    st.altair_chart(clv_line, use_container_width=True)
            else:
                st.caption(
                    "CLV: add **closing odds** to settled bet rows to track bet-time vs first-pitch price."
                )

    if "projected_bf" in settled.columns and "actual_bf" in settled.columns:
        m = settled["projected_bf"].notna() & settled["actual_bf"].notna()
        if m.any():
            wdf = settled.loc[m].copy()
            pbf = wdf["projected_bf"].astype(float)
            abf = wdf["actual_bf"].astype(float)
            delta = abf - pbf
            mae_bf = float(delta.abs().mean())
            mean_delta = float(delta.mean())
            pct_actual_higher = float((delta > 0).mean() * 100.0)
            st.metric(
                "Workload accuracy (MAE): |projected BF − actual BF|",
                f"{mae_bf:.1f} batters faced",
            )
            c_w1, c_w2 = st.columns(2)
            with c_w1:
                st.metric(
                    "Workload bias (mean actual − projected)",
                    f"{mean_delta:+.2f} BF",
                    help="Positive = starters facing more batters than we projected on average.",
                )
            with c_w2:
                st.metric(
                    "Games with actual BF above projection",
                    f"{pct_actual_higher:.0f}%",
                    help="Share of settled rows where actual workload exceeded the logged projection.",
                )
            st.caption(f"Workload sample: **{int(m.sum())}** settled game(s) with both values.")
            if int(m.sum()) >= 3 and mean_delta >= 1.0 and pct_actual_higher >= 55.0:
                st.success(
                    "**Leash read:** Actual BF is running **above** projections consistently — "
                    "workload is no longer being undershot by the model; you can treat the old "
                    "conservative volume haircut as **off** for interpretation."
                )
            elif int(m.sum()) >= 3 and mean_delta <= -1.0:
                st.info(
                    "**Leash read:** On average, **actual BF ≤ projected** — projections are not "
                    "systematically too low vs realized workload in this sample."
                )
            bf_scatter = wdf.assign(
                projected_bf=pbf,
                actual_bf=abf,
                bf_above_proj=(abf > pbf).map({True: "Actual > projected", False: "Actual ≤ projected"}),
            )
            bf_ts = wdf.copy()
            bf_ts["_dt"] = pd.to_datetime(bf_ts["date"], errors="coerce")
            bf_ts = bf_ts[bf_ts["_dt"].notna()].sort_values(["_dt", "id"])
            if not bf_ts.empty:
                bf_long = pd.concat(
                    [
                        bf_ts[["_dt", "projected_bf"]].rename(columns={"projected_bf": "bf"}).assign(series="Projected BF"),
                        bf_ts[["_dt", "actual_bf"]].rename(columns={"actual_bf": "bf"}).assign(series="Actual BF"),
                    ],
                    ignore_index=True,
                )
                bf_time = (
                    alt.Chart(bf_long)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("_dt:T", title="Date"),
                        y=alt.Y("bf:Q", title="Batters Faced"),
                        color=alt.Color("series:N", title=""),
                        tooltip=[
                            alt.Tooltip("_dt:T", title="Date"),
                            alt.Tooltip("series:N", title="Series"),
                            alt.Tooltip("bf:Q", format=".1f", title="BF"),
                        ],
                    )
                    .properties(height=220, title="Volume Decay Check: Actual vs Projected BF")
                )
                st.altair_chart(bf_time, use_container_width=True)
            bf_diag_max = max(float(pbf.max()), float(abf.max()), 30.0)
            bf_line_df = pd.DataFrame({"x": [0.0, bf_diag_max], "y": [0.0, bf_diag_max]})
            bf_line = (
                alt.Chart(bf_line_df)
                .mark_line(color="#94a3b8", strokeDash=[4, 4])
                .encode(
                    x=alt.X("x:Q", title="Projected BF (logged)"),
                    y=alt.Y("y:Q", title="Actual BF (box score)"),
                )
            )
            bf_pts = (
                alt.Chart(bf_scatter)
                .mark_circle(size=64, opacity=0.78)
                .encode(
                    x=alt.X("projected_bf:Q", title="Projected BF (logged)"),
                    y=alt.Y("actual_bf:Q", title="Actual BF (box score)"),
                    color=alt.Color(
                        "bf_above_proj:N",
                        title="",
                        scale=alt.Scale(
                            domain=["Actual > projected", "Actual ≤ projected"],
                            range=["#f97316", "#64748b"],
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("date:N"),
                        alt.Tooltip("pitcher:N"),
                        alt.Tooltip("projected_bf:Q", format=".1f"),
                        alt.Tooltip("actual_bf:Q", format="d"),
                    ],
                )
            )
            st.caption("Points **orange** when actual BF **>** projected (above the diagonal).")
            st.altair_chart(
                (bf_line + bf_pts).properties(height=280, title="Workload: projected vs actual BF"),
                use_container_width=True,
            )
        else:
            st.caption(
                "Log **projected BF** (manual or auto beta rows), then run **Fetch Results** "
                "to backfill **actual BF** from the box score."
            )
    else:
        st.caption("Upgrade tracker schema: reopen the app to migrate the database.")

    scatter = settled.assign(actual_k=actk)
    if "confidence_flag" in scatter.columns:
        scatter = scatter.assign(
            confidence_flag=scatter["confidence_flag"].fillna("n/a").astype(str)
        )
    else:
        scatter = scatter.assign(confidence_flag="n/a")
    diag = pd.DataFrame({"x": [0.0, 20.0], "y": [0.0, 20.0]})
    line = (
        alt.Chart(diag)
        .mark_line(color="#94a3b8", strokeDash=[4, 4])
        .encode(x=alt.X("x:Q", title="Model λ"), y=alt.Y("y:Q", title="Actual K"))
    )
    pts = (
        alt.Chart(scatter)
        .mark_circle(size=70, opacity=0.75)
        .encode(
            x=alt.X("our_projected_k:Q", title="Model λ (expected K)"),
            y=alt.Y("actual_k:Q", title="Actual K"),
            color=alt.Color(
                "confidence_flag:N",
                title="Confidence",
                scale=alt.Scale(scheme="set2"),
            ),
            tooltip=[
                alt.Tooltip("date:N"),
                alt.Tooltip("pitcher:N"),
                alt.Tooltip("opponent:N"),
                alt.Tooltip("our_projected_k:Q", format=".2f"),
                alt.Tooltip("actual_k:Q", format="d"),
                alt.Tooltip("projected_bf:Q", format=".1f"),
                alt.Tooltip("actual_bf:Q", format="d"),
                alt.Tooltip("tracker_tag:N"),
                alt.Tooltip("confidence_flag:N"),
            ],
        )
    )
    st.altair_chart((line + pts).properties(height=320), use_container_width=True)

    tag_col = "tracker_tag" if "tracker_tag" in settled.columns else None
    if tag_col:
        beta_only = settled[settled[tag_col].astype(str) == BETA_2026_TEST_TAG].copy()
        if not beta_only.empty:
            st.markdown(f"**Beta auto-logged games** (`{BETA_2026_TEST_TAG}`) — **{len(beta_only)}** row(s).")
    show_cols = [
        c
        for c in (
            "date",
            "pitcher",
            "opponent",
            "our_projected_k",
            "actual_k",
            "projected_bf",
            "actual_bf",
            "confidence_flag",
            "tracker_tag",
        )
        if c in settled.columns
    ]
    tail = settled.sort_values("id", ascending=False).head(40)[show_cols]
    st.caption("Recent settled games (confidence meter value at log time).")
    st.dataframe(tail, use_container_width=True, hide_index=True)


def _profit_from_result(
    *, result: str, american_odds: float | None, stake: float | None
) -> float | None:
    if result not in ("Win", "Loss"):
        return None
    if american_odds is None or stake is None or stake <= 0:
        return None
    if result == "Loss":
        return -float(stake)
    if american_odds > 0:
        return float(stake) * (float(american_odds) / 100.0)
    if american_odds < 0:
        return float(stake) * (100.0 / abs(float(american_odds)))
    return None


def _edge_pct_from_logged_row(row: pd.Series) -> float | None:
    side = str(row.get("bet_side") or "").strip().lower()
    if side not in ("over", "under"):
        return None
    try:
        projection = float(row["our_projected_k"])
        line = float(row["betting_line"])
        odds = float(row["american_odds"])
    except (TypeError, ValueError, KeyError):
        return None
    if odds == 0:
        return None
    try:
        implied_pct = american_odds_to_implied_probability_pct(odds)
    except ValueError:
        return None
    if side == "over":
        our_pct = poisson_over_probability_pct(projection, line)
    else:
        our_pct = poisson_under_probability_pct(projection, line)
    return float(our_pct - implied_pct)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_pitcher_throws_by_name(name: str) -> str | None:
    raw = _cached_lookup_player(name.strip())
    pitchers = filter_pitchers(raw)
    if not pitchers:
        return None
    target = name.strip().lower()
    pick = pitchers[0]
    for p in pitchers:
        if str(p.get("fullName", "")).strip().lower() == target:
            pick = p
            break
    pid = pick.get("id")
    if pid is None:
        return None
    return _cached_pitcher_throws(int(pid))


def _render_history_drilldown(hist_df: pd.DataFrame) -> None:
    req = {
        "id",
        "date",
        "pitcher",
        "our_projected_k",
        "betting_line",
        "american_odds",
        "bet_side",
        "placed_bet",
        "result",
    }
    if not req.issubset(hist_df.columns):
        st.caption("Drill-down unavailable: tracker is missing expected columns.")
        return

    settled = hist_df[
        (hist_df["placed_bet"].fillna(0).astype(int) == 1)
        & hist_df["result"].isin(["Win", "Loss"])
    ].copy()
    if settled.empty:
        st.caption("No settled placed bets yet for drill-down analysis.")
        return

    settled["edge_pct"] = settled.apply(_edge_pct_from_logged_row, axis=1)
    settled = settled[settled["edge_pct"].notna()].copy()
    if settled.empty:
        st.caption("No settled rows with valid side/odds for edge drill-down.")
        return

    settled["pitcher_type"] = (
        settled["pitcher"]
        .astype(str)
        .map(_cached_pitcher_throws_by_name)
        .map(lambda x: "RHP" if x == "R" else "LHP" if x == "L" else "Unknown")
    )
    by_hand = (
        settled.groupby("pitcher_type", as_index=False)
        .agg(
            bets=("id", "count"),
            wins=("result", lambda s: int((s == "Win").sum())),
        )
        .sort_values("pitcher_type")
    )
    by_hand["win_rate_pct"] = (100.0 * by_hand["wins"] / by_hand["bets"]).round(1)
    st.markdown("**Win Rate by Pitcher Type**")
    st.dataframe(
        by_hand.rename(
            columns={
                "pitcher_type": "Pitcher Type",
                "bets": "Bets",
                "wins": "Wins",
                "win_rate_pct": "Win Rate %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    bins = [-1e9, 2.0, 5.0, 10.0, 1e9]
    labels = ["<2%", "2-5%", "5-10%", "10%+"]
    settled["edge_tier"] = pd.cut(
        settled["edge_pct"], bins=bins, labels=labels, right=False
    )
    by_edge = (
        settled.groupby("edge_tier", as_index=False, observed=False)
        .agg(
            bets=("id", "count"),
            wins=("result", lambda s: int((s == "Win").sum())),
            avg_edge_pct=("edge_pct", "mean"),
        )
        .sort_values("edge_tier")
    )
    by_edge["win_rate_pct"] = (100.0 * by_edge["wins"] / by_edge["bets"]).round(1)
    by_edge["avg_edge_pct"] = by_edge["avg_edge_pct"].round(2)
    st.markdown("**Win Rate by Edge Tier**")
    st.dataframe(
        by_edge.rename(
            columns={
                "edge_tier": "Edge Tier",
                "bets": "Bets",
                "wins": "Wins",
                "win_rate_pct": "Win Rate %",
                "avg_edge_pct": "Avg Edge %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

def _render_manual_override(hist_df: pd.DataFrame, *, user_id: int) -> None:
    with st.expander("Manual override (fix incorrect settlements)", expanded=False):
        st.caption(
            "Use this to correct a row if the auto-settle matched the wrong game or K total."
        )
        try:
            max_id = int(hist_df["id"].max())
        except Exception:
            max_id = 0
        override_id = int(
            st.number_input(
                "Row id to edit",
                min_value=1,
                max_value=max(1, max_id),
                step=1,
                value=max(1, max_id),
                format="%d",
                key="override_row_id",
            )
        )
        row = hist_df[hist_df["id"] == override_id]
        if row.empty:
            st.warning("Row id not found in loaded history.")
            return

        r0 = row.iloc[0].to_dict()
        st.markdown(
            f"Editing **#{override_id}** — **{r0.get('pitcher','')}** "
            f"({r0.get('pitcher_team') or '—'}) vs **{r0.get('opponent','')}** "
            f"on **{r0.get('date','')}**"
        )

        st.divider()
        can_delete = bool(int(r0.get("placed_bet") or 0) == 1)
        confirm_delete = st.checkbox(
            "Confirm delete selected row (bets only)",
            value=False,
            key="confirm_delete_selected_row",
            help="Deletes from SQLite predictions by row id. Only placed_bet=1 rows are deletable.",
        )
        if st.button(
            "Delete selected bet row",
            key="btn_delete_selected_row",
            disabled=(not confirm_delete or not can_delete),
            use_container_width=True,
        ):
            ok = _delete_prediction_row(
                int(override_id), user_id=int(user_id), require_placed_bet=True
            )
            if ok:
                st.success(f"Deleted row #{override_id}.")
                st.rerun()
            else:
                st.error("Delete failed (row may not be deletable or already removed).")

        c_a, c_b, c_c, c_d = st.columns(4)
        with c_a:
            new_actual_k = st.number_input(
                "Actual K",
                min_value=0,
                max_value=30,
                step=1,
                value=int(r0["actual_k"]) if pd.notna(r0.get("actual_k")) else 0,
                key="override_actual_k",
            )
        with c_b:
            new_result = st.selectbox(
                "Result",
                options=["Win", "Loss", "No-Bet", ""],
                index=0
                if r0.get("result") == "Win"
                else 1
                if r0.get("result") == "Loss"
                else 2
                if r0.get("result") == "No-Bet"
                else 3,
                key="override_result",
            )
        with c_c:
            auto_profit = _profit_from_result(
                result=new_result,
                american_odds=float(r0["american_odds"])
                if pd.notna(r0.get("american_odds"))
                else None,
                stake=float(r0["stake"]) if pd.notna(r0.get("stake")) else None,
            )
            new_profit = st.number_input(
                "Profit (leave as auto or override)",
                step=1.0,
                value=float(auto_profit) if auto_profit is not None else 0.0,
                format="%.2f",
                key="override_profit",
            )
        with c_d:
            _co = r0.get("closing_american_odds")
            _co_default = int(round(float(_co))) if pd.notna(_co) else 0
            closing_odds_input = st.number_input(
                "Closing odds (0 = clear)",
                min_value=-5000,
                max_value=5000,
                step=1,
                value=int(_co_default),
                format="%d",
                key="override_closing_odds",
            )
        set_pending = st.checkbox(
            "Mark as pending (clear actual_k/result/profit)",
            value=False,
            key="override_set_pending",
        )
        if st.button("Apply override", type="primary", key="override_apply"):
            conn = sqlite3.connect(str(default_tracker_db_path()))
            try:
                init_db(conn)
                if set_pending:
                    conn.execute(
                        """
                        UPDATE predictions
                        SET actual_k=NULL, actual_bf=NULL, result=NULL, profit=NULL
                        WHERE id=? AND user_id=?
                        """,
                        (override_id, int(user_id)),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE predictions
                        SET actual_k=?, result=?, profit=?, closing_american_odds=?
                        WHERE id=? AND user_id=?
                        """,
                        (
                            int(new_actual_k),
                            (new_result or None),
                            float(new_profit)
                            if (new_result in ("Win", "Loss"))
                            else None,
                            (
                                float(int(closing_odds_input))
                                if int(closing_odds_input) != 0
                                else None
                            ),
                            override_id,
                            int(user_id),
                        ),
                    )
                conn.commit()
                st.success("Override saved.")
            finally:
                conn.close()
            st.rerun()


def _team_logo_url(team_id: int) -> str:
    return (
        "https://www.mlbstatic.com/team-logos/team-cap-on-dark/"
        f"{int(team_id)}.svg"
    )




def _prop_keys_for_game(gid: int) -> tuple[str, str, str]:
    return (f"focus_line_{gid}", f"focus_odds_{gid}", f"focus_side_{gid}")


def _prop_keys_for_game_side(gid: int, side: str) -> tuple[str, str, str]:
    """Focus ("editable") keys for a specific game side in the game detail screen."""
    return (
        f"focus_line_{gid}_{side}",
        f"focus_odds_{gid}_{side}",
        f"focus_side_{gid}_{side}",
    )


def _bet_side_suffix(bet_side_label: str) -> str:
    return "over" if str(bet_side_label).strip().lower() == "over" else "under"


def _locked_prop_keys_for_bet(
    gid: int, side: str, bet_side_label: str
) -> tuple[str, str]:
    """Slate-persistent ("locked") values for a specific game side + Over/Under."""
    suffix = _bet_side_suffix(bet_side_label)
    return (
        f"locked_line_{gid}_{side}_{suffix}",
        f"locked_odds_{gid}_{side}_{suffix}",
    )


def _format_locked_prop(
    gid: int, side: str
) -> tuple[str | None, str | None, str | None]:
    """Return (line_str, odds_str, bet_side_label) if exactly one bet side is locked."""
    # Not used currently by the slate UI (cards are kept simple), but keep it around for debugging.
    over_line, over_odds = _locked_prop_keys_for_bet(gid, side, "Over")
    under_line, under_odds = _locked_prop_keys_for_bet(gid, side, "Under")
    over_ok = over_line in st.session_state and over_odds in st.session_state
    under_ok = under_line in st.session_state and under_odds in st.session_state
    if over_ok and not under_ok:
        return (
            f"{float(st.session_state.get(over_line, 0.0)):.1f}",
            f"{int(st.session_state.get(over_odds, 0)):+d}",
            "Over",
        )
    if under_ok and not over_ok:
        return (
            f"{float(st.session_state.get(under_line, 0.0)):.1f}",
            f"{int(st.session_state.get(under_odds, 0)):+d}",
            "Under",
        )
    return None, None, None


def _init_prop_keys_if_needed(
    line_key: str, odds_key: str, side_key: str, *, seed_line: float | None = None
) -> None:
    if line_key not in st.session_state:
        if line_key == _SIDEBAR_KEY_LINE:
            st.session_state[line_key] = float(seed_line if seed_line is not None else 5.5)
        else:
            st.session_state[line_key] = float(
                seed_line
                if seed_line is not None
                else st.session_state.get(_SIDEBAR_KEY_LINE, 5.5)
            )
    if odds_key not in st.session_state:
        if odds_key == _SIDEBAR_KEY_AMERICAN_ODDS:
            st.session_state[odds_key] = -110
        else:
            raw = st.session_state.get(_SIDEBAR_KEY_AMERICAN_ODDS, -110)
            try:
                st.session_state[odds_key] = int(round(float(raw)))
            except (TypeError, ValueError):
                st.session_state[odds_key] = -110
    if side_key not in st.session_state:
        if side_key == _SIDEBAR_KEY_BET_SIDE:
            st.session_state[side_key] = "Over"
        else:
            st.session_state[side_key] = str(
                st.session_state.get(_SIDEBAR_KEY_BET_SIDE, "Over")
            )


def _render_prop_controls(
    *,
    line_key: str,
    odds_key: str,
    side_key: str,
    button_suffix: str,
    seed_line: float | None = None,
) -> tuple[float, float, str, PropSide]:
    _init_prop_keys_if_needed(line_key, odds_key, side_key, seed_line=seed_line)
    line_val = st.slider(
        "K prop line",
        min_value=1.5,
        max_value=15.5,
        step=0.5,
        key=line_key,
    )
    odds_i = st.number_input(
        "American odds (your side)",
        min_value=-5000,
        max_value=5000,
        step=1,
        format="%d",
        key=odds_key,
        help="Whole-number price (e.g. -110, +105).",
    )
    odds_f = float(int(odds_i))

    bet_side_label = st.radio(
        "Side",
        ["Over", "Under"],
        horizontal=True,
        key=side_key,
    )
    prop_side: PropSide = "under" if bet_side_label == "Under" else "over"
    return float(line_val), odds_f, bet_side_label, prop_side


def _ensure_main_prop_defaults() -> None:
    if _SIDEBAR_KEY_LINE not in st.session_state:
        st.session_state[_SIDEBAR_KEY_LINE] = 5.5
    if _SIDEBAR_KEY_AMERICAN_ODDS not in st.session_state:
        st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS] = -110
    if _SIDEBAR_KEY_BET_SIDE not in st.session_state:
        st.session_state[_SIDEBAR_KEY_BET_SIDE] = "Over"


def _try_auto_beta_2026_log(
    *,
    game_date: str,
    pitcher_name: str,
    pitcher_team: str | None,
    opponent: str,
    line: float,
    projection: float,
    projected_bf: float,
    american_odds: float,
    prop_side: PropSide,
    recommend: bool,
    game_id: int | None,
    edge_pct: float,
    confidence_flag: str | None,
) -> None:
    """Persist high-edge looks to tracker when auto-beta logging is enabled."""
    if not bool(st.session_state.get(_AUTO_BETA_LOGGING_KEY, False)):
        return
    if edge_pct <= 5.0:
        return
    _uid_beta = int(st.session_state[_AUTH_USER_ID])
    rid = _overwrite_open_prediction(
        user_id=_uid_beta,
        game_date=game_date,
        pitcher=pitcher_name,
        pitcher_team=pitcher_team,
        opponent=opponent,
        betting_line=line,
        our_projected_k=projection,
        recommended=bool(recommend),
        placed_bet=False,
        bet_side=prop_side,
        american_odds=float(american_odds),
        stake=None,
        game_id=game_id,
        projected_bf=float(projected_bf),
        confidence_flag=confidence_flag,
        tracker_tag=BETA_2026_TEST_TAG,
    )
    if rid is not None:
        return
    log_prediction(
        game_date,
        pitcher_name.strip(),
        (pitcher_team or "").strip() or None,
        opponent.strip(),
        float(projection),
        float(line),
        recommended=bool(recommend),
        placed_bet=False,
        bet_side=prop_side,
        american_odds=float(american_odds),
        stake=None,
        game_id=game_id,
        tracker_tag=BETA_2026_TEST_TAG,
        projected_bf=float(projected_bf),
        confidence_flag=confidence_flag,
        user_id=_uid_beta,
        db_path=default_tracker_db_path(),
    )


def render_projection_block(
    *,
    player: dict | None,
    pitcher_name: str,
    pitcher_team_api_id: int | None,
    team_label: str,
    opposing_team: str,
    pitcher_k_pct: float,
    projected_bf: float,
    pitcher_throws: str | None,
    weighted_pitcher: WeightedPitcherStats | None,
    line: float,
    american_odds: float,
    prop_side: PropSide,
    bet_side_label: str,
    relax_edge: bool,
    total_bankroll: float,
    kelly_fraction: float,
    next_mu: ScheduledMatchup | None,
    context_heading: str = "Context",
    show_league_banners: bool = True,
    show_tracker_expander: bool = True,
    tracker_key_prefix: str = "main_",
) -> None:
    _league_2025 = _cached_league_doc(SEASON_PRIOR)
    _league_2026 = _cached_league_doc(SEASON_CURRENT)
    _as_of = date.today()
    team_k_pct, team_k_split_note, team_cal_blend = opponent_k_pct_blended(
        opposing_team,
        pitcher_throws,
        as_of=_as_of,
        league_doc_2025=_league_2025,
        league_doc_2026=_league_2026,
    )
    team_k_source = f"{team_k_split_note}; {team_cal_blend}"
    pitcher_k_invalid = pitcher_k_pct <= 0.0
    team_k_missing = team_k_pct is None
    odds_invalid = american_odds == 0

    if show_league_banners:
        if _league_2025 is None and _league_2026 is None:
            st.info(
                f"No **`data/league_data_{SEASON_PRIOR}.json`** or **`league_data_{SEASON_CURRENT}.json`** — "
                f"opponent K% uses `team_k_2025` only. Refresh: "
                f"`python data/league_splits.py` (2025) and again after setting season to {SEASON_CURRENT} in that script, "
                f"or run fetch for both years from Python."
            )
        elif _league_2026 is None:
            st.caption(
                f"_Missing **`league_data_{SEASON_CURRENT}.json`** — opponent blend falls back to {SEASON_PRIOR} where needed._"
            )
    if team_k_missing:
        st.warning(
            f"No K% data for **{opposing_team}**. "
            f"Add it in `data/team_k_2025.py` or run `python data/league_splits.py`."
        )
    if pitcher_k_invalid:
        st.warning("Enter a pitcher K% greater than 0.")
    if odds_invalid:
        st.warning(
            "Set **Current odds** to a non-zero American price for your side (e.g. -110 or +120)."
        )

    st.subheader(context_heading)
    pitcher_display = pitcher_name.strip() or "(no pitcher selected)"
    _hand_txt = (
        "RHP" if pitcher_throws == "R" else "LHP" if pitcher_throws == "L" else "UNK"
    )
    blend_txt = "Manual / missing blend"
    conf_txt = "n/a"
    if weighted_pitcher is not None:
        _pw = int(round(weighted_pitcher.weight_2025 * 100))
        _p2 = int(round(weighted_pitcher.weight_2026 * 100))
        blend_txt = f"{_pw}% {SEASON_PRIOR} / {_p2}% {SEASON_CURRENT}"
        conf_txt = weighted_pitcher.confidence_label
        _cl = conf_txt
        if _cl == "High Confidence":
            st.success(f"**Confidence meter:** {_cl} — 2025 actual vs 2026 projection K% agree.")
        elif _cl == "Volatility Warning":
            st.warning(
                f"**Confidence meter:** {_cl} — 2025 actual and 2026 projection K% diverge "
                f"(threshold ±{CONFIDENCE_K_DIFF_THRESHOLD_PCT:g} percentage points)."
            )
        else:
            st.caption(
                "**Confidence meter:** n/a — add this pitcher to `data/pitcher_projections_2026.json` "
                "to compare vs 2025 actuals."
            )
    if player is not None and pitcher_team_api_id is not None:
        if next_mu is not None:
            st.markdown(
                f"Next Matchup: **{next_mu.opponent_canonical}** on **{next_mu.game_date_display}**"
            )
            if next_mu.probable_not_listed:
                st.warning("Probable starter not yet confirmed")
            elif next_mu.pitcher_is_probable:
                st.success("Your pitcher is listed as the probable starter for this game.")
            elif next_mu.probable_starter:
                st.info(
                    f"Listed probable starter: **{next_mu.probable_starter}** "
                    "(selected pitcher does not match this listing)."
                )
        else:
            tn = canonical_team_for_id(pitcher_team_api_id) or team_label
            st.warning(f"No upcoming game found for {tn}.")

    can_compute = (
        player is not None
        and not team_k_missing
        and not pitcher_k_invalid
        and not odds_invalid
    )

    if can_compute:
        assert team_k_pct is not None
        projection = compute_projection(pitcher_k_pct, team_k_pct, projected_bf)
        vv = compute_value_verdict(
            projection,
            line,
            american_odds,
            side=prop_side,
            relax_criteria=relax_edge,
        )
        edge_arrow = "UP" if prop_side == "over" else "DOWN"
        cards_html = (
            f'<div class="metric-grid">'
            f'<div class="metric-pop-card"><p class="scout-label">PROJECTED KS (LAMBDA)</p><p class="metric-digital">{projection:.2f}</p></div>'
            f'<div class="metric-pop-card"><p class="scout-label">MARKET EDGE</p><p class="metric-digital edge">{vv.edge_pct:+.1f}% {edge_arrow}</p></div>'
            f'<div class="metric-pop-card"><p class="scout-label">MODEL P ({_html.escape(bet_side_label.upper())})</p><p class="metric-digital-sm">{vv.our_prob_pct:.1f}%</p></div>'
            f'<div class="metric-pop-card"><p class="scout-label">IMPLIED ({_html.escape(bet_side_label.upper())})</p><p class="metric-digital-sm">{vv.implied_prob_pct:.1f}%</p></div>'
            f"</div>"
        )
        st.markdown(cards_html, unsafe_allow_html=True)
        model_w = max(0.0, min(100.0, float(vv.our_prob_pct)))
        implied_w = max(0.0, min(100.0, float(vv.implied_prob_pct)))
        st.markdown(
            (
                f'<p class="scout-label">PROBABILITY COMPARISON</p>'
                f'<div class="prob-row"><span>MODEL {model_w:.1f}%</span><span>MARKET {implied_w:.1f}%</span></div>'
                f'<div class="prob-track">'
                f'<div class="prob-model" style="width:{model_w:.1f}%"></div>'
                f'<div class="prob-market" style="left:{implied_w:.1f}%"></div>'
                f"</div>"
            ),
            unsafe_allow_html=True,
        )
        st.caption(
            f"Recommend when edge >= **{vv.min_edge_required_pct:.0f}%** "
            f"({'relaxed' if relax_edge else 'strict'})."
        )
        st.markdown(
            (
                f'<div class="metric-grid">'
                f'<div class="metric-pop-card"><p class="scout-label">PITCHER</p><p class="metric-text">{_html.escape(pitcher_display)} - {_hand_txt}</p></div>'
                f'<div class="metric-pop-card"><p class="scout-label">OPPONENT</p><p class="metric-text">{_html.escape(opposing_team)}</p></div>'
                f'<div class="metric-pop-card"><p class="scout-label">LINE / BF</p><p class="metric-text">{line:.1f} K LINE - {projected_bf:.1f} BF</p></div>'
                f'<div class="metric-pop-card"><p class="scout-label">BLEND / CONFIDENCE</p><p class="metric-text">{_html.escape(blend_txt)} - {_html.escape(conf_txt)}</p></div>'
                f"</div>"
            ),
            unsafe_allow_html=True,
        )
        _conf_for_log = (
            weighted_pitcher.confidence_label if weighted_pitcher is not None else None
        )
        _beta_game_date = (
            (next_mu.game_date_iso if next_mu else "").strip()
            or date.today().strftime("%Y-%m-%d")
        )
        _beta_lineup_ok = (
            next_mu is not None
            and opposing_team == next_mu.opponent_canonical
        )
        _beta_log_gid = next_mu.game_pk if _beta_lineup_ok else None
        _try_auto_beta_2026_log(
            game_date=_beta_game_date,
            pitcher_name=pitcher_name,
            pitcher_team=(
                (team_label or "").strip()
                or (
                    canonical_team_for_id(pitcher_team_api_id)
                    if pitcher_team_api_id is not None
                    else None
                )
            ),
            opponent=opposing_team,
            line=line,
            projection=projection,
            projected_bf=float(projected_bf),
            american_odds=float(american_odds),
            prop_side=prop_side,
            recommend=vv.recommend,
            game_id=_beta_log_gid,
            edge_pct=float(vv.edge_pct),
            confidence_flag=_conf_for_log,
        )
        stamp_cls = "verdict-stamp play" if vv.recommend else "verdict-stamp nobet"
        stamp_text = "PLAY" if vv.recommend else "NO BET"
        st.markdown(
            (
                f'<p class="scout-label">VERDICT</p>'
                f'<div class="{stamp_cls}">{stamp_text}</div>'
            ),
            unsafe_allow_html=True,
        )
        st.caption(vv.headline)
        st.caption(vv.detail)

        st.subheader("Staking Recommendation")
        decimal_odds = _american_to_decimal_odds(float(american_odds))
        kelly_bet = calculate_kelly_bet(
            vv.our_prob_pct / 100.0,
            decimal_odds,
            float(total_bankroll),
            float(kelly_fraction),
        )
        unit_base = float(total_bankroll) / 100.0 if total_bankroll > 0 else 1.0
        units_from_kelly = (kelly_bet / unit_base) if (unit_base > 0 and kelly_bet > 0) else 0.0

        if kelly_bet <= 0:
            st.warning("No Edge Found - Skip Bet")
        else:
            units = units_from_kelly
            net_profit = kelly_bet * (decimal_odds - 1.0)
            st.success(
                f"Bet **${kelly_bet:,.2f}**\n\n"
                f"Unit Size: **{units:.1f} Units**"
            )

            risk_reward_df = pd.DataFrame(
                {
                    "Category": ["Risk", "Reward"],
                    "Amount ($)": [kelly_bet, net_profit],
                }
            )
            st.caption("Risk vs. Reward")
            risk_reward_chart = (
                alt.Chart(risk_reward_df)
                .mark_bar(size=36)
                .encode(
                    x=alt.X("Category:N", title=None),
                    y=alt.Y("Amount ($):Q", title="Amount ($)"),
                    color=alt.Color(
                        "Category:N",
                        scale=alt.Scale(
                            domain=["Risk", "Reward"],
                            range=["#ef4444", "#10b981"],
                        ),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("Category:N"),
                        alt.Tooltip("Amount ($):Q", format=",.2f"),
                    ],
                )
                .properties(height=180)
            )
            st.altair_chart(risk_reward_chart, use_container_width=True)

        # --- Best Value from The Odds API ---------------------------------
        _proj_odds_data = st.session_state.get("odds_data", {})
        if _proj_odds_data and pitcher_name:
            _proj_entry = lookup_pitcher_in_odds(pitcher_name, _proj_odds_data)
            if _proj_entry is not None:
                _bv = find_best_kelly_odds(_proj_entry, projection, side=prop_side)
                if _bv is not None:
                    with st.expander("Best Value (Odds API)", expanded=True):
                        _bk_label = _bv.bookmaker.replace("_", " ").title()
                        _bv_side_label = "Over" if _bv.side == "over" else "Under"
                        _bv_price = _bv.over_price if _bv.side == "over" else _bv.under_price
                        st.markdown(
                            f"**{_bk_label}** — Line **{_bv.line:.1f}** "
                            f"{_bv_side_label} @ **{fmt_american(_bv_price)}** "
                            f"| Kelly Edge: **{_bv.kelly_edge_pct:+.1f}%**"
                        )
                        _ts = get_cached_fetch_time()
                        if _ts:
                            st.caption(f"Odds last fetched: {_ts}")
                        if st.button(
                            f"Use {_bk_label} line in sidebar",
                            key=f"apply_best_kelly_{pitcher_name[:20]}",
                        ):
                            st.session_state[_SIDEBAR_KEY_LINE] = float(_bv.line)
                            st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS] = int(_bv_price)
                            st.session_state[_SIDEBAR_KEY_BET_SIDE] = _bv_side_label
                            st.success(
                                f"Sidebar updated to {_bk_label} "
                                f"{_bv_side_label} {_bv.line:.1f} @ {fmt_american(_bv_price)}"
                            )

        if show_tracker_expander:
            with st.expander("Prediction tracker", expanded=False):
                lineup_ok = (
                    next_mu is not None
                    and opposing_team == next_mu.opponent_canonical
                )
                log_date = next_mu.game_date_iso if next_mu else ""
                log_gid = next_mu.game_pk if lineup_ok else None
                placed_bet = st.checkbox(
                    "I placed this bet",
                    value=bool(vv.recommend and kelly_bet > 0),
                    key=f"{tracker_key_prefix}placed_bet",
                    help="If unchecked, this row settles as No-Bet and is excluded from ROI.",
                )
                default_stake = float(kelly_bet) if kelly_bet > 0 else 0.0
                stake_key = f"{tracker_key_prefix}stake"
                stake_auto_key = f"{tracker_key_prefix}stake_auto"
                if stake_auto_key not in st.session_state:
                    st.session_state[stake_auto_key] = False
                auto_fill_stake = st.checkbox(
                    "Auto-fill stake from Kelly",
                    key=stake_auto_key,
                    help="When enabled, stake tracks the latest Kelly recommendation.",
                )
                if auto_fill_stake:
                    st.session_state[stake_key] = round(default_stake, 2)
                elif stake_key not in st.session_state:
                    st.session_state[stake_key] = round(default_stake, 2)
                stake_amount = st.number_input(
                    "Stake ($)",
                    min_value=0.0,
                    step=1.0,
                    format="%.2f",
                    disabled=not placed_bet,
                    key=stake_key,
                    help="Amount risked if you actually placed the bet.",
                )
                if next_mu is None:
                    st.caption(
                        "No next **Scheduled/Pre-Game** in the API — log uses **today’s date** and no `game_id` "
                        "(run `mlb_tracker.py update` may need `--date` to match)."
                    )
                elif not lineup_ok:
                    st.caption(
                        f"Opponent **{opposing_team}** ≠ API next opponent **{next_mu.opponent_canonical}** — "
                        "`game_id` omitted so the tracker won’t attach the wrong box score."
                    )
                if st.button(
                    "Log this line to SQLite",
                    key=f"{tracker_key_prefix}btn_log_tracker",
                ):
                    _log_uid = int(st.session_state[_AUTH_USER_ID])
                    gdate = log_date or date.today().strftime("%Y-%m-%d")
                    pitcher_team_value = (
                        (team_label or "").strip()
                        or (canonical_team_for_id(pitcher_team_api_id) if pitcher_team_api_id is not None else "")
                    ).strip() or None
                    if placed_bet and stake_amount <= 0:
                        st.warning(
                            "Set a stake greater than 0, or uncheck 'I placed this bet'."
                        )
                        st.stop()
                    if would_duplicate_open_log(
                        gdate,
                        pitcher_name,
                        opposing_team,
                        line,
                        user_id=_log_uid,
                    ):
                        rid = _overwrite_open_prediction(
                            user_id=_log_uid,
                            game_date=gdate,
                            pitcher=pitcher_name,
                            pitcher_team=pitcher_team_value,
                            opponent=opposing_team,
                            betting_line=line,
                            our_projected_k=projection,
                            recommended=vv.recommend,
                            placed_bet=placed_bet,
                            bet_side=prop_side,
                            american_odds=float(american_odds),
                            stake=float(stake_amount) if placed_bet else None,
                            game_id=log_gid,
                            projected_bf=float(projected_bf),
                            confidence_flag=(
                                weighted_pitcher.confidence_label
                                if weighted_pitcher is not None
                                else None
                            ),
                        )
                        if rid is None:
                            st.warning(
                                "Could not find the existing open log row to overwrite."
                            )
                        else:
                            st.success(f"Updated existing open row **#{rid}**.")
                    else:
                        rid = log_prediction(
                            gdate,
                            pitcher_name,
                            pitcher_team_value,
                            opposing_team,
                            projection,
                            line,
                            recommended=vv.recommend,
                            placed_bet=placed_bet,
                            bet_side=prop_side,
                            american_odds=float(american_odds),
                            stake=float(stake_amount) if placed_bet else None,
                            game_id=log_gid,
                            projected_bf=float(projected_bf),
                            confidence_flag=(
                                weighted_pitcher.confidence_label
                                if weighted_pitcher is not None
                                else None
                            ),
                            user_id=_log_uid,
                            db_path=default_tracker_db_path(),
                        )
                        st.success(
                            f"Saved row **#{rid}**. After the game: `python mlb_tracker.py update`"
                        )
    else:
        st.metric("Projection (expected K, λ)", "—")
        st.subheader("Verdict")
        st.info(
            "Open a game from the slate (or search a pitcher in the sidebar), set **K%** / **BF** / opponent, "
            "choose **Over** or **Under** and non-zero odds on the main screen, "
            "and ensure the opponent has team K% data to see value and a recommendation."
        )


def _render_slate_game_tile(game: dict, id_abb: dict[int, str]) -> None:
    gid_raw = game.get("game_id")
    try:
        gid = int(gid_raw) if gid_raw is not None else 0
    except (TypeError, ValueError):
        gid = 0
    if gid <= 0:
        st.caption("_Missing game id — skipped._")
        return
    away_id = int(game["away_id"])
    home_id = int(game["home_id"])
    away_ab = id_abb.get(away_id, "")
    home_ab = id_abb.get(home_id, "")
    away_name = str(game.get("away_name") or "")
    home_name = str(game.get("home_name") or "")
    status = str(game.get("status") or "")
    away_p = str(game.get("away_probable_pitcher") or "").strip() or "TBD"
    home_p = str(game.get("home_probable_pitcher") or "").strip() or "TBD"

    score_note = ""
    if status not in UPCOMING_GAME_STATUSES:
        score_note = f" · {game.get('away_score', '—')}-{game.get('home_score', '—')}"
    start_time = _format_game_start_time(game)
    status_line = (
        f"{status}{score_note} · First pitch {start_time}"
        if start_time
        else f"{status}{score_note}"
    )

    _final_statuses = {"Final", "Game Over", "Completed Early"}
    _is_live_game = (status not in UPCOMING_GAME_STATUSES) and (status not in _final_statuses)
    _live_badge_html = (
        '<div class="card-live-indicator"><span class="live-dot"></span>LIVE</div>'
        if _is_live_game
        else ""
    )
    card_html = (
        f'<div class="matchup-card">'
        f'<div class="beta-watermark">Beta Testing</div>'
        f"{_live_badge_html}"
        f'<p class="matchup-title">{_html.escape(away_ab or away_name)} @ {_html.escape(home_ab or home_name)}</p>'
        f'<div class="scoreboard-row">'
        f'<div class="team-logo-wrap">'
        f'<img src="{_team_logo_url(away_id)}" alt="{_html.escape(away_ab or away_name)} logo">'
        f"</div>"
        f'<div class="pitchers-center">'
        f'<p class="pitcher-label">Away SP</p>'
        f'<p class="pitcher-name">{_html.escape(away_p)}</p>'
        f'<p class="pitcher-label">Home SP</p>'
        f'<p class="pitcher-name">{_html.escape(home_p)}</p>'
        f"</div>"
        f'<div class="team-logo-wrap">'
        f'<img src="{_team_logo_url(home_id)}" alt="{_html.escape(home_ab or home_name)} logo">'
        f"</div>"
        f"</div>"
        f'<p class="game-status">{_html.escape(status_line)}</p>'
        f"</div>"
    )

    with st.container():
        st.markdown(card_html, unsafe_allow_html=True)

        if st.button(
            "Line, odds & starters",
            key=f"open_game_{gid}",
            use_container_width=True,
        ):
            st.session_state[_MAIN_SCREEN] = "game_detail"
            st.session_state[_DETAIL_GID] = gid
            # Clear stale player from a previous game so the sidebar/projection
            # don't show the wrong pitcher until the user selects one here.
            st.session_state.selected_player = None
            st.session_state.dash_active_gid = None
            st.session_state.dash_active_side = None
            st.session_state.pop(_DASH_SIDEBAR_SYNC_DONE, None)
            st.session_state.pop(_NEXT_MU_DASH_OVERRIDE, None)
            st.rerun()


def _render_matchup_dashboard(games: list[dict]) -> None:
    id_abb = get_id_to_abbrev()
    if not games:
        st.info("No regular-season games on this date.")
        return
    for i in range(0, len(games), 3):
        chunk = games[i : i + 3]
        cols = st.columns(3)
        for col, g in zip(cols, chunk):
            with col:
                _render_slate_game_tile(g, id_abb)


def _render_game_detail_screen(game: dict, id_abb: dict[int, str]) -> None:
    gid = int(game["game_id"])
    away_id = int(game["away_id"])
    home_id = int(game["home_id"])
    away_ab = id_abb.get(away_id, "")
    home_ab = id_abb.get(home_id, "")
    away_name = str(game.get("away_name") or "")
    home_name = str(game.get("home_name") or "")
    away_p = str(game.get("away_probable_pitcher") or "").strip() or "TBD"
    home_p = str(game.get("home_probable_pitcher") or "").strip() or "TBD"

    if st.button("← Back to slate"):
        st.session_state[_MAIN_SCREEN] = "slate"
        st.session_state[_DETAIL_GID] = None
        st.session_state.selected_player = None
        st.session_state.dash_active_gid = None
        st.session_state.dash_active_side = None
        st.session_state.pop(_DASH_SIDEBAR_SYNC_DONE, None)
        st.session_state.pop(_NEXT_MU_DASH_OVERRIDE, None)
        st.rerun()

    _away_hand = _pitcher_hand_label(away_p)
    _home_hand = _pitcher_hand_label(home_p)
    st.subheader(f"{away_ab or away_name} @ {home_ab or home_name}")
    h1, hmid, h2 = st.columns([1.2, 0.7, 1.2])
    with h1:
        st.markdown(
            _textwrap.dedent(
                f"""
                <div class="team-logo-wrap detail-team-logo-wrap" style="margin: 0 auto;">
                    <img src="{_team_logo_url(away_id)}" alt="{_html.escape(away_ab or away_name)} logo">
                </div>
                <p class="detail-pitcher-name">{_html.escape(away_p)} - {_away_hand}</p>
                """
            ).strip(),
            unsafe_allow_html=True,
        )
    with hmid:
        st.markdown('<p class="detail-vs">VS</p>', unsafe_allow_html=True)
    with h2:
        st.markdown(
            _textwrap.dedent(
                f"""
                <div class="team-logo-wrap detail-team-logo-wrap" style="margin: 0 auto;">
                    <img src="{_team_logo_url(home_id)}" alt="{_html.escape(home_ab or home_name)} logo">
                </div>
                <p class="detail-pitcher-name">{_html.escape(home_p)} - {_home_hand}</p>
                """
            ).strip(),
            unsafe_allow_html=True,
        )
    status_detail = str(game.get("status") or "")
    if status_detail not in UPCOMING_GAME_STATUSES:
        st.caption(
            f"{status_detail} · Score **{game.get('away_score', '—')}**-"
            f"**{game.get('home_score', '—')}**"
        )

    live_away_d: LiveStarterPitchingLine | None = None
    live_home_d: LiveStarterPitchingLine | None = None
    if status_detail not in UPCOMING_GAME_STATUSES:
        live_away_d, live_home_d = _cached_live_starter_lines(gid)
    if live_away_d is not None or live_home_d is not None:
        with st.container(border=True):
            st.markdown("**Starter box score** _(live / final from MLB feed)_")
            bx1, bx2 = st.columns(2)
            with bx1:
                st.caption(f"{away_ab or away_name}")
                st.markdown(f"_{away_p}_")
                if live_away_d is not None:
                    st.markdown(_format_live_starter_line(live_away_d))
                else:
                    st.caption("_No starter line yet._")
            with bx2:
                st.caption(f"{home_ab or home_name}")
                st.markdown(f"_{home_p}_")
                if live_home_d is not None:
                    st.markdown(_format_live_starter_line(live_home_d))
                else:
                    st.caption("_No starter line yet._")

    placed_tracker = _placed_bets_for_game(
        gid, user_id=int(st.session_state[_AUTH_USER_ID])
    )
    if placed_tracker:
        with st.container(border=True):
            st.markdown("**Your placed bets**")
            st.caption(
                "Only picks you saved with **Placed bet** checked (not locked lines alone)."
            )
            for row in placed_tracker:
                st.markdown(_format_placed_bet_line(row, away_p, home_p))
    else:
        st.caption(
            "_No placed bets for this game. Save from **Projection & tracker** with "
            "**Placed bet** on so the row is tied to this game._"
        )

    st.divider()
    st.markdown("**Pick probable starter (select which SP you want to analyze)**")
    b1, b2 = st.columns(2)
    with b1:
        away_clicked = st.button(
            "Use away SP",
            key=f"detail_analyze_away_{gid}",
            disabled=(away_p == "TBD"),
            use_container_width=True,
        )
        if st.session_state.get("dash_active_gid") == gid and st.session_state.get(
            "dash_active_side"
        ) == "away":
            st.success(f"Selected: {away_p}")
        else:
            st.caption(f"Probable: {away_p}")
    with b2:
        home_clicked = st.button(
            "Use home SP",
            key=f"detail_analyze_home_{gid}",
            disabled=(home_p == "TBD"),
            use_container_width=True,
        )
        if st.session_state.get("dash_active_gid") == gid and st.session_state.get(
            "dash_active_side"
        ) == "home":
            st.success(f"Selected: {home_p}")
        else:
            st.caption(f"Probable: {home_p}")

    if away_clicked:
        st.session_state.dash_active_gid = gid
        st.session_state.dash_active_side = "away"
    if home_clicked:
        st.session_state.dash_active_gid = gid
        st.session_state.dash_active_side = "home"

    active_gid = st.session_state.get("dash_active_gid")
    active_side = st.session_state.get("dash_active_side")
    if active_gid != gid or active_side not in ("away", "home"):
        st.caption("_Choose away or home probable above to load the pitcher._")
        return

    side = active_side
    lk, ok, sk = _prop_keys_for_game_side(gid, side)
    over_odds_key = f"{ok}_over"
    under_odds_key = f"{ok}_under"
    over_locked_line_key, over_locked_odds_key = _locked_prop_keys_for_bet(
        gid, side, "Over"
    )
    under_locked_line_key, under_locked_odds_key = _locked_prop_keys_for_bet(
        gid, side, "Under"
    )
    if lk not in st.session_state:
        # Prefer a locked line if we have one for either side label.
        if over_locked_line_key in st.session_state:
            st.session_state[lk] = float(st.session_state[over_locked_line_key])
        elif under_locked_line_key in st.session_state:
            st.session_state[lk] = float(st.session_state[under_locked_line_key])
        else:
            st.session_state[lk] = 5.5
    if sk not in st.session_state:
        st.session_state[sk] = "Over"
    # Critical: always ensure odds keys are present so number_input doesn't fall back to min_value (-5000).
    if over_odds_key not in st.session_state:
        if over_locked_odds_key in st.session_state:
            st.session_state[over_odds_key] = int(st.session_state[over_locked_odds_key])
        else:
            st.session_state[over_odds_key] = -110
    if under_odds_key not in st.session_state:
        if under_locked_odds_key in st.session_state:
            st.session_state[under_odds_key] = int(st.session_state[under_locked_odds_key])
        else:
            st.session_state[under_odds_key] = -110

    # Seed per-side odds fields from locked values once.
    seeded_key = f"seeded_odds_{gid}_{side}"
    if not st.session_state.get(seeded_key, False):
        if over_locked_odds_key in st.session_state:
            st.session_state[over_odds_key] = int(st.session_state[over_locked_odds_key])
            st.session_state[lk] = float(st.session_state.get(over_locked_line_key, 5.5))
        elif under_locked_odds_key in st.session_state:
            st.session_state[under_odds_key] = int(st.session_state[under_locked_odds_key])
            st.session_state[lk] = float(st.session_state.get(under_locked_line_key, 5.5))
        st.session_state[seeded_key] = True

    # Determine which sides are currently locked.
    _over_locked = (
        over_locked_line_key in st.session_state
        and over_locked_odds_key in st.session_state
    )
    _under_locked = (
        under_locked_line_key in st.session_state
        and under_locked_odds_key in st.session_state
    )

    # Pin widget keys to locked values on EVERY render so they can never drift.
    # (Whichever side is locked wins the line; if both locked they should share the same line.)
    if _over_locked:
        st.session_state[lk] = float(st.session_state[over_locked_line_key])
        st.session_state[over_odds_key] = int(st.session_state[over_locked_odds_key])
    if _under_locked:
        st.session_state[lk] = float(st.session_state[under_locked_line_key])
        st.session_state[under_odds_key] = int(st.session_state[under_locked_odds_key])

    # --- Odds API bookmaker table (shown before manual inputs) ----------
    _odds_data_raw = st.session_state.get("odds_data", {})
    _sp_name = (
        str(game.get("away_probable_pitcher") or "").strip()
        if side == "away"
        else str(game.get("home_probable_pitcher") or "").strip()
    )
    _odds_entry: PitcherOddsEntry | None = (
        lookup_pitcher_in_odds(_sp_name, _odds_data_raw) if _sp_name else None
    )
    if _odds_entry is not None and _odds_entry.books:
        with st.expander("Bookmaker lines (pitcher_strikeouts)", expanded=True):
            import pandas as _pd
            _rows = [
                {
                    "Book": b.bookmaker.replace("_", " ").title(),
                    "Line": b.line,
                    "Over": fmt_american(b.over_price),
                    "Under": fmt_american(b.under_price),
                }
                for b in _odds_entry.books
            ]
            st.dataframe(_pd.DataFrame(_rows), use_container_width=True, hide_index=True)

            # Apply-best-odds button: picks the book with highest Over price
            _best_book = best_over_price_book(_odds_entry)
            _modal = modal_line(_odds_entry)
            if _best_book is not None and _modal is not None:
                _apply_key = f"apply_best_odds_{gid}_{side}"
                if st.button(
                    f"Apply best value: O {_modal:.1f} @ {fmt_american(_best_book.over_price)}"
                    f" ({_best_book.bookmaker.replace('_', ' ').title()})",
                    key=_apply_key,
                ):
                    # Snap the slider and odds inputs to the best-value combo
                    st.session_state[lk] = float(_modal)
                    st.session_state[over_odds_key] = int(_best_book.over_price)
                    if _best_book.under_price != 0:
                        st.session_state[under_odds_key] = int(_best_book.under_price)
                    st.session_state[sk] = "Over"
                    st.success(
                        f"Applied {_best_book.bookmaker.replace('_', ' ').title()} "
                        f"O {_modal:.1f} @ {fmt_american(_best_book.over_price)}"
                    )

    st.divider()
    st.markdown(f"**Line + odds ({side} side)**")
    line_v = st.slider(
        "K prop line",
        min_value=1.5,
        max_value=15.5,
        step=0.5,
        key=lk,
    )
    o1, o2 = st.columns(2)
    with o1:
        over_odds_v = int(
            st.number_input(
                "Over odds",
                min_value=-5000,
                max_value=5000,
                step=1,
                format="%d",
                key=over_odds_key,
            )
        )
    with o2:
        under_odds_v = int(
            st.number_input(
                "Under odds",
                min_value=-5000,
                max_value=5000,
                step=1,
                format="%d",
                key=under_odds_key,
            )
        )

    bet_side_label = st.radio(
        "Side to analyze",
        ["Over", "Under"],
        horizontal=True,
        key=sk,
    )
    selected_odds_v = over_odds_v if bet_side_label == "Over" else under_odds_v

    st.markdown("**Lock these line + odds for the slate tile**")
    l1, l2 = st.columns(2)
    with l1:
        if _over_locked:
            st.success(
                f"Over locked: {st.session_state[over_locked_line_key]:.1f} "
                f"@ {int(st.session_state[over_locked_odds_key]):+d}"
            )
            if st.button("Unlock Over", key=f"unlock_over_{gid}_{side}"):
                st.session_state.pop(over_locked_line_key, None)
                st.session_state.pop(over_locked_odds_key, None)
                st.session_state.pop(seeded_key, None)
                _save_persisted_locks_for_session()
                st.rerun()
        else:
            if st.button(f"Lock {side.capitalize()} Over", key=f"lock_over_{gid}_{side}"):
                st.session_state[over_locked_line_key] = float(line_v)
                st.session_state[over_locked_odds_key] = int(over_odds_v)
                if bet_side_label == "Over":
                    st.session_state[_SIDEBAR_KEY_LINE] = float(line_v)
                    st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS] = int(over_odds_v)
                    st.session_state[_SIDEBAR_KEY_BET_SIDE] = "Over"
                _save_persisted_locks_for_session()
                st.rerun()
    with l2:
        if _under_locked:
            st.success(
                f"Under locked: {st.session_state[under_locked_line_key]:.1f} "
                f"@ {int(st.session_state[under_locked_odds_key]):+d}"
            )
            if st.button("Unlock Under", key=f"unlock_under_{gid}_{side}"):
                st.session_state.pop(under_locked_line_key, None)
                st.session_state.pop(under_locked_odds_key, None)
                st.session_state.pop(seeded_key, None)
                _save_persisted_locks_for_session()
                st.rerun()
        else:
            if st.button(f"Lock {side.capitalize()} Under", key=f"lock_under_{gid}_{side}"):
                st.session_state[under_locked_line_key] = float(line_v)
                st.session_state[under_locked_odds_key] = int(under_odds_v)
                if bet_side_label == "Under":
                    st.session_state[_SIDEBAR_KEY_LINE] = float(line_v)
                    st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS] = int(under_odds_v)
                    st.session_state[_SIDEBAR_KEY_BET_SIDE] = "Under"
                _save_persisted_locks_for_session()
                st.rerun()

    raw_name = (
        str(game.get("away_probable_pitcher") or "").strip()
        if side == "away"
        else str(game.get("home_probable_pitcher") or "").strip()
    )
    if not raw_name:
        st.warning("Probable starter not listed (TBD).")
        return

    pitchers = filter_pitchers(_cached_lookup_player(raw_name))
    if not pitchers:
        st.warning(f"No pitcher match in the API for “{raw_name}”.")
        return

    if len(pitchers) > 1:
        labels = [format_player_option(p) for p in pitchers]
        idx_opts = list(range(len(pitchers)))
        pick = st.selectbox(
            "Match this name to one player",
            idx_opts,
            format_func=lambda i: labels[int(i)],
            key=f"detail_disambig_{gid}_{side}",
        )
        idx_safe = 0 if pick is None else int(pick)
        idx_safe = max(0, min(idx_safe, len(pitchers) - 1))
        resolved = pitchers[idx_safe]
    else:
        resolved = pitchers[0]

    opp_team = _opponent_canonical_for_starter_side(game, side)
    if opp_team is None or opp_team not in MLB_TEAMS:
        st.warning("Could not map opponent to a team with K% data.")
        return

    pfull = str(resolved.get("fullName") or "").strip()
    team_id_for_game = away_id if side == "away" else home_id
    nm_dash = scheduled_matchup_from_game_for_team(game, team_id_for_game, pfull)
    # Use current selected side's odds for projection sync.
    # Prefer locked values so a lock is never overwritten by a stale widget read.
    line_v = float(st.session_state.get(lk, line_v))
    bet_side = str(st.session_state.get(sk, bet_side_label))
    odds_v = float(over_odds_v if bet_side == "Over" else under_odds_v)

    if bet_side == "Over" and _over_locked:
        line_v = float(st.session_state[over_locked_line_key])
        odds_v = float(st.session_state[over_locked_odds_key])
    elif bet_side == "Under" and _under_locked:
        line_v = float(st.session_state[under_locked_line_key])
        odds_v = float(st.session_state[under_locked_odds_key])

    # Include bet_side so that switching Over <-> Under updates the analysis immediately.
    _sync_key = (gid, side, int(resolved["id"]), bet_side)
    if st.session_state.get(_DASH_SIDEBAR_SYNC_DONE) != _sync_key:
        st.session_state[_PENDING_SIDEBAR_FROM_DASH] = {
            "player": resolved,
            "pid": int(resolved["id"]),
            "opp": opp_team,
            "line": line_v,
            "odds": odds_v,
            "bet_side": bet_side,
            "next_mu": nm_dash,
        }
        st.session_state[_DASH_SIDEBAR_SYNC_DONE] = _sync_key
        st.rerun()

    st.success(
        f"**{pfull}** vs **{opp_team}** — set **K%** and **BF** in the sidebar, "
        "then see **Projection & tracker** below."
    )


# ---------------------------------------------------------------------------
# Sidebar helper functions
# When called inside ``with st.sidebar:``, all st.* widgets render there.
# ---------------------------------------------------------------------------

def _sidebar_pitcher_inputs(
    player: dict | None,
) -> tuple[str, int | None, str, "WeightedPitcherStats | None", "str | None", float, float, str]:
    """
    Render pitcher stats (K%, BF, opposing team) inside whichever sidebar context
    is active.  Returns the computed values so callers can pass them to the
    projection block.
    """
    pitcher_name = ""
    pitcher_team_api_id: int | None = None
    team_label = ""
    weighted_pitcher: WeightedPitcherStats | None = None
    pitcher_throws: str | None = None

    if player:
        pitcher_name = str(player.get("fullName", "")).strip()
        pitcher_team_api_id = pitcher_team_id(player)
        if pitcher_team_api_id is not None:
            team_label = team_display_for_id(pitcher_team_api_id)
            st.caption(f"**{pitcher_name}** — {team_label}")
        else:
            st.caption(f"**{pitcher_name}**")
            st.warning("No **currentTeam** in the API (e.g. free agent).")

    pid = player.get("id") if player else None
    if pid is not None:
        with st.spinner(f"Loading {SEASON_PRIOR}/{SEASON_CURRENT} stats…"):
            weighted_pitcher = _cached_weighted_pitcher(int(pid))
        pitcher_throws = _cached_pitcher_throws(int(pid))
        if pitcher_throws == "R":
            st.caption("Throws: **RHP**")
        elif pitcher_throws == "L":
            st.caption("Throws: **LHP**")
        else:
            st.caption("Throws: _unknown (using overall K%)_")

    _k_key = f"pitcher_k_pct_{player.get('id') if player else 'none'}"
    if _k_key not in st.session_state:
        st.session_state[_k_key] = (
            float(weighted_pitcher.k_pct) if weighted_pitcher is not None else 25.0
        )

    _bf_key = f"proj_bf_{player.get('id') if player else 'none'}"
    if _bf_key not in st.session_state:
        st.session_state[_bf_key] = (
            float(weighted_pitcher.bf_per_start)
            if weighted_pitcher is not None and weighted_pitcher.bf_per_start is not None
            else float(BF_PROJECTED)
        )

    if pid is not None and weighted_pitcher is None:
        st.warning(
            f"No {SEASON_PRIOR}/{SEASON_CURRENT} stats in the MLB API. "
            "Enter K% and BF manually."
        )
    elif weighted_pitcher is not None and weighted_pitcher.bf_per_start is None:
        st.info("No BF/start in blended stats — set projected BF manually.")
    elif weighted_pitcher is not None:
        s25 = weighted_pitcher.stats_2025
        s26 = weighted_pitcher.stats_2026
        bits = []
        if s25 is not None:
            bits.append(
                f"{SEASON_PRIOR}: {s25.strikeouts}K / {s25.total_batters_faced}BF, {s25.games_started}GS"
            )
        if s26 is not None:
            bits.append(
                f"{SEASON_CURRENT}: {s26.strikeouts}K / {s26.total_batters_faced}BF, {s26.games_started}GS"
            )
        if bits:
            st.caption("_API: " + " · ".join(bits) + " — fields below are editable._")

    k_pct = st.number_input(
        "Pitcher K%",
        min_value=0.0,
        max_value=100.0,
        step=0.1,
        format="%.1f",
        key=_k_key,
    )
    bf = st.number_input(
        "Projected BF",
        min_value=1.0,
        max_value=40.0,
        step=0.5,
        format="%.1f",
        key=_bf_key,
        help=f"Prefilled from blended {SEASON_PRIOR}/{SEASON_CURRENT} BF÷GS when available.",
    )

    opp: str = MLB_TEAMS[0]
    if player and pitcher_team_api_id is not None:
        matchup = _cached_scheduled_matchup(pitcher_team_api_id, pitcher_name)
        api_opp = matchup.opponent_canonical if matchup is not None else None
        wkey = f"opp_player_{player.get('id')}"
        label = "Opposing team" if (api_opp and api_opp in MLB_TEAMS) else "Opposing team (manual)"
        if wkey not in st.session_state:
            st.session_state[wkey] = api_opp if (api_opp and api_opp in MLB_TEAMS) else MLB_TEAMS[0]
        opp = str(
            st.selectbox(
                label,
                MLB_TEAMS,
                key=wkey,
                help="Auto-filled from today's schedule where available.",
            )
        )
    else:
        opp = str(
            st.selectbox(
                "Opposing team",
                MLB_TEAMS,
                index=0,
                key="opp_no_pitcher",
                help="Select a pitcher to auto-fill from the schedule.",
            )
        )

    return (
        pitcher_name,
        pitcher_team_api_id,
        team_label,
        weighted_pitcher,
        pitcher_throws,
        float(k_pct),
        float(bf),
        opp,
    )


def _sidebar_model_bankroll() -> tuple[bool, float, float]:
    """Render Model Settings + Bankroll Manager; return (relax_edge, bankroll, kelly)."""
    st.divider()
    st.subheader("Model settings")
    relax = st.checkbox(
        "Relax criteria",
        value=False,
        key="model_relax_edge",
        help=(
            f"Strict: edge ≥ **{EDGE_MIN_STRICT_PCT:.0f}%**. "
            f"Relaxed: edge ≥ **{EDGE_MIN_RELAXED_PCT:.0f}%**."
        ),
    )
    st.checkbox(
        "Enable auto beta logging",
        value=False,
        key=_AUTO_BETA_LOGGING_KEY,
        help=(
            "When ON, high-edge beta rows are auto-logged to tracker with tag "
            f"`{BETA_2026_TEST_TAG}`. Matching open rows are overwritten (no duplicates)."
        ),
    )
    st.divider()
    st.subheader("Bankroll Manager")
    bankroll = st.number_input(
        "Total Bankroll",
        min_value=0.0,
        value=float(_BETA_DEFAULT_BANKROLL),
        step=50.0,
        format="%.2f",
        key="bankroll_input",
    )
    kelly = st.slider(
        "Kelly Fraction",
        min_value=0.05,
        max_value=1.0,
        value=0.25,
        step=0.01,
        key="kelly_slider",
    )
    return relax, float(bankroll), float(kelly)


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

_ensure_authenticated()
_current_uid = int(st.session_state[_AUTH_USER_ID])
if _AUTH_DISPLAY_NAME not in st.session_state:
    _sync_auth_display_name_from_db(_current_uid)

if "player_search_results" not in st.session_state:
    st.session_state.player_search_results = []
if "selected_player" not in st.session_state:
    st.session_state.selected_player = None
if "manual_player" not in st.session_state:
    st.session_state.manual_player = None
if "last_search_query" not in st.session_state:
    st.session_state.last_search_query = ""
if "dash_active_gid" not in st.session_state:
    st.session_state.dash_active_gid = None
if "dash_active_side" not in st.session_state:
    st.session_state.dash_active_side = None
if _MAIN_SCREEN not in st.session_state:
    st.session_state[_MAIN_SCREEN] = "slate"
if _SLATE_DATE_ISO not in st.session_state:
    st.session_state[_SLATE_DATE_ISO] = date.today().strftime("%Y-%m-%d")
if _TOP_NAV_VIEW not in st.session_state:
    st.session_state[_TOP_NAV_VIEW] = "LIVE MATCHUPS"

_migrate_legacy_locks_if_needed(_current_uid)
# Restore any locks saved from a previous session.
if "_locks_loaded" not in st.session_state:
    _load_persisted_locks(_current_uid)
    st.session_state["_locks_loaded"] = True

if _refresh_league_json_if_stale(SEASON_CURRENT):
    _cached_league_doc.clear()

_dash_pending = st.session_state.pop(_PENDING_SIDEBAR_FROM_DASH, None)
if _dash_pending is not None:
    st.session_state.selected_player = _dash_pending["player"]
    st.session_state[f"opp_player_{_dash_pending['pid']}"] = _dash_pending["opp"]
    st.session_state[_SIDEBAR_KEY_LINE] = float(_dash_pending["line"])
    st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS] = float(_dash_pending["odds"])
    st.session_state[_SIDEBAR_KEY_BET_SIDE] = str(_dash_pending["bet_side"])
    nm = _dash_pending.get("next_mu")
    if nm is not None:
        st.session_state[_NEXT_MU_DASH_OVERRIDE] = nm
    else:
        st.session_state.pop(_NEXT_MU_DASH_OVERRIDE, None)

_inject_custom_theme_css()
_render_grass_header()
_render_green_banner()
st.title("MLB scouting dashboard")

# Determine screen BEFORE rendering sidebar so it can show the right content.
_scr = st.session_state.get(_MAIN_SCREEN, "slate")

# Apply bankroll reset before instantiating the sidebar ``bankroll_input`` widget.
if st.session_state.pop(_PENDING_BANKROLL_RESET, False):
    st.session_state["bankroll_input"] = float(_BETA_DEFAULT_BANKROLL)
    if hasattr(st, "toast"):
        st.toast(
            f"Bankroll reset to ${_BETA_DEFAULT_BANKROLL:.0f} (sidebar).",
            icon="💵",
        )

# ---------------------------------------------------------------------------
# Sidebar — content varies by active screen
# ---------------------------------------------------------------------------

# Declare variables with safe defaults; overwritten by the active sidebar branch.
player: dict | None = None
pitcher_name: str = ""
pitcher_team_api_id: int | None = None
team_label: str = ""
weighted_pitcher: WeightedPitcherStats | None = None
pitcher_throws: str | None = None
pitcher_k_pct: float = 25.0
projected_bf: float = float(BF_PROJECTED)
opposing_team: str = MLB_TEAMS[0]
relax_edge: bool = False
total_bankroll: float = 1000.0
kelly_fraction: float = 0.25

with st.sidebar:
    st.markdown('<div class="control-panel-title">Control Panel</div>', unsafe_allow_html=True)

    # -----------------------------------------------------------------------
    # MANUAL SEARCH screen — full pitcher search + model inputs
    # -----------------------------------------------------------------------
    if _scr == "manual_search":
        if st.button("← Back to Slate", key="manual_back_slate"):
            st.session_state[_MAIN_SCREEN] = "slate"
            st.rerun()

        st.header("Pitcher Search")
        search_q = st.text_input(
            "Search Pitcher Name",
            placeholder="e.g. Gerrit Cole",
            key="pitcher_search_text",
        )
        search_clicked = st.button("Search", type="primary")

        if search_clicked and search_q.strip():
            raw = _cached_lookup_player(search_q.strip())
            pitchers = filter_pitchers(raw)
            st.session_state.player_search_results = pitchers
            st.session_state.last_search_query = search_q.strip()
            st.session_state.manual_player = None
            st.session_state.pop(_DASH_SIDEBAR_SYNC_DONE, None)
        elif search_clicked:
            st.session_state.player_search_results = []
            st.session_state.manual_player = None

        results: list[dict] = st.session_state.player_search_results
        _sel: dict | None = None
        if search_clicked and not results:
            st.caption("No pitchers matched that search.")
        elif len(results) == 1:
            _sel = results[0]
        elif len(results) > 1:
            _labels = [format_player_option(p) for p in results]
            _idx_opts = list(range(len(results)))
            _idx = st.selectbox(
                "Select the correct player",
                _idx_opts,
                index=0,
                format_func=lambda i: _labels[int(i)],
            )
            _idx_safe = max(0, min(int(_idx) if _idx is not None else 0, len(results) - 1))
            _sel = results[_idx_safe]

        if _sel is not None:
            st.session_state.manual_player = _sel

        player = st.session_state.get("manual_player")

        st.divider()
        (
            pitcher_name, pitcher_team_api_id, team_label, weighted_pitcher,
            pitcher_throws, pitcher_k_pct, projected_bf, opposing_team,
        ) = _sidebar_pitcher_inputs(player)
        relax_edge, total_bankroll, kelly_fraction = _sidebar_model_bankroll()

    # -----------------------------------------------------------------------
    # GAME DETAIL screen — model inputs only (pitcher comes from game tile)
    # -----------------------------------------------------------------------
    elif _scr == "game_detail":
        player = st.session_state.get("selected_player")
        (
            pitcher_name, pitcher_team_api_id, team_label, weighted_pitcher,
            pitcher_throws, pitcher_k_pct, projected_bf, opposing_team,
        ) = _sidebar_pitcher_inputs(player)
        relax_edge, total_bankroll, kelly_fraction = _sidebar_model_bankroll()

    # -----------------------------------------------------------------------
    # SLATE screen — minimal: just a nav button + bankroll
    # -----------------------------------------------------------------------
    else:
        if st.button(
            "Manual Pitcher Search →",
            key="goto_manual_search",
            use_container_width=True,
        ):
            st.session_state[_MAIN_SCREEN] = "manual_search"
            st.session_state.player_search_results = []
            st.rerun()
        _ignore, total_bankroll, kelly_fraction = _sidebar_model_bankroll()

if _scr == "game_detail":
    next_mu: ScheduledMatchup | None = st.session_state.get(_NEXT_MU_DASH_OVERRIDE)
    if next_mu is None and player is not None and pitcher_team_api_id is not None:
        next_mu = _cached_scheduled_matchup(pitcher_team_api_id, pitcher_name)
elif _scr == "manual_search":
    next_mu = None
    if player is not None and pitcher_team_api_id is not None:
        next_mu = _cached_scheduled_matchup(pitcher_team_api_id, pitcher_name)
else:
    next_mu = None

_ensure_main_prop_defaults()
_id_abb = get_id_to_abbrev()

if "odds_refresh_counter" not in st.session_state:
    st.session_state["odds_refresh_counter"] = 0

# Load odds into session state so tile/detail/projection renderers can access them.
if "odds_data" not in st.session_state:
    st.session_state["odds_data"] = _cached_odds_data(
        st.session_state["odds_refresh_counter"]
    )

_active_top_nav = _render_top_nav()
_maybe_auto_settle_tracker()

if _active_top_nav == "LIVE MATCHUPS":
    _, _, _calendar_blend_lbl = get_current_weights()
    _model_badge = (
        weighted_pitcher.model_status_label
        if weighted_pitcher is not None
        else _calendar_blend_lbl
    )
    st.caption(f"Model Status: **{_model_badge}**")

    if _scr == "manual_search":
        st.subheader("Manual Pitcher Analysis")

        if player is None:
            st.info("Search for a pitcher using the sidebar to get started.")
        else:
            st.divider()
            st.subheader("Line & Odds")
            _render_prop_controls(
                line_key=_SIDEBAR_KEY_LINE,
                odds_key=_SIDEBAR_KEY_AMERICAN_ODDS,
                side_key=_SIDEBAR_KEY_BET_SIDE,
                button_suffix="manual",
            )

            _bet_side_label = str(st.session_state.get(_SIDEBAR_KEY_BET_SIDE, "Over"))
            _prop_side: PropSide = "under" if _bet_side_label == "Under" else "over"
            _line = float(st.session_state[_SIDEBAR_KEY_LINE])
            _american_odds = float(int(st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS]))

            st.divider()
            st.subheader("Projection & tracker")
            render_projection_block(
                player=player,
                pitcher_name=pitcher_name,
                pitcher_team_api_id=pitcher_team_api_id,
                team_label=team_label,
                opposing_team=opposing_team,
                pitcher_k_pct=float(pitcher_k_pct),
                projected_bf=float(projected_bf),
                pitcher_throws=pitcher_throws,
                weighted_pitcher=weighted_pitcher,
                line=_line,
                american_odds=_american_odds,
                prop_side=_prop_side,
                bet_side_label=_bet_side_label,
                relax_edge=relax_edge,
                total_bankroll=float(total_bankroll),
                kelly_fraction=float(kelly_fraction),
                next_mu=next_mu,
                context_heading="Context",
                show_league_banners=True,
                show_tracker_expander=True,
                tracker_key_prefix="manual_",
            )

    elif _scr == "slate":
        st.subheader("Matchup dashboard")

        try:
            _d0 = datetime.strptime(
                st.session_state[_SLATE_DATE_ISO], "%Y-%m-%d"
            ).date()
        except ValueError:
            _d0 = date.today()
        _row_c1, _row_c2 = st.columns([3, 1])
        with _row_c1:
            _slate_date = st.date_input("Slate date", value=_d0, key="slate_date")
        with _row_c2:
            st.markdown("<div style='height: 1.8rem;'></div>", unsafe_allow_html=True)
            if st.button("Refresh Odds", key="refresh_odds_btn", use_container_width=True):
                _cached_odds_data.clear()
                st.session_state["odds_refresh_counter"] += 1
                # Force a fresh fetch by calling with force_refresh
                with st.spinner("Fetching today's odds from The Odds API…"):
                    try:
                        fresh = fetch_mlb_odds_data(force_refresh=True)
                        st.session_state["odds_data"] = fresh
                        st.success(f"Fetched odds for {len(fresh)} pitchers.")
                    except Exception as _e:
                        st.error(f"Odds fetch failed: {_e}")
        st.session_state[_SLATE_DATE_ISO] = _slate_date.strftime("%Y-%m-%d")

        _ts_display = get_cached_fetch_time()
        if _ts_display:
            st.caption(f"Odds last fetched: {_ts_display}")
        else:
            st.caption("Odds not yet fetched — click **Refresh Odds** to load.")
        st.caption(
            f"`data/league_data_{SEASON_CURRENT}.json` is refreshed from the API when older than 24h "
            f"so opponent K% weighting can move with early {SEASON_CURRENT} season samples."
        )
        _slate_games = _cached_daily_schedule(st.session_state[_SLATE_DATE_ISO])
        _render_matchup_dashboard(_slate_games)

    elif _scr == "game_detail":
        _slate_iso = st.session_state.get(
            _SLATE_DATE_ISO, date.today().strftime("%Y-%m-%d")
        )
        _slate_games = _cached_daily_schedule(_slate_iso)
        _dgid = st.session_state.get(_DETAIL_GID)
        _game_row: dict | None = None
        if _dgid is not None:
            for _g in _slate_games:
                try:
                    if int(_g.get("game_id") or 0) == int(_dgid):
                        _game_row = _g
                        break
                except (TypeError, ValueError):
                    continue
        if _game_row is None:
            st.error("That game is not on the loaded slate.")
            if st.button("Return to slate", key="return_slate_bad_gid"):
                st.session_state[_MAIN_SCREEN] = "slate"
                st.session_state[_DETAIL_GID] = None
                st.rerun()
        else:
            # --- Game navigation row ----------------------------------------
            _valid_games = [
                _g for _g in _slate_games
                if _g.get("game_id") is not None
            ]
            _cur_idx = next(
                (i for i, _g in enumerate(_valid_games)
                 if str(_g.get("game_id")) == str(_dgid)),
                None,
            )

            def _jump_to_game(target_gid: int) -> None:
                st.session_state[_DETAIL_GID] = target_gid
                st.session_state.selected_player = None
                st.session_state.dash_active_gid = None
                st.session_state.dash_active_side = None
                st.session_state.pop(_DASH_SIDEBAR_SYNC_DONE, None)
                st.session_state.pop(_NEXT_MU_DASH_OVERRIDE, None)

            _nav_c1, _nav_c2, _nav_c3 = st.columns([1, 2, 1])
            with _nav_c1:
                _has_prev = _cur_idx is not None and _cur_idx > 0
                if st.button(
                    "← Prev game",
                    key="nav_prev_game",
                    disabled=not _has_prev,
                    use_container_width=True,
                ):
                    _jump_to_game(int(_valid_games[_cur_idx - 1]["game_id"]))
                    st.rerun()
            with _nav_c2:
                if _cur_idx is not None:
                    st.caption(
                        f"Game {_cur_idx + 1} of {len(_valid_games)}",
                    )
            with _nav_c3:
                _has_next = _cur_idx is not None and _cur_idx < len(_valid_games) - 1
                if st.button(
                    "Next game →",
                    key="nav_next_game",
                    disabled=not _has_next,
                    use_container_width=True,
                ):
                    _jump_to_game(int(_valid_games[_cur_idx + 1]["game_id"]))
                    st.rerun()

            _render_game_detail_screen(_game_row, _id_abb)

            # Only show projection when a pitcher for THIS game has been selected.
            _pitcher_ready = (
                player is not None
                and st.session_state.get("dash_active_gid") == _dgid
                and st.session_state.get("dash_active_side") in ("away", "home")
            )
            if _pitcher_ready:
                st.divider()
                st.subheader("Projection & tracker")

                _bet_side_label = str(st.session_state.get(_SIDEBAR_KEY_BET_SIDE, "Over"))
                _prop_side: PropSide = "under" if _bet_side_label == "Under" else "over"
                _line = float(st.session_state[_SIDEBAR_KEY_LINE])
                _american_odds = float(int(st.session_state[_SIDEBAR_KEY_AMERICAN_ODDS]))

            if _pitcher_ready:
                render_projection_block(
                    player=player,
                    pitcher_name=pitcher_name,
                    pitcher_team_api_id=pitcher_team_api_id,
                    team_label=team_label,
                    opposing_team=opposing_team,
                    pitcher_k_pct=float(pitcher_k_pct),
                    projected_bf=float(projected_bf),
                    pitcher_throws=pitcher_throws,
                    weighted_pitcher=weighted_pitcher,
                    line=_line,
                    american_odds=_american_odds,
                    prop_side=_prop_side,
                    bet_side_label=_bet_side_label,
                    relax_edge=relax_edge,
                    total_bankroll=float(total_bankroll),
                    kelly_fraction=float(kelly_fraction),
                    next_mu=next_mu,
                    context_heading="Context",
                    show_league_banners=True,
                    show_tracker_expander=True,
                    tracker_key_prefix="main_",
                )

elif _active_top_nav == "LIVE GAME CENTER":
    _render_live_game_center()

elif _active_top_nav == "BET LOG":
    if _FETCH_RESULTS_COUNT_KEY in st.session_state:
        _n_done = int(st.session_state.pop(_FETCH_RESULTS_COUNT_KEY))
        st.success(f"Fetch complete. Updated {_n_done} bet(s).")

    if st.button("Fetch Results", key="fetch_results_btn", use_container_width=True):
        with st.spinner(
            "Checking MLB API — settling open rows whose games are Final / Game Over…"
        ):
            _n_updated = update_results(
                db_path=default_tracker_db_path(),
                settle_all_pending=True,
            )
        st.session_state[_FETCH_RESULTS_COUNT_KEY] = int(_n_updated)
        st.rerun()

    hist_df = _load_predictions_df(user_id=_current_uid)
    if hist_df.empty:
        st.info(
            "No bet history yet — predictions are stored in **`mlb_tracker.db`** "
            "(or legacy `mlb_predictions.sqlite`) next to this app."
        )
    else:
        _render_manual_override(hist_df, user_id=_current_uid)
        st.divider()
        pending_df = _pending_bets_df(hist_df)
        st.divider()
        st.subheader("Bet log by game date")
        dates_sorted = _history_game_dates_descending(hist_df)
        if not dates_sorted:
            st.caption("No dated rows to group.")
            _render_history_by_day(hist_df, pending_df, date_iso=None)
        elif len(dates_sorted) <= _MAX_HISTORY_DAY_TABS:
            _tab_labels = ["All dates"] + [_history_tab_label(d) for d in dates_sorted]
            _day_tabs = st.tabs(_tab_labels)
            with _day_tabs[0]:
                _render_history_by_day(hist_df, pending_df, date_iso=None)
            for _ti, _d in enumerate(dates_sorted, start=1):
                with _day_tabs[_ti]:
                    _render_history_by_day(hist_df, pending_df, date_iso=_d)
        else:
            st.caption(
                f"_{len(dates_sorted)} distinct game dates — use the picker below (tab limit is {_MAX_HISTORY_DAY_TABS})._"
            )
            _pick = st.selectbox(
                "Jump to game date",
                options=["All dates"] + dates_sorted,
                format_func=lambda x: "All dates" if x == "All dates" else _history_tab_label(str(x)),
                key="history_day_pick_many",
            )
            if _pick == "All dates":
                _render_history_by_day(hist_df, pending_df, date_iso=None)
            else:
                _render_history_by_day(hist_df, pending_df, date_iso=str(_pick))


elif _active_top_nav == "ANALYTICS":
    hist_df = _load_predictions_df(user_id=_current_uid)
    if hist_df.empty:
        st.info("No settled history yet for statistics.")
    else:
        total_profit, roi_pct, win_rate = _history_metrics(user_id=_current_uid)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(
                _textwrap.dedent(
                    f"""
                    <div class="analytics-card">
                        <p class="analytics-label">Total Profit/Loss</p>
                        <p class="analytics-value profit">${total_profit:+.2f}</p>
                    </div>
                    """
                ).strip(),
                unsafe_allow_html=True,
            )
        with c2:
            _roi_text = "n/a" if roi_pct is None else f"{roi_pct:+.2f}%"
            st.markdown(
                _textwrap.dedent(
                    f"""
                    <div class="analytics-card">
                        <p class="analytics-label">ROI</p>
                        <p class="analytics-value">{_roi_text}</p>
                    </div>
                    """
                ).strip(),
                unsafe_allow_html=True,
            )
        with c3:
            _wr_text = "n/a" if win_rate is None else f"{win_rate:.1f}%"
            st.markdown(
                _textwrap.dedent(
                    f"""
                    <div class="analytics-card">
                        <p class="analytics-label">Win Rate</p>
                        <p class="analytics-value winrate">{_wr_text}</p>
                    </div>
                    """
                ).strip(),
                unsafe_allow_html=True,
            )

        with st.expander("Performance Dashboard", expanded=True):
            _render_performance_dashboard(hist_df)
        with st.expander("Drill-Down", expanded=True):
            _render_history_drilldown(hist_df)
