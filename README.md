# MLB strikeout props (Streamlit)

Streamlit dashboard for MLB pitcher strikeout props: projections, odds (DraftKings / FanDuel via [The Odds API](https://the-odds-api.com/)), and a SQLite pick tracker.

## Setup

Requires **Python 3.11+**.

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Set your Odds API key (required for live odds):

```bash
export ODDS_API_KEY="your_key"
```

Or copy `.env.example` to `.env` and add the key (Streamlit does not auto-load `.env`; use `export` or a tool like `direnv`).

## Run

```bash
streamlit run app/streamlit_app.py
```

## Project layout

| Path | Purpose |
|------|--------|
| `app/streamlit_app.py` | Streamlit entrypoint |
| `app/tracker.py` | SQLite tracker schema and CLI (`python -m app.tracker …`) |
| `app/accounts.py` | Auth (bcrypt) |
| `mlb_tracker.py` | Thin shim: same CLI as before (`python mlb_tracker.py …`) |
| `logic/` | Schedule, odds API, projections, player lookup |
| `data/` | League splits JSON, pitcher projections CSV/JSON, team helpers |
| `assets/style.css` | App styling |
| `docs/` | Algorithm notes |

## Optional environment variables

| Variable | Purpose |
|----------|--------|
| `MLB_TRACKER_DB_PATH` | Path to tracker SQLite file |
| `MLB_DATA_DIR` | Writable dir for `league_data_*.json` and projection files |
| `MLB_USER_DATA_DIR` | Per-user `user_settings` root |
| `MLB_ODDS_CACHE_PATH` | Same-day odds JSON cache |
| `MLB_ALLOW_REGISTRATION` | Set to `1` to allow new accounts |
| `MLB_BOOTSTRAP_EMAIL` | With `MLB_BOOTSTRAP_PASSWORD`, creates the first user when the DB has no accounts (Streamlit Cloud / fresh deploy) |
| `MLB_BOOTSTRAP_PASSWORD` | Password for that first user |

## Streamlit Community Cloud

The tracker DB on Cloud is **not** your laptop’s SQLite file. To get a **working login** on a new deploy, add **Secrets** (TOML), for example:

```toml
ODDS_API_KEY = "your_the_odds_api_key"

MLB_BOOTSTRAP_EMAIL = "you@example.com"
MLB_BOOTSTRAP_PASSWORD = "choose-a-strong-password"
```

Redeploy after saving secrets. Sign in with that email and password.

Optional: `MLB_ALLOW_REGISTRATION = "1"` in secrets (as a string) if you want the in-app “Create account” flow for extra users.

**Note:** Pick history on Cloud may reset when the service recycles storage; for durable history use a host with a persistent disk or an external database.

## Push to GitHub

1. Create an empty repo on GitHub (no README/license) named e.g. `mlb-props`.
2. In this folder:

```bash
git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
git push -u origin main
```

Use SSH if you prefer: `git@github.com:YOUR_USER/YOUR_REPO.git`.

## License

Add a `LICENSE` file if you open-source this repo.
