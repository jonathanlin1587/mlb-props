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
streamlit run mlb_prop_app.py
```

## Project layout

| Path | Purpose |
|------|--------|
| `mlb_prop_app.py` | Streamlit entrypoint |
| `mlb_tracker.py` | SQLite tracker schema and CLI helpers |
| `mlb_accounts.py` | Auth (bcrypt) |
| `logic/` | Schedule, odds API, projections, player lookup |
| `data/` | League splits JSON, pitcher projections CSV/JSON, team helpers |
| `style.css` | App styling |
| `docs/` | Algorithm notes |

## Optional environment variables

| Variable | Purpose |
|----------|--------|
| `MLB_TRACKER_DB_PATH` | Path to tracker SQLite file |
| `MLB_DATA_DIR` | Writable dir for `league_data_*.json` and projection files |
| `MLB_USER_DATA_DIR` | Per-user `user_settings` root |
| `MLB_ODDS_CACHE_PATH` | Same-day odds JSON cache |
| `MLB_ALLOW_REGISTRATION` | Set to `1` to allow new accounts |

## License

Add a `LICENSE` file if you open-source this repo.
