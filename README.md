# blundriq-pipeline

Hourly data pipeline for BlundrIQ. Imports chess games from Chess.com and Lichess, matches positions against opening repertoire lines, and analyzes blunders with Stockfish 18.

## Pipeline Steps

1. **Import Chess.com games** — incremental fetch for all active players
2. **Import Lichess games** — incremental fetch for all active players
3. **Match repertoire** — FEN subsequence matching against declared opening lines
4. **Analyze blunders** — Stockfish depth 18 analysis on unanalyzed games
5. **Import opponent games** — incremental fetch for opponent profiles

## Schedule

Runs hourly via GitHub Actions. Concurrency protection prevents overlapping runs.

## Stack

| Component | Technology |
|---|---|
| Pipeline | GitHub Actions (hourly cron) |
| Chess engine | Stockfish 18 |
| Database | Supabase (Postgres, port 6543) |
| Bulk analysis | Dell PowerEdge T420 (ESXi) — `analyze_parallel.py` |
| Language | Python 3.13 |

## Local / Dell Setup

```bash
cp .env.example .env
# Fill in credentials
pip install -r requirements.txt

# Run individual steps manually
python pipeline/import_chesscom.py
python pipeline/import_lichess.py
python pipeline/match_repertoire.py
python pipeline/analyze_blunders.py
python pipeline/import_opponent_games.py

# Bulk parallel analysis (Dell only)
python analyze_parallel.py

# New player onboarding (Dell only)
python setup.py
```

## Environment Variables

See `.env.example` for required variables.

## GitHub Actions Secrets Required

- `DATABASE_URL` — Supabase pooler connection string (port 6543)
- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_KEY` — Supabase anon key