import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import requests
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from utils import ts, moves_to_fen_sequence

CHESSCOM_HEADERS = {
    "User-Agent": "initiative-chess/1.0 (github.com/donotquestionauthority/initiative)"
}
LICHESS_API     = "https://lichess.org/api"
LICHESS_HEADERS = {
    "Accept":     "application/x-ndjson",
    "User-Agent": "initiative-chess/1.0 (github.com/donotquestionauthority/initiative)"
}


# ─── Chess.com fetch ──────────────────────────────────────────────────────────

def _get_chesscom_archives(username: str) -> list:
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    r = requests.get(url, headers=CHESSCOM_HEADERS, timeout=10)
    r.raise_for_status()
    return r.json().get("archives", [])

def _filter_archives_since(archives: list, since_dt: datetime) -> list:
    """Keep only monthly archives that could contain games after since_dt."""
    cutoff = since_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    filtered = []
    for url in archives:
        parts = url.rstrip("/").split("/")
        year, month = int(parts[-2]), int(parts[-1])
        archive_dt = datetime(year, month, 1, tzinfo=timezone.utc)
        if archive_dt >= cutoff:
            filtered.append(url)
    return filtered

def _parse_chesscom_moves(pgn: str) -> list:
    pgn = re.sub(r'\[[^\]]*\]', '', pgn)
    pgn = re.sub(r'\{[^}]*\}', '', pgn)
    pgn = re.sub(r'\s*(1-0|0-1|1/2-1/2)\s*$', '', pgn)
    pgn = re.sub(r'\d+\.+', '', pgn)
    return [m.strip() for m in pgn.split() if m.strip()]

def fetch_opponent_chesscom(username: str, since_dt: datetime) -> list:
    """
    Fetch opponent games from Chess.com since since_dt.
    Returns a list of raw game dicts (no fen_sequence yet).
    """
    print(f"[{ts()}]   Chess.com: fetching {username} since {since_dt.date()}...")
    try:
        archives = _get_chesscom_archives(username)
    except Exception as e:
        print(f"[{ts()}]   Chess.com: failed to get archives for {username}: {e}")
        return []

    archives = _filter_archives_since(archives, since_dt)
    print(f"[{ts()}]   Chess.com: {len(archives)} archive(s) to process.")

    raw_games = []
    for archive_url in archives:
        try:
            r = requests.get(archive_url, headers=CHESSCOM_HEADERS, timeout=10)
            r.raise_for_status()
            games = r.json().get("games", [])
        except Exception as e:
            print(f"[{ts()}]   Chess.com: failed to fetch {archive_url}: {e}")
            continue

        for game in games:
            if game.get("rules") != "chess":
                continue
            pgn = game.get("pgn", "")
            if not pgn:
                continue

            end_time  = game.get("end_time")
            played_at = datetime.fromtimestamp(end_time, tz=timezone.utc) if end_time else None

            if played_at and played_at <= since_dt:
                continue

            white_username = game.get("white", {}).get("username", "").lower()
            played_as      = "white" if white_username == username.lower() else "black"

            pgn_headers = {}
            for match in re.finditer(r'\[(\w+)\s+"([^"]*)"\]', pgn):
                pgn_headers[match.group(1)] = match.group(2)

            eco      = pgn_headers.get("ECO", "")
            eco_url  = pgn_headers.get("ECOUrl", "")
            opening_name = eco_url.split("/openings/")[-1].replace("-", " ") if eco_url else ""

            moves = _parse_chesscom_moves(pgn)
            if not moves:
                continue

            external_game_id = game.get("url", "").split("/")[-1]

            raw_games.append({
                "external_game_id": external_game_id,
                "moves":            moves,
                "played_at":        played_at,
                "played_as":        played_as,
                "opening_eco":      eco,
                "opening_name":     opening_name,
                "raw_pgn":          pgn,
            })

    print(f"[{ts()}]   Chess.com: {len(raw_games)} games fetched.")
    return raw_games


# ─── Lichess fetch ────────────────────────────────────────────────────────────

def fetch_opponent_lichess(username: str, since_dt: datetime) -> list:
    """
    Fetch opponent games from Lichess since since_dt.
    Returns a list of raw game dicts (no fen_sequence yet).
    """
    since_ms = int(since_dt.timestamp() * 1000)
    print(f"[{ts()}]   Lichess: fetching {username} since {since_dt.date()}...")

    url    = f"{LICHESS_API}/games/user/{username}"
    params = {
        "since":   since_ms,
        "moves":   "true",
        "opening": "true",
        "clocks":  "false",
        "evals":   "false",
        "format":  "application/x-ndjson",
    }

    try:
        r = requests.get(url, headers=LICHESS_HEADERS, params=params,
                         stream=True, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[{ts()}]   Lichess: failed to fetch games for {username}: {e}")
        return []

    raw_games = []
    for line in r.iter_lines():
        if not line:
            continue
        try:
            game = json.loads(line)
        except json.JSONDecodeError:
            continue

        variant = game.get("variant", "standard")
        if isinstance(variant, dict):
            variant = variant.get("key", "standard")
        if variant != "standard":
            continue

        source_game_id = game.get("id")
        if not source_game_id:
            continue

        moves_str = game.get("moves", "")
        if not moves_str:
            continue
        moves = moves_str.strip().split()

        played_at_ms = game.get("lastMoveAt") or game.get("createdAt")
        played_at    = (datetime.fromtimestamp(played_at_ms / 1000, tz=timezone.utc)
                        if played_at_ms else None)

        players    = game.get("players", {})
        white_name = players.get("white", {}).get("user", {}).get("name", "").lower()
        played_as  = "white" if white_name == username.lower() else "black"

        opening = game.get("opening", {})

        raw_games.append({
            "external_game_id": source_game_id,
            "moves":            moves,
            "played_at":        played_at,
            "played_as":        played_as,
            "opening_eco":      opening.get("eco", ""),
            "opening_name":     opening.get("name", ""),
            "raw_pgn":          None,
        })

    print(f"[{ts()}]   Lichess: {len(raw_games)} games fetched.")
    return raw_games


# ─── FEN computation — top-level function required for multiprocessing ────────

def compute_fen_for_game(game_dict: dict) -> dict:
    """
    Compute fen_sequence for a raw game dict and return it.
    Must be a module-level function so multiprocessing.Pool can pickle it.
    """
    try:
        game_dict["fen_sequence"] = moves_to_fen_sequence(game_dict["moves"])
    except Exception as e:
        game_dict["fen_sequence"] = []
        game_dict["_fen_error"]   = str(e)
    return game_dict


# ─── DB insert ────────────────────────────────────────────────────────────────

def insert_opponent_games(conn, profile_id: int, source_type: str,
                          games: list) -> int:
    """
    Insert processed games (must already have fen_sequence) into opponent_games,
    and populate opponent_game_fens for fast query-time aggregation.
    Skips games with empty fen_sequence. Returns count of newly inserted rows.
    """
    inserted = 0
    for game in games:
        if not game.get("fen_sequence"):
            continue
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO opponent_games (
                        opponent_profile_id, source_type, moves, fen_sequence,
                        played_at, played_as, opening_eco, opening_name,
                        raw_pgn, external_game_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (opponent_profile_id, source_type, external_game_id)
                    DO NOTHING
                    RETURNING id
                """, (
                    profile_id,
                    source_type,
                    json.dumps(game["moves"]),
                    json.dumps(game["fen_sequence"]),
                    game["played_at"],
                    game["played_as"],
                    game.get("opening_eco", ""),
                    game.get("opening_name", ""),
                    game.get("raw_pgn"),
                    game.get("external_game_id"),
                ))

                row = cur.fetchone()
                if not row:
                    # ON CONFLICT — game already exists, skip FEN rows
                    continue

                game_id   = row["id"]
                inserted += 1

                # Populate opponent_game_fens — one row per FEN.
                # Skip depth 0 (starting position, same for every game).
                fen_rows = [
                    (game_id, profile_id, fen, depth,
                     game["played_at"], game["played_as"])
                    for depth, fen in enumerate(game["fen_sequence"])
                    if depth >= 1
                ]
                if fen_rows:
                    cur.executemany("""
                        INSERT INTO opponent_game_fens
                            (opponent_game_id, opponent_profile_id, fen,
                             depth, played_at, played_as)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, fen_rows)

                # Update opponent_fen_stats summary table for affected FENs
                if fen_rows:
                    cur.execute("""
                        INSERT INTO opponent_fen_stats
                            (opponent_profile_id, fen, max_depth, game_count, most_common_color)
                        SELECT
                            opponent_profile_id,
                            fen,
                            MAX(depth)                                    AS max_depth,
                            COUNT(DISTINCT opponent_game_id)              AS game_count,
                            MODE() WITHIN GROUP (ORDER BY played_as)      AS most_common_color
                        FROM opponent_game_fens
                        WHERE opponent_profile_id = %s
                          AND fen IN %s
                        GROUP BY opponent_profile_id, fen
                        ON CONFLICT (opponent_profile_id, fen) DO UPDATE
                            SET max_depth         = EXCLUDED.max_depth,
                                game_count        = EXCLUDED.game_count,
                                most_common_color = EXCLUDED.most_common_color
                    """, (profile_id, tuple({r[2] for r in fen_rows})))

            conn.commit()

        except Exception as e:
            print(f"[{ts()}]   Insert failed for game "
                  f"{game.get('external_game_id')}: {e}")
            conn.rollback()

    return inserted



def run_opponent_import_pipeline(conn):
    """
    Fetch new games for all initialized, active opponent sources.
    Skips uninitialized profiles — those are handled by setup_opponents.py.
    Single-threaded FEN computation (pipeline budget is tight).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT os.id          AS source_id,
                   os.source_type,
                   os.username,
                   os.last_fetched,
                   op.id          AS profile_id,
                   op.name        AS profile_name
            FROM   opponent_sources os
            JOIN   opponent_profiles op ON op.id = os.opponent_profile_id
            WHERE  os.active          = TRUE
              AND  os.source_type    != 'manual'
              AND  op.active          = TRUE
              AND  op.is_initialized  = TRUE
        """)
        sources = cur.fetchall()

    if not sources:
        print(f"[{ts()}] No initialized opponent sources to process.")
        return

    print(f"[{ts()}] Processing {len(sources)} opponent source(s)...")

    for source in sources:
        profile_id   = source["profile_id"]
        profile_name = source["profile_name"]
        source_type  = source["source_type"]
        username     = source["username"]
        last_fetched = source["last_fetched"]

        since_dt = last_fetched if last_fetched else (
            datetime.now(timezone.utc) - timedelta(days=30)
        )

        print(f"[{ts()}] {profile_name} / {source_type} ({username})...")

        if source_type == "chesscom":
            raw_games = fetch_opponent_chesscom(username, since_dt)
        elif source_type == "lichess":
            raw_games = fetch_opponent_lichess(username, since_dt)
        else:
            continue

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE opponent_sources SET last_fetched = NOW() WHERE id = %s",
                (source["source_id"],)
            )
        conn.commit()

        if not raw_games:
            print(f"[{ts()}] {profile_name} / {source_type}: no new games.")
            continue

        print(f"[{ts()}] Computing FEN sequences for {len(raw_games)} games...")
        processed = [compute_fen_for_game(g) for g in raw_games]

        inserted = insert_opponent_games(conn, profile_id, source_type, processed)
        print(f"[{ts()}] {profile_name} / {source_type}: inserted {inserted} new games.")


if __name__ == "__main__":
    conn = get_conn()
    run_opponent_import_pipeline(conn)
    conn.close()