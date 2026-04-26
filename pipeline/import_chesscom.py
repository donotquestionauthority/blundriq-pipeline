import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, log_pipeline_run  # log_pipeline_run used in main()
from config import INITIAL_IMPORT_MONTHS
from utils import ts, moves_to_fen_sequence

HEADERS = {
    "User-Agent": "blundriq/1.0 (github.com/donotquestionauthority/blundriq-pipeline)"
}

# ─── Retry session ────────────────────────────────────────────────────────────
# Retries up to 3 times on connection errors and 5xx responses.
# Backoff: 0s, 2s, 4s between attempts (factor=2, first retry immediate).
def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = _make_session()


def get_archives(username: str) -> list:
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    r = SESSION.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json().get("archives", [])

def get_games_from_archive(url: str) -> list:
    r = SESSION.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json().get("games", [])

def parse_moves(pgn: str) -> list:
    import re
    pgn = re.sub(r'\[[^\]]*\]', '', pgn)
    pgn = re.sub(r'\{[^}]*\}', '', pgn)
    pgn = re.sub(r'\s*(1-0|0-1|1/2-1/2)\s*$', '', pgn)
    pgn = re.sub(r'\d+\.+', '', pgn)
    moves = [m.strip() for m in pgn.split() if m.strip()]
    return moves

def get_result(game: dict, username: str) -> str:
    white = game.get("white", {})
    black = game.get("black", {})
    if white.get("username", "").lower() == username.lower():
        result = white.get("result", "")
    else:
        result = black.get("result", "")
    if result == "win":
        return "win"
    elif result in ("checkmated", "timeout", "resigned", "lose", "abandoned"):
        return "loss"
    else:
        return "draw"

def get_player_color(game: dict, username: str) -> str:
    white = game.get("white", {}).get("username", "").lower()
    return "white" if white == username.lower() else "black"

def get_opponent(game: dict, username: str) -> tuple:
    white = game.get("white", {})
    black = game.get("black", {})
    if white.get("username", "").lower() == username.lower():
        opponent = black
        player = white
    else:
        opponent = white
        player = black
    return (
        opponent.get("username", ""),
        opponent.get("rating", None),
        player.get("rating", None)
    )

def parse_pgn_headers(pgn: str) -> dict:
    import re
    headers = {}
    for match in re.finditer(r'\[(\w+)\s+"([^"]*)"\]', pgn):
        headers[match.group(1)] = match.group(2)
    return headers

def filter_recent_archives(archives: list, months: int) -> list:
    cutoff = datetime.now(timezone.utc) - relativedelta(months=months)
    filtered = []
    for url in archives:
        parts = url.rstrip("/").split("/")
        year, month = int(parts[-2]), int(parts[-1])
        archive_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if archive_date >= cutoff:
            filtered.append(url)
    return filtered

def import_chesscom_games(conn, player: dict, months: int = None, game_limit: int = None):
    username = player["chesscom_username"]
    if not username:
        print(f"[{ts()}] Player {player['user_display_name']} has no Chess.com username, skipping.")
        return 0

    # all_history=True fetches all archives regardless of months cutoff
    all_history = (game_limit is None)
    if months is None:
        months = INITIAL_IMPORT_MONTHS

    print(f"[{ts()}] Fetching Chess.com games for {username}...")
    archives = get_archives(username)
    if not all_history:
        archives = filter_recent_archives(archives, months)
    else:
        # Sort newest-first so we fill the cap with most recent games
        archives = sorted(archives, reverse=True)
    print(f"[{ts()}] Found {len(archives)} archive(s) to process.")

    inserted = 0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(played_at) FROM games
            WHERE player_id = %s AND source = 'chesscom'
        """, (player["id"],))
        row = cur.fetchone()
        latest = row["max"] if row and row["max"] else None

    for archive_url in archives:
        if game_limit is not None and inserted >= game_limit:
            print(f"[{ts()}] Game limit of {game_limit} reached, stopping import.")
            break
        try:
            games = get_games_from_archive(archive_url)
        except Exception as e:
            print(f"[{ts()}] Failed to fetch {archive_url}: {e}")
            continue

        for game in games:
            if game_limit is not None and inserted >= game_limit:
                break
            if game.get("rules") != "chess":
                continue
            pgn = game.get("pgn", "")
            if not pgn:
                continue

            end_time = game.get("end_time")
            played_at = datetime.fromtimestamp(end_time, tz=timezone.utc) if end_time else None

            # In incremental mode, skip games older than the newest already imported.
            # In all_history mode, skip this check and rely on ON CONFLICT DO NOTHING.
            if not all_history and latest and played_at and played_at <= latest:
                continue

            source_game_id = game.get("url", "").split("/")[-1]
            player_color = get_player_color(game, username)
            opponent_username, opponent_rating, player_rating = get_opponent(game, username)
            result = get_result(game, username)
            moves = parse_moves(pgn)
            headers = parse_pgn_headers(pgn)
            eco = headers.get("ECO", "")
            eco_url = headers.get("ECOUrl", "")
            opening_name = eco_url.split("/openings/")[-1].replace("-", " ") if eco_url else ""
            opening = {"name": opening_name, "eco": eco}

            try:
                with conn.cursor() as cur:
                    fen_seq = moves_to_fen_sequence(moves)
                    cur.execute("""
                        INSERT INTO games (
                            player_id, source, source_game_id, url,
                            player_color, opponent_username, opponent_rating,
                            player_rating, time_control, result, moves,
                            fen_sequence, opening_name, opening_eco, played_at
                        ) VALUES (
                            %s, 'chesscom', %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                        ON CONFLICT (source, source_game_id) DO NOTHING
                    """, (
                        player["id"],
                        source_game_id,
                        game.get("url"),
                        player_color,
                        opponent_username,
                        opponent_rating,
                        player_rating,
                        game.get("time_control"),
                        result,
                        json.dumps(moves),
                        json.dumps(fen_seq),
                        opening.get("name", ""),
                        opening.get("eco", ""),
                        played_at
                    ))
                    if cur.rowcount > 0:
                        inserted += 1
                conn.commit()
            except Exception as e:
                print(f"[{ts()}] Failed to insert game {source_game_id}: {e}")
                conn.rollback()

    print(f"[{ts()}] Imported {inserted} new Chess.com games for {username}.")

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE players SET chesscom_last_checked = NOW()
            WHERE id = %s
        """, (player["id"],))
    conn.commit()

    return inserted

def main():
    conn = get_conn()
    run_id = log_pipeline_run(conn, status="running")
    print(f"[{ts()}] Pipeline run {run_id} started (import_chesscom).")

    try:
        players = get_all_active_players(conn)
        print(f"[{ts()}] Found {len(players)} active players.")
        total_inserted = 0
        for player in players:
            print(f"[{ts()}] Processing {player['user_display_name']}...")
            inserted = import_chesscom_games(conn, player, months=1)
            total_inserted += inserted
        print(f"[{ts()}] Total imported: {total_inserted} new Chess.com games.")
        log_pipeline_run(conn, status="completed", games_imported=total_inserted, run_id=run_id)
    except Exception as e:
        print(f"[{ts()}] Pipeline run {run_id} failed: {e}")
        try:
            log_pipeline_run(conn, status="failed", error_message=str(e)[:500], run_id=run_id)
        except Exception:
            pass
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
