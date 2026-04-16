import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players
from config import INITIAL_IMPORT_MONTHS
from utils import ts, moves_to_fen_sequence

LICHESS_API = "https://lichess.org/api"
HEADERS = {
    "Accept": "application/x-ndjson",
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


def get_result(game: dict, username: str) -> str:
    winner = game.get("winner")
    status = game.get("status")
    if status in ("draw", "stalemate"):
        return "draw"
    if winner is None:
        return "draw"
    players = game.get("players", {})
    white_user = players.get("white", {}).get("user", {}).get("name", "").lower()
    if winner == "white":
        return "win" if white_user == username.lower() else "loss"
    else:
        return "win" if white_user != username.lower() else "loss"

def get_player_color(game: dict, username: str) -> str:
    players = game.get("players", {})
    white_user = players.get("white", {}).get("user", {}).get("name", "").lower()
    return "white" if white_user == username.lower() else "black"

def get_opponent_info(game: dict, username: str) -> tuple:
    players = game.get("players", {})
    white = players.get("white", {})
    black = players.get("black", {})
    white_name = white.get("user", {}).get("name", "").lower()
    if white_name == username.lower():
        opponent = black
        player = white
    else:
        opponent = white
        player = black
    return (
        opponent.get("user", {}).get("name", ""),
        opponent.get("rating", None),
        player.get("rating", None)
    )

def parse_moves(moves_str: str) -> list:
    if not moves_str:
        return []
    return moves_str.strip().split()

def get_cutoff_timestamp(conn, player_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(played_at) FROM games
            WHERE player_id = %s AND source = 'lichess'
        """, (player_id,))
        row = cur.fetchone()
        if row and row["max"]:
            return int(row["max"].timestamp() * 1000)
    from dateutil.relativedelta import relativedelta
    cutoff = datetime.now(timezone.utc) - relativedelta(months=INITIAL_IMPORT_MONTHS)
    return int(cutoff.timestamp() * 1000)

def import_lichess_games(conn, player: dict):
    username = player["lichess_username"]
    if not username:
        print(f"[{ts()}] Player {player['user_display_name']} has no Lichess username, skipping.")
        return 0

    since = get_cutoff_timestamp(conn, player["id"])
    print(f"[{ts()}] Fetching Lichess games for {username} since {datetime.fromtimestamp(since/1000)}...")

    url = f"{LICHESS_API}/games/user/{username}"
    params = {
        "since": since,
        "moves": "true",
        "opening": "true",
        "clocks": "false",
        "evals": "false",
        "format": "application/x-ndjson"
    }

    r = SESSION.get(url, headers=HEADERS, params=params, stream=True, timeout=30)
    r.raise_for_status()

    inserted = 0
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

        played_at_ms = game.get("lastMoveAt") or game.get("createdAt")
        played_at = datetime.fromtimestamp(played_at_ms / 1000, tz=timezone.utc) if played_at_ms else None

        player_color = get_player_color(game, username)
        opponent_username, opponent_rating, player_rating = get_opponent_info(game, username)
        result = get_result(game, username)
        moves = parse_moves(game.get("moves", ""))

        if not moves:
            continue

        opening = game.get("opening", {})
        time_control = game.get("clock", {})
        tc_str = f"{time_control.get('initial', 0)}+{time_control.get('increment', 0)}" if time_control else game.get("speed", "")

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
                        %s, 'lichess', %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (source, source_game_id) DO NOTHING
                """, (
                    player["id"],
                    source_game_id,
                    f"https://lichess.org/{source_game_id}",
                    player_color,
                    opponent_username,
                    opponent_rating,
                    player_rating,
                    tc_str,
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

    print(f"[{ts()}] Imported {inserted} new Lichess games for {username}.")

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE players SET lichess_last_checked = NOW()
            WHERE id = %s
        """, (player["id"],))
    conn.commit()

    return inserted

def main():
    conn = get_conn()
    players = get_all_active_players(conn)
    print(f"[{ts()}] Found {len(players)} active players.")
    total_inserted = 0
    for player in players:
        print(f"[{ts()}] Processing {player['user_display_name']}...")
        inserted = import_lichess_games(conn, player)
        total_inserted += inserted
    print(f"[{ts()}] Total imported: {total_inserted} new Lichess games.")
    conn.close()

if __name__ == "__main__":
    main()