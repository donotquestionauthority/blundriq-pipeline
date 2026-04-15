import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import re
import time
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players
from utils import ts

HEADERS = {
    "User-Agent": "initiative-chess/1.0 (github.com/donotquestionauthority/initiative)"
}

def parse_pgn_headers(pgn: str) -> dict:
    headers = {}
    for match in re.finditer(r'\[(\w+)\s+"([^"]*)"\]', pgn):
        headers[match.group(1)] = match.group(2)
    return headers

def get_opening_from_eco_url(eco_url: str) -> str:
    if not eco_url:
        return ""
    name = eco_url.split("/openings/")[-1].replace("-", " ")
    return name

def get_games_from_archive(url: str) -> list:
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("games", [])

def get_archives(username: str) -> list:
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json().get("archives", [])

def main():
    conn = get_conn()
    players = get_all_active_players(conn)

    for player in players:
        username = player["chesscom_username"]
        if not username:
            continue

        print(f"[{ts()}] Backfilling openings for {username}...")

        # Get all chesscom games with empty opening names
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, source_game_id
                FROM games
                WHERE player_id = %s
                  AND source = 'chesscom'
                  AND (opening_name IS NULL OR opening_name = '')
            """, (player["id"],))
            games_to_fix = {row["source_game_id"]: row["id"] for row in cur.fetchall()}

        print(f"[{ts()}] Found {len(games_to_fix)} games needing opening names.")

        if not games_to_fix:
            continue

        archives = get_archives(username)
        print(f"[{ts()}] Fetching {len(archives)} archives...")

        updated = 0
        for archive_url in archives:
            try:
                games = get_games_from_archive(archive_url)
            except Exception as e:
                print(f"[{ts()}] Failed to fetch {archive_url}: {e}")
                continue

            for game in games:
                source_game_id = game.get("url", "").split("/")[-1]
                if source_game_id not in games_to_fix:
                    continue

                pgn = game.get("pgn", "")
                if not pgn:
                    continue

                headers = parse_pgn_headers(pgn)
                eco = headers.get("ECO", "")
                eco_url = headers.get("ECOUrl", "")
                opening_name = get_opening_from_eco_url(eco_url)

                if not opening_name and not eco:
                    continue

                game_id = games_to_fix[source_game_id]
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE games
                        SET opening_name = %s,
                            opening_eco  = %s
                        WHERE id = %s
                    """, (opening_name, eco, game_id))
                conn.commit()
                updated += 1

            time.sleep(0.5)  # be nice to Chess.com API

        print(f"[{ts()}] Updated {updated} games with opening names.")

    conn.close()
    print(f"[{ts()}] Done!")

if __name__ == "__main__":
    main()