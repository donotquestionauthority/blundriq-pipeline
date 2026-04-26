"""
deep_pass.py — Fargate phase-2 analysis. Re-runs Stockfish at full depth (18)
on the most recent ANALYSIS_GAME_LIMIT games for a player.

Deletes existing blunders for each game before inserting fresh results —
this ensures stale rows from prior analyses or threshold changes are never
left behind (ON CONFLICT alone is insufficient when thresholds change).

Usage:
    python deep_pass.py                        # dry run — shows counts, touches nothing
    python deep_pass.py --run                  # reanalyze all active players
    python deep_pass.py --run --player-id 1   # single player by ID (Fargate)
    python deep_pass.py --run --player rob    # single player by name (local)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
import time
import subprocess
import chess
import chess.engine
from multiprocessing import Pool
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, get_analysis_game_limit, get_app_settings
from config import STOCKFISH_VERSION
from utils import ts
from pipeline.analysis_core import analyze_game_full

STOCKFISH_PATH = "/usr/local/bin/stockfish"
NUM_WORKERS    = 16
_SETTINGS      = None  # populated from app_settings at startup; inherited by workers via fork


# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_all_games_for_player(conn, player_id: int, since=None, limit: int = None) -> list:
    with conn.cursor() as cur:
        query  = """
            SELECT g.id, g.moves, g.opening_eco, g.player_color, g.played_at
            FROM games g
            WHERE g.player_id = %s AND g.moves IS NOT NULL
        """
        params = [player_id]
        if since:
            query += " AND g.played_at >= %s"
            params.append(since)
        query += " ORDER BY g.played_at DESC"
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        cur.execute(query, params)
        return cur.fetchall()


# ─── Worker — analyze + save in one function so nothing large crosses the queue

def analyze_and_save_game(game_dict: dict) -> dict:
    """
    Analyzes one game and writes results directly to DB inside the worker process.
    Only returns a tiny summary dict back through the multiprocessing queue.
    Uses module-level _SETTINGS (set in main() before Pool, inherited via fork).
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    depth        = _SETTINGS["stockfish_depth"]

    moves = game_dict["moves"]
    if isinstance(moves, str):
        moves = json.loads(moves)

    if not moves:
        return {"game_id": game_id, "inserted": 0, "updated": 0, "issues": 0, "success": True}

    try:
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        engine.configure({"Threads": 1})

        blunders, peak_advantage, final_eval = analyze_game_full(engine, game_dict, player_color, _SETTINGS, depth=depth)

        engine.quit()

        # Write directly to DB in this worker process.
        # Delete existing blunders first — ON CONFLICT alone leaves stale rows
        # when thresholds change (plies no longer classified won't be overwritten).
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM blunders WHERE game_id = %s", (game_id,))
            if blunders:
                cur.executemany("""
                    INSERT INTO blunders
                        (game_id, ply, phase, fen, move_played, best_move, best_line,
                         centipawn_loss, classification, opening_eco,
                         engine_version, analysis_depth)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [
                    (
                        game_id,
                        b["ply"], b["phase"], b["fen"],
                        b["move_played"], b["best_move"], b["best_line"],
                        b["centipawn_loss"], b["classification"],
                        b["opening_eco"],
                        STOCKFISH_VERSION, depth,
                    )
                    for b in blunders
                ])
            cur.execute("""
                UPDATE games
                SET stockfish_analyzed = TRUE,
                    analysis_engine    = %s,
                    analysis_depth     = %s,
                    peak_advantage     = %s,
                    final_eval         = %s
                WHERE id = %s
            """, (STOCKFISH_VERSION, depth, peak_advantage, final_eval, game_id))
        conn.commit()
        conn.close()

        return {"game_id": game_id, "inserted": len(blunders), "updated": 0, "issues": len(blunders), "success": True}

    except Exception as e:
        return {"game_id": game_id, "inserted": 0, "updated": 0, "issues": 0, "success": False, "error": str(e)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global _SETTINGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--run",       action="store_true", help="Actually run (default is dry run)")
    parser.add_argument("--player",    type=str, default=None, help="Filter to one player by name (case-insensitive)")
    parser.add_argument("--player-id", type=int, default=None, help="Filter to one player by ID (used by Fargate worker)")
    parser.add_argument("--days",      type=int, default=None, help="Only reanalyze games from the last N days")
    parser.add_argument("--workers",   type=int, default=NUM_WORKERS, help=f"Parallel workers (default {NUM_WORKERS})")
    args = parser.parse_args()

    dry_run = not args.run
    if dry_run:
        print(f"[{ts()}] DRY RUN — pass --run to execute")

    since = None
    if args.days:
        from datetime import datetime, timezone, timedelta
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        print(f"[{ts()}] Limiting to games since {since.date()} (--days {args.days})")

    result = subprocess.run(
        [STOCKFISH_PATH], input="uci\nquit\n",
        capture_output=True, text=True, timeout=10
    )
    version_line = next(
        (l for l in result.stdout.splitlines() if l.startswith("id name")), "unknown"
    )
    num_workers = args.workers

    conn      = get_conn()
    _SETTINGS = get_app_settings(conn)

    print(f"[{ts()}] Engine:  {version_line.replace('id name ', '')}")
    print(f"[{ts()}] Depth:   {_SETTINGS['stockfish_depth']}")
    print(f"[{ts()}] Workers: {num_workers}")

    # When called from Fargate with --player-id, fetch the player directly
    # without requiring is_initialized = TRUE (used during onboarding deep pass)
    if args.player_id:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*, u.email, u.display_name as user_display_name
                FROM players p
                JOIN users u ON u.id = p.user_id
                WHERE p.id = %s AND p.active = TRUE AND u.active = TRUE
            """, (args.player_id,))
            row = cur.fetchone()
        if not row:
            print(f"[{ts()}] No player with ID {args.player_id} found.")
            conn.close()
            return
        players = [row]
    else:
        players = get_all_active_players(conn)

    if args.player:
        players = [p for p in players if args.player.lower() in p["user_display_name"].lower()]
        if not players:
            print(f"[{ts()}] No player matching '{args.player}' found.")
            conn.close()
            return

    print(f"[{ts()}] Players: {[p['user_display_name'] for p in players]}")

    if dry_run:
        for player in players:
            limit = get_analysis_game_limit(conn, player["id"])
            games = get_all_games_for_player(conn, player["id"], since=since, limit=limit)
            print(f"[{ts()}] {player['user_display_name']}: {len(games)} games would be reanalyzed (limit={limit})")
        conn.close()
        return

    for player in players:
        print(f"\n[{ts()}] Processing {player['user_display_name']}...")
        limit      = get_analysis_game_limit(conn, player["id"])
        games      = get_all_games_for_player(conn, player["id"], since=since, limit=limit)
        game_dicts = [dict(g) for g in games]
        print(f"[{ts()}] {len(game_dicts)} games to reanalyze with {num_workers} workers (limit={limit})")

        if not game_dicts:
            print(f"[{ts()}] Nothing to do.")
            continue

        # Mark deep pass in-progress: clear both completion flags so admin UI
        # shows accurate state while the job runs.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE players SET deep_pass_complete = FALSE, is_initialized = FALSE WHERE id = %s",
                (player["id"],),
            )
        conn.commit()
        print(f"[{ts()}] deep_pass_complete=FALSE, is_initialized=FALSE for player {player['id']}")

        # Close the main connection before the Pool starts — it would sit idle
        # for 30+ minutes while workers run, causing Supabase to terminate it.
        # Each worker opens its own connection internally so this is safe.
        try:
            conn.close()
        except Exception:
            pass
        conn = get_conn()  # fresh connection for next player after Pool completes

        total          = len(game_dicts)
        done           = 0
        total_inserted = 0
        total_updated  = 0
        start_time     = time.time()

        with Pool(processes=num_workers) as pool:
            for result in pool.imap_unordered(analyze_and_save_game, game_dicts):
                done += 1

                if result["success"]:
                    total_inserted += result["inserted"]
                    total_updated  += result["updated"]
                    issues = result["issues"]
                else:
                    issues = 0
                    print(f"[{ts()}] Game {result['game_id']} failed: {result.get('error')}")

                elapsed   = time.time() - start_time
                rate      = done / elapsed * 60 if elapsed > 0 else 0
                remaining = (total - done) / rate / 60 if rate > 0 else 0

                print(
                    f"[{ts()}] {done}/{total} | "
                    f"{issues} issues | "
                    f"{total_inserted} new, {total_updated} updated | "
                    f"~{remaining:.1f} hrs remaining"
                )

        elapsed_hrs = (time.time() - start_time) / 3600
        print(
            f"[{ts()}] Done {player['user_display_name']} in {elapsed_hrs:.2f} hrs — "
            f"{total_inserted} new blunders, {total_updated} updated in-place"
        )

        # Mark deep pass complete — re-enables hourly pipeline for this player.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE players SET deep_pass_complete = TRUE, is_initialized = TRUE WHERE id = %s",
                (player["id"],),
            )
        conn.commit()
        print(f"[{ts()}] deep_pass_complete=TRUE, is_initialized=TRUE for player {player['id']}")

    conn.close()


if __name__ == "__main__":
    main()
