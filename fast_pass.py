"""
fast_pass.py — Phase 1 onboarding analysis.

Analyzes the most recent games (up to analysis_game_limit) for a single player
at fast_pass_depth using parallel workers. Designed to complete in ~5 minutes
on Fargate (16 vCPU).

On completion, sets players.fast_pass_complete = TRUE.
Deep pass runs after and overwrites results at full depth.

Usage:
    python fast_pass.py --player-id 1
    python fast_pass.py --player-id 1 --workers 8
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
import time
from multiprocessing import Pool
from dotenv import load_dotenv
load_dotenv()

import chess
import chess.engine

from db import get_conn, get_app_settings, get_analysis_game_limit
from config import STOCKFISH_VERSION
from utils import ts
from pipeline.analysis_core import analyze_game_full

# Set by main() before Pool — inherited by workers via fork
_STOCKFISH_PATH = None
_SETTINGS       = None  # populated from app_settings at startup
NUM_WORKERS     = 16    # Fargate 16 vCPU


# ─── Helpers ──────────────────────────────────────────────────────────────────

def find_stockfish() -> str:
    candidates = [
        "/usr/local/bin/stockfish",
        "/opt/homebrew/bin/stockfish",
        "/usr/bin/stockfish",
        "/usr/games/stockfish",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    import shutil
    path = shutil.which("stockfish")
    if path:
        return path
    raise FileNotFoundError("Stockfish not found.")


def get_games_for_player(conn, player_id: int, limit: int) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, moves, opening_eco, player_color
            FROM games
            WHERE player_id = %s AND moves IS NOT NULL
            ORDER BY played_at DESC
            LIMIT %s
        """, (player_id, limit))
        return cur.fetchall()


# ─── Worker ───────────────────────────────────────────────────────────────────

def analyze_and_save_game(game_dict: dict) -> dict:
    """
    Multiprocessing worker. Spawns its own Stockfish instance, analyzes one game,
    writes blunders directly to DB, returns a small summary dict.
    Uses module-level _SETTINGS (inherited via fork from main process).
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    depth        = _SETTINGS["fast_pass_depth"]

    moves = game_dict["moves"]
    if isinstance(moves, str):
        moves = json.loads(moves)
    if not moves:
        return {"game_id": game_id, "issues": 0, "success": True}

    try:
        engine = chess.engine.SimpleEngine.popen_uci(_STOCKFISH_PATH)
        engine.configure({"Threads": 1})

        blunders, peak_advantage = analyze_game_full(engine, game_dict, player_color, _SETTINGS, depth=depth)

        engine.quit()

        conn = get_conn()
        with conn.cursor() as cur:
            # Delete before insert — ON CONFLICT alone leaves stale rows when
            # thresholds change (plies no longer classified won't be overwritten).
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
                    peak_advantage     = %s
                WHERE id = %s
            """, (STOCKFISH_VERSION, depth, peak_advantage, game_id))
        conn.commit()
        conn.close()

        return {"game_id": game_id, "issues": len(blunders), "success": True}

    except Exception as e:
        return {"game_id": game_id, "issues": 0, "success": False, "error": str(e)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global _STOCKFISH_PATH, _SETTINGS

    parser = argparse.ArgumentParser(description="Fast pass onboarding analysis")
    parser.add_argument("--player-id", type=int, required=True, help="Player ID to analyze")
    parser.add_argument("--workers",   type=int, default=NUM_WORKERS, help="Parallel workers")
    args = parser.parse_args()

    _STOCKFISH_PATH = find_stockfish()

    conn      = get_conn()
    _SETTINGS = get_app_settings(conn)
    limit     = get_analysis_game_limit(conn, args.player_id)
    games     = get_games_for_player(conn, args.player_id, limit)
    game_dicts = [dict(g) for g in games]
    conn.close()

    print(f"[{ts()}] fast_pass.py starting")
    print(f"[{ts()}] Player ID:  {args.player_id}")
    print(f"[{ts()}] Depth:      {_SETTINGS['fast_pass_depth']}")
    print(f"[{ts()}] Game limit: {limit}")
    print(f"[{ts()}] Workers:    {args.workers}")
    print(f"[{ts()}] Stockfish:  {_STOCKFISH_PATH}")
    print(f"[{ts()}] {len(game_dicts)} games to analyze")

    if not game_dicts:
        print(f"[{ts()}] Nothing to do.")
        return

    total        = len(game_dicts)
    done         = 0
    total_issues = 0
    start_time   = time.time()

    with Pool(processes=args.workers) as pool:
        for result in pool.imap_unordered(analyze_and_save_game, game_dicts):
            done += 1
            if result["success"]:
                total_issues += result["issues"]
            else:
                print(f"[{ts()}] Game {result['game_id']} failed: {result.get('error')}")

            elapsed   = time.time() - start_time
            rate      = done / elapsed * 60 if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            print(f"[{ts()}] {done}/{total} | {result.get('issues', 0)} issues | "
                  f"~{remaining:.1f} min remaining")

    elapsed_min = (time.time() - start_time) / 60
    print(f"[{ts()}] Fast pass complete in {elapsed_min:.1f} min — {total_issues} issues found")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE players SET fast_pass_complete = TRUE WHERE id = %s
        """, (args.player_id,))
    conn.commit()
    conn.close()
    print(f"[{ts()}] players.fast_pass_complete = TRUE for player {args.player_id}")


if __name__ == "__main__":
    main()
