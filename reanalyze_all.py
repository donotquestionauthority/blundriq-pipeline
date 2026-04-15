"""
reanalyze_all.py — Re-run Stockfish analysis on ALL games for all active players,
using the same parallel worker pattern as analyze_parallel.py.

Key differences from analyze_parallel.py:
- Processes ALL games regardless of stockfish_analyzed flag
- Upserts blunders with ON CONFLICT DO UPDATE (overwrites old evaluations in-place)
- Each worker saves directly to DB — avoids BrokenPipeError from large queue payloads
- Supports --player flag to target a single player

Usage:
    python reanalyze_all.py              # dry run — shows counts, touches nothing
    python reanalyze_all.py --run        # reanalyze everyone
    python reanalyze_all.py --run --player rob   # single player (case-insensitive)
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

from db import get_conn, get_all_active_players
from config import (
    STOCKFISH_DEPTH,
    INACCURACY_THRESHOLD,
    MISTAKE_THRESHOLD,
    BLUNDER_THRESHOLD,
    MISS_THRESHOLD,
)
from utils import ts

STOCKFISH_PATH = "/usr/local/bin/stockfish"
NUM_WORKERS    = 16


# ─── Helpers ──────────────────────────────────────────────────────────────────

def classify(centipawn_loss: int) -> str | None:
    if centipawn_loss >= MISS_THRESHOLD:
        return "miss"
    elif centipawn_loss >= BLUNDER_THRESHOLD:
        return "blunder"
    elif centipawn_loss >= MISTAKE_THRESHOLD:
        return "mistake"
    elif centipawn_loss >= INACCURACY_THRESHOLD:
        return "inaccuracy"
    return None


def get_phase(ply: int, board: chess.Board) -> str:
    if ply < 20:
        return "opening"
    pieces = sum(
        len(board.pieces(pt, color))
        for color in chess.COLORS
        for pt in [chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT]
    )
    if pieces <= 6:
        return "endgame"
    return "middlegame"


def get_all_games_for_player(conn, player_id: int, since=None) -> list:
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
        cur.execute(query, params)
        return cur.fetchall()


# ─── Worker — analyze + save in one function so nothing large crosses the queue

def analyze_and_save_game(game_dict: dict) -> dict:
    """
    Analyzes one game and writes results directly to DB inside the worker process.
    Only returns a tiny summary dict back through the multiprocessing queue.
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    opening_eco  = game_dict.get("opening_eco", "")
    moves        = game_dict["moves"]

    if isinstance(moves, str):
        moves = json.loads(moves)

    if not moves:
        return {"game_id": game_id, "inserted": 0, "updated": 0, "issues": 0, "success": True}

    try:
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        engine.configure({"Threads": 1})

        board    = chess.Board()
        blunders = []

        for ply, san in enumerate(moves):
            try:
                move = board.parse_san(san)
            except Exception:
                break

            info_before   = engine.analyse(board, chess.engine.Limit(depth=STOCKFISH_DEPTH))
            score_before  = info_before["score"].white().score(mate_score=10000)
            best_move     = info_before.get("pv", [None])[0]
            best_move_san = board.san(best_move) if best_move else None

            board.push(move)

            info_after  = engine.analyse(board, chess.engine.Limit(depth=STOCKFISH_DEPTH))
            score_after = info_after["score"].white().score(mate_score=10000)

            if score_before is None or score_after is None:
                continue

            cp_loss = (score_before - score_after) if player_color == "white" else (score_after - score_before)

            is_player_move = (
                (ply % 2 == 0 and player_color == "white") or
                (ply % 2 == 1 and player_color == "black")
            )
            if not is_player_move:
                continue

            classification = classify(cp_loss)
            if classification is None:
                continue

            board.pop()
            fen   = board.fen()
            phase = get_phase(ply, board)
            board.push(move)

            blunders.append((
                game_id,
                ply, phase, fen,
                san, best_move_san,
                max(0, cp_loss), classification,
                opening_eco,
            ))

        engine.quit()

        # Write directly to DB in this worker process
        inserted = updated = 0
        conn = get_conn()
        with conn.cursor() as cur:
            for b in blunders:
                cur.execute("""
                    INSERT INTO blunders
                        (game_id, ply, phase, fen, move_played, best_move,
                         centipawn_loss, classification, opening_eco)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, ply) DO UPDATE SET
                        phase          = EXCLUDED.phase,
                        fen            = EXCLUDED.fen,
                        move_played    = EXCLUDED.move_played,
                        best_move      = EXCLUDED.best_move,
                        centipawn_loss = EXCLUDED.centipawn_loss,
                        classification = EXCLUDED.classification,
                        opening_eco    = EXCLUDED.opening_eco
                    RETURNING (xmax = 0) AS was_inserted
                """, b)
                row = cur.fetchone()
                if row and row["was_inserted"]:
                    inserted += 1
                else:
                    updated += 1

            cur.execute("UPDATE games SET stockfish_analyzed = TRUE WHERE id = %s", (game_id,))
        conn.commit()
        conn.close()

        return {"game_id": game_id, "inserted": inserted, "updated": updated, "issues": len(blunders), "success": True}

    except Exception as e:
        return {"game_id": game_id, "inserted": 0, "updated": 0, "issues": 0, "success": False, "error": str(e)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run",    action="store_true", help="Actually run (default is dry run)")
    parser.add_argument("--player", type=str, default=None, help="Filter to one player (case-insensitive)")
    parser.add_argument("--days",   type=int, default=None, help="Only reanalyze games from the last N days")
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
    print(f"[{ts()}] Engine:  {version_line.replace('id name ', '')}")
    print(f"[{ts()}] Depth:   {STOCKFISH_DEPTH}")
    print(f"[{ts()}] Workers: {NUM_WORKERS}")

    conn = get_conn()
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
            games = get_all_games_for_player(conn, player["id"], since=since)
            print(f"[{ts()}] {player['user_display_name']}: {len(games)} games would be reanalyzed")
        conn.close()
        return

    for player in players:
        print(f"\n[{ts()}] Processing {player['user_display_name']}...")
        games      = get_all_games_for_player(conn, player["id"], since=since)
        game_dicts = [dict(g) for g in games]
        print(f"[{ts()}] {len(game_dicts)} games to reanalyze with {NUM_WORKERS} workers")

        if not game_dicts:
            print(f"[{ts()}] Nothing to do.")
            continue

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

        with Pool(processes=NUM_WORKERS) as pool:
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

    conn.close()


if __name__ == "__main__":
    main()