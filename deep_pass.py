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

STOCKFISH_PATH = "/usr/local/bin/stockfish"
NUM_WORKERS    = 16
_SETTINGS      = None  # populated from app_settings at startup


# ─── Helpers ──────────────────────────────────────────────────────────────────

def classify(cp_loss: int, eval_before_white: int, player_color: str) -> str | None:
    s = _SETTINGS
    player_eval = eval_before_white if player_color == "white" else -eval_before_white
    if cp_loss >= s["miss_threshold"]:
        if abs(player_eval) <= s["miss_contested_gate"]:
            return "miss"
    if cp_loss >= s["blunder_threshold"]:
        return "blunder"
    if cp_loss >= s["mistake_threshold"]:
        return "mistake"
    if cp_loss >= s["inaccuracy_threshold"]:
        return "inaccuracy"
    return None


def capture_pv_san(board: chess.Board, pv_moves: list, n: int = 5) -> str | None:
    san_list = []
    b = board.copy()
    for move in pv_moves[:n]:
        try:
            san_list.append(b.san(move))
            b.push(move)
        except Exception:
            break
    return " ".join(san_list) if san_list else None


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
        depth  = _SETTINGS["stockfish_depth"]
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        engine.configure({"Threads": 1})

        board    = chess.Board()
        blunders = []

        for ply, san in enumerate(moves):
            try:
                move = board.parse_san(san)
            except Exception:
                break

            info_before   = engine.analyse(board, chess.engine.Limit(depth=depth))
            score_before  = info_before["score"].white().score(mate_score=10000)
            pv            = info_before.get("pv", [])
            best_move_obj = pv[0] if pv else None
            best_move_san = board.san(best_move_obj) if best_move_obj else None
            best_line     = capture_pv_san(board, pv, n=5)

            board.push(move)

            info_after  = engine.analyse(board, chess.engine.Limit(depth=depth))
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

            if best_move_san and san == best_move_san:
                continue

            cp             = max(0, cp_loss)
            classification = classify(cp, score_before, player_color)
            if classification is None:
                continue

            board.pop()
            fen   = board.fen()
            phase = get_phase(ply, board)
            board.push(move)

            blunders.append((
                game_id,
                ply, phase, fen,
                san, best_move_san, best_line,
                cp, classification,
                opening_eco,
                STOCKFISH_VERSION, depth,
            ))

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
                """, blunders)
            cur.execute("""
                UPDATE games
                SET stockfish_analyzed = TRUE,
                    analysis_engine    = %s,
                    analysis_depth     = %s
                WHERE id = %s
            """, (STOCKFISH_VERSION, depth, game_id))
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

    print(f"[{ts()}] Engine:  {version_line.replace('id name ', '')}")
    print(f"[{ts()}] Depth:   {_SETTINGS['stockfish_depth']}")
    print(f"[{ts()}] Workers: {num_workers}")

    conn      = get_conn()
    _SETTINGS = get_app_settings(conn)

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
