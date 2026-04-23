"""
fast_pass.py — Phase 1 onboarding analysis.

Analyzes the last ANALYSIS_GAME_LIMIT games for a single player at FAST_PASS_DEPTH
using parallel workers. Designed to complete in ~2-3 minutes on Fargate (4 vCPU).

On completion, sets players.fast_pass_complete = TRUE.
Deep pass (analyze_blunders.py at full depth) runs after and overwrites results
in-place via ON CONFLICT DO UPDATE.

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

from db import get_conn
from config import (
    FAST_PASS_DEPTH,
    STOCKFISH_VERSION,
    ANALYSIS_GAME_LIMIT,
    INACCURACY_THRESHOLD,
    MISTAKE_THRESHOLD,
    BLUNDER_THRESHOLD,
    MISS_THRESHOLD,
    MISS_CONTESTED_GATE,
)
from utils import ts

# Set by main() before Pool — inherited by workers via fork
_STOCKFISH_PATH = None
NUM_WORKERS     = 8   # conservative default for Fargate 4 vCPU


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


def classify(cp_loss: int, eval_before_white: int, player_color: str) -> str | None:
    player_eval = eval_before_white if player_color == "white" else -eval_before_white
    if cp_loss >= MISS_THRESHOLD:
        if abs(player_eval) <= MISS_CONTESTED_GATE:
            return "miss"
    if cp_loss >= BLUNDER_THRESHOLD:
        return "blunder"
    if cp_loss >= MISTAKE_THRESHOLD:
        return "mistake"
    if cp_loss >= INACCURACY_THRESHOLD:
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
    return "endgame" if pieces <= 6 else "middlegame"


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
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    opening_eco  = game_dict.get("opening_eco", "")
    moves        = game_dict["moves"]

    if isinstance(moves, str):
        moves = json.loads(moves)
    if not moves:
        return {"game_id": game_id, "issues": 0, "success": True}

    try:
        engine = chess.engine.SimpleEngine.popen_uci(_STOCKFISH_PATH)
        engine.configure({"Threads": 1})

        board    = chess.Board()
        blunders = []

        for ply, san in enumerate(moves):
            try:
                move = board.parse_san(san)
            except Exception:
                break

            info_before   = engine.analyse(board, chess.engine.Limit(depth=FAST_PASS_DEPTH))
            score_before  = info_before["score"].white().score(mate_score=10000)
            pv            = info_before.get("pv", [])
            best_move_obj = pv[0] if pv else None
            best_move_san = board.san(best_move_obj) if best_move_obj else None
            best_line     = capture_pv_san(board, pv, n=5)

            board.push(move)

            info_after  = engine.analyse(board, chess.engine.Limit(depth=FAST_PASS_DEPTH))
            score_after = info_after["score"].white().score(mate_score=10000)

            if score_before is None or score_after is None:
                continue

            cp_loss = (score_before - score_after) if player_color == "white" \
                      else (score_after - score_before)

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
                game_id, ply, phase, fen,
                san, best_move_san, best_line,
                cp, classification, opening_eco,
                STOCKFISH_VERSION, FAST_PASS_DEPTH,
            ))

        engine.quit()

        conn = get_conn()
        with conn.cursor() as cur:
            for b in blunders:
                cur.execute("""
                    INSERT INTO blunders
                        (game_id, ply, phase, fen, move_played, best_move, best_line,
                         centipawn_loss, classification, opening_eco,
                         engine_version, analysis_depth)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, ply) DO UPDATE SET
                        phase          = EXCLUDED.phase,
                        fen            = EXCLUDED.fen,
                        move_played    = EXCLUDED.move_played,
                        best_move      = EXCLUDED.best_move,
                        best_line      = EXCLUDED.best_line,
                        centipawn_loss = EXCLUDED.centipawn_loss,
                        classification = EXCLUDED.classification,
                        opening_eco    = EXCLUDED.opening_eco,
                        engine_version = EXCLUDED.engine_version,
                        analysis_depth = EXCLUDED.analysis_depth
                """, b)
            cur.execute("""
                UPDATE games
                SET stockfish_analyzed = TRUE,
                    analysis_engine    = %s,
                    analysis_depth     = %s
                WHERE id = %s
            """, (STOCKFISH_VERSION, FAST_PASS_DEPTH, game_id))
        conn.commit()
        conn.close()

        return {"game_id": game_id, "issues": len(blunders), "success": True}

    except Exception as e:
        return {"game_id": game_id, "issues": 0, "success": False, "error": str(e)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global _STOCKFISH_PATH

    parser = argparse.ArgumentParser(description="Fast pass onboarding analysis (depth 12)")
    parser.add_argument("--player-id", type=int, required=True, help="Player ID to analyze")
    parser.add_argument("--workers",   type=int, default=NUM_WORKERS, help="Parallel workers")
    args = parser.parse_args()

    _STOCKFISH_PATH = find_stockfish()

    print(f"[{ts()}] fast_pass.py starting")
    print(f"[{ts()}] Player ID:  {args.player_id}")
    print(f"[{ts()}] Depth:      {FAST_PASS_DEPTH}")
    print(f"[{ts()}] Game limit: {ANALYSIS_GAME_LIMIT}")
    print(f"[{ts()}] Workers:    {args.workers}")
    print(f"[{ts()}] Stockfish:  {_STOCKFISH_PATH}")

    conn       = get_conn()
    games      = get_games_for_player(conn, args.player_id, ANALYSIS_GAME_LIMIT)
    game_dicts = [dict(g) for g in games]
    conn.close()

    print(f"[{ts()}] {len(game_dicts)} games to analyze")

    if not game_dicts:
        print(f"[{ts()}] Nothing to do.")
        return

    total      = len(game_dicts)
    done       = 0
    total_issues = 0
    start_time = time.time()

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

    # Mark fast pass complete
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
