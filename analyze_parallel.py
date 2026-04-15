import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import time
import chess
import chess.engine
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, get_unanalyzed_games_for_player
from config import (
    STOCKFISH_DEPTH,
    INACCURACY_THRESHOLD,
    MISTAKE_THRESHOLD,
    BLUNDER_THRESHOLD,
    MISS_THRESHOLD
)
from utils import ts

STOCKFISH_PATH = "/usr/local/bin/stockfish"
NUM_WORKERS = 16  # parallel games to analyze simultaneously

def classify(centipawn_loss: int) -> str:
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

def analyze_single_game(game_dict: dict) -> dict:
    """
    Analyzes one game in its own process with its own Stockfish instance.
    Returns dict with game_id, blunders, and success status.
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    opening_eco  = game_dict.get("opening_eco", "")
    moves        = game_dict["moves"]

    if isinstance(moves, str):
        moves = json.loads(moves)

    if not moves:
        return {"game_id": game_id, "blunders": [], "success": True}

    try:
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
        engine.configure({"Threads": 1})  # one thread per worker

        board    = chess.Board()
        blunders = []

        for ply, san in enumerate(moves):
            try:
                move = board.parse_san(san)
            except Exception:
                break

            info_before  = engine.analyse(board, chess.engine.Limit(depth=STOCKFISH_DEPTH))
            score_before = info_before["score"].white().score(mate_score=10000)
            best_move    = info_before.get("pv", [None])[0]
            best_move_san = board.san(best_move) if best_move else None

            board.push(move)

            info_after  = engine.analyse(board, chess.engine.Limit(depth=STOCKFISH_DEPTH))
            score_after = info_after["score"].white().score(mate_score=10000)

            if score_before is None or score_after is None:
                continue

            if player_color == "white":
                cp_loss = score_before - score_after
            else:
                cp_loss = score_after - score_before

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

            blunders.append({
                "ply":            ply,
                "phase":          phase,
                "fen":            fen,
                "move_played":    san,
                "best_move":      best_move_san,
                "centipawn_loss": max(0, cp_loss),
                "classification": classification,
                "opening_eco":    opening_eco,
            })

        engine.quit()
        return {"game_id": game_id, "blunders": blunders, "success": True}

    except Exception as e:
        return {"game_id": game_id, "blunders": [], "success": False, "error": str(e)}

def save_results(results: list):
    """Save blunders and mark games analyzed in one DB connection."""
    conn = get_conn()
    with conn.cursor() as cur:
        for result in results:
            if not result["success"]:
                continue

            game_id  = result["game_id"]
            blunders = result["blunders"]

            if blunders:
                cur.executemany("""
                    INSERT INTO blunders
                        (game_id, ply, phase, fen, move_played, best_move,
                         centipawn_loss, classification, opening_eco)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, ply) DO NOTHING
                """, [
                    (
                        game_id,
                        b["ply"],
                        b["phase"],
                        b["fen"],
                        b["move_played"],
                        b["best_move"],
                        b["centipawn_loss"],
                        b["classification"],
                        b["opening_eco"],
                    )
                    for b in blunders
                ])

            cur.execute(
                "UPDATE games SET stockfish_analyzed = TRUE WHERE id = %s",
                (game_id,)
            )

    conn.commit()
    conn.close()

def main():
    print(f"[{ts()}] Parallel Stockfish Analysis")
    print(f"[{ts()}] Workers: {NUM_WORKERS} | Depth: {STOCKFISH_DEPTH}")

    conn = get_conn()
    players = get_all_active_players(conn)
    print(f"[{ts()}] Found {len(players)} active players.")

    for player in players:
        print(f"\n[{ts()}] Processing {player['user_display_name']}...")
        games = get_unanalyzed_games_for_player(conn, player["id"])
        game_dicts = [dict(g) for g in games]
        print(f"[{ts()}] Found {len(game_dicts)} unanalyzed games.")
        print(f"[{ts()}] Sample game id: {game_dicts[0]['id'] if game_dicts else 'none'}")
        print(f"[{ts()}] Sample moves type: {type(game_dicts[0]['moves']) if game_dicts else 'none'}")

        if not game_dicts:
            print(f"[{ts()}] Nothing to do.")
            continue

        total      = len(game_dicts)
        done       = 0
        start_time = time.time()

        with Pool(processes=NUM_WORKERS) as pool:
            for result in pool.imap_unordered(analyze_single_game, game_dicts):
                done += 1

                if result["success"]:
                    save_results([result])
                    issues = len(result["blunders"])
                else:
                    issues = 0
                    print(f"[{ts()}] Game {result['game_id']} failed: {result.get('error')}")

                elapsed   = time.time() - start_time
                rate      = done / elapsed * 60
                remaining = (total - done) / rate / 60 if rate > 0 else 0

                print(
                    f"[{ts()}] {done}/{total} games | "
                    f"{issues} issues this game | "
                    f"~{remaining:.1f} hrs remaining"
                )

        print(f"[{ts()}] Done {player['user_display_name']}! Total time: {(time.time()-start_time)/3600:.1f} hours")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT classification, COUNT(*)
            FROM blunders
            GROUP BY classification
            ORDER BY COUNT(*) DESC
        """)
        breakdown = cur.fetchall()
    conn.close()

    print(f"\n[{ts()}] Blunder breakdown:")
    for row in breakdown:
        print(f"  {row['classification']}: {row['count']}")

if __name__ == "__main__":
    main()