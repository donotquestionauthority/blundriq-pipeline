import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import chess
import chess.engine
import json
import shutil
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, get_unanalyzed_games_for_player, get_app_settings
from config import STOCKFISH_VERSION
from utils import ts


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
    path = shutil.which("stockfish")
    if path:
        return path
    raise FileNotFoundError(
        "Stockfish not found. Install with: brew install stockfish"
    )


def classify(cp_loss: int, eval_before_white: int, player_color: str,
             settings: dict) -> str | None:
    """
    Classify a move error by centipawn loss.

    Miss requires the position to have been contested (within miss_contested_gate
    from the player's perspective) before the error. This prevents mate-score
    contamination from already-decided games inflating the miss count.
    If the contested gate is not met, a miss-threshold loss falls through to
    blunder/mistake/inaccuracy instead of being dropped entirely.
    """
    player_eval = eval_before_white if player_color == "white" else -eval_before_white

    if cp_loss >= settings["miss_threshold"]:
        if abs(player_eval) <= settings["miss_contested_gate"]:
            return "miss"
        # Position was already decided — reclassify downward rather than drop
        # Fall through to blunder/mistake/inaccuracy checks below

    if cp_loss >= settings["blunder_threshold"]:
        return "blunder"
    if cp_loss >= settings["mistake_threshold"]:
        return "mistake"
    if cp_loss >= settings["inaccuracy_threshold"]:
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


def capture_pv_san(board: chess.Board, pv_moves: list, n: int = 5) -> str | None:
    """
    Convert the first N moves of a Stockfish PV into a SAN string.
    Returns None if the PV is empty.
    Used to populate best_line for AI explanations.
    """
    san_list = []
    b = board.copy()
    for move in pv_moves[:n]:
        try:
            san_list.append(b.san(move))
            b.push(move)
        except Exception:
            break
    return " ".join(san_list) if san_list else None


def analyze_game(engine, game: dict, player_color: str, settings: dict) -> list:
    moves = game["moves"]
    if isinstance(moves, str):
        moves = json.loads(moves)

    if not moves:
        return []

    depth  = settings["stockfish_depth"]
    board  = chess.Board()
    blunders = []

    for ply, san in enumerate(moves):
        try:
            move = board.parse_san(san)
        except Exception:
            break

        info_before      = engine.analyse(board, chess.engine.Limit(depth=depth))
        score_before     = info_before["score"].white().score(mate_score=10000)
        pv               = info_before.get("pv", [])
        best_move_obj    = pv[0] if pv else None
        best_move_san    = board.san(best_move_obj) if best_move_obj else None
        best_line        = capture_pv_san(board, pv, n=5)

        board.push(move)

        info_after  = engine.analyse(board, chess.engine.Limit(depth=depth))
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

        # If the move played was already the best move, it's not a blunder —
        # the cp_loss reflects a bad position, not a bad move.
        if best_move_san and san == best_move_san:
            continue

        classification = classify(max(0, cp_loss), score_before, player_color, settings)
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
            "best_line":      best_line,
            "centipawn_loss": max(0, cp_loss),
            "classification": classification,
            "opening_eco":    game.get("opening_eco", ""),
        })

    return blunders


def insert_blunders(conn, game_id: int, blunders: list, settings: dict):
    depth = settings["stockfish_depth"]
    with conn.cursor() as cur:
        # Delete existing blunders first — ensures stale rows from prior analyses
        # or threshold changes are never left behind
        cur.execute("DELETE FROM blunders WHERE game_id = %s", (game_id,))
        if not blunders:
            return
        cur.executemany("""
            INSERT INTO blunders
                (game_id, ply, phase, fen, move_played, best_move, best_line,
                 centipawn_loss, classification, opening_eco,
                 engine_version, analysis_depth)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                game_id,
                b["ply"],
                b["phase"],
                b["fen"],
                b["move_played"],
                b["best_move"],
                b["best_line"],
                b["centipawn_loss"],
                b["classification"],
                b["opening_eco"],
                STOCKFISH_VERSION,
                depth,
            )
            for b in blunders
        ])
    conn.commit()


def mark_analyzed(conn, game_id: int, settings: dict):
    depth = settings["stockfish_depth"]
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE games
            SET stockfish_analyzed = TRUE,
                analysis_engine    = %s,
                analysis_depth     = %s
            WHERE id = %s
        """, (STOCKFISH_VERSION, depth, game_id))
    conn.commit()


def main():
    stockfish_path = find_stockfish()

    conn     = get_conn()
    settings = get_app_settings(conn)

    print(f"[{ts()}] Using Stockfish at: {stockfish_path}")
    print(f"[{ts()}] Analysis depth:     {settings['stockfish_depth']}")
    print(f"[{ts()}] Thresholds:         inaccuracy={settings['inaccuracy_threshold']} "
          f"mistake={settings['mistake_threshold']} "
          f"blunder={settings['blunder_threshold']} "
          f"miss={settings['miss_threshold']}")

    players = get_all_active_players(conn)
    print(f"[{ts()}] Found {len(players)} active players.")

    engine = chess.engine.SimpleEngine.popen_uci(stockfish_path)
    engine.configure({"Threads": os.cpu_count()})

    for player in players:
        print(f"\n[{ts()}] Analyzing games for {player['user_display_name']}...")
        games = get_unanalyzed_games_for_player(conn, player["id"])
        print(f"[{ts()}] Found {len(games)} unanalyzed games.")

        if not games:
            print(f"[{ts()}] Nothing to do.")
            continue

        total_blunders = 0
        for i, game in enumerate(games):
            try:
                blunders = analyze_game(engine, game, game["player_color"], settings)
                # Reconnect before writing — Supabase drops idle connections
                # during long Stockfish analysis runs
                conn.close()
                conn = get_conn()
                insert_blunders(conn, game["id"], blunders, settings)
                mark_analyzed(conn, game["id"], settings)
                total_blunders += len(blunders)
                print(f"[{ts()}] Game {i+1}/{len(games)}: {len(blunders)} issues found")
            except Exception as e:
                print(f"[{ts()}] Game {game['id']} failed: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_conn()
                continue

        print(f"[{ts()}] Total issues for {player['user_display_name']}: {total_blunders}")

    engine.quit()

    conn.close()
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT classification, COUNT(*)
            FROM blunders
            GROUP BY classification
            ORDER BY COUNT(*) DESC
        """)
        breakdown = cur.fetchall()

    print(f"\n[{ts()}] Overall blunder breakdown:")
    for row in breakdown:
        print(f"  {row['classification']}: {row['count']}")

    conn.close()


if __name__ == "__main__":
    main()
