import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, get_unanalyzed_games_for_player, get_app_settings, log_pipeline_run, cancel_stale_gh_runs
from config import STOCKFISH_VERSION
from utils import ts
from pipeline.analysis_core import analyze_game_full


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


def mark_analyzed(conn, game_id: int, settings: dict, peak_advantage: int | None = None,
                  final_eval: int | None = None):
    depth = settings["stockfish_depth"]
    with conn.cursor() as cur:
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


def main():
    import chess.engine

    stockfish_path = find_stockfish()

    conn     = get_conn()
    settings = get_app_settings(conn)

    cancel_stale_gh_runs(conn)
    run_id = log_pipeline_run(conn, status="running", script_name="analyze_blunders")
    print(f"[{ts()}] Pipeline run {run_id} started (analyze_blunders).")
    print(f"[{ts()}] Using Stockfish at: {stockfish_path}")
    print(f"[{ts()}] Analysis depth:     {settings['stockfish_depth']}")
    print(f"[{ts()}] Thresholds:         inaccuracy={settings['inaccuracy_threshold']} "
          f"mistake={settings['mistake_threshold']} "
          f"blunder={settings['blunder_threshold']} "
          f"miss={settings['miss_threshold']}")

    players = get_all_active_players(conn)
    print(f"[{ts()}] Found {len(players)} active players.")

    import chess
    engine = chess.engine.SimpleEngine.popen_uci(stockfish_path)
    engine.configure({"Threads": os.cpu_count()})

    total_analyzed = 0
    try:
        for player in players:
            print(f"\n[{ts()}] Analyzing games for {player['user_display_name']}...")
            games = get_unanalyzed_games_for_player(conn, player["id"])
            print(f"[{ts()}] Found {len(games)} unanalyzed games.")

            if not games:
                print(f"[{ts()}] Nothing to do.")
                continue

            failed = 0
            total_blunders = 0
            for i, game in enumerate(games):
                try:
                    blunders, peak_advantage, final_eval = analyze_game_full(
                        engine, game, game["player_color"], settings,
                        depth=settings["stockfish_depth"]
                    )
                    # Reconnect before writing — Supabase drops idle connections
                    # during long Stockfish analysis runs
                    conn.close()
                    conn = get_conn()
                    insert_blunders(conn, game["id"], blunders, settings)
                    mark_analyzed(conn, game["id"], settings, peak_advantage=peak_advantage,
                                  final_eval=final_eval)
                    total_blunders += len(blunders)
                    total_analyzed += 1
                    print(f"[{ts()}] Game {i+1}/{len(games)}: {len(blunders)} issues found")
                except Exception as e:
                    failed += 1
                    print(f"[{ts()}] Game {game['id']} failed: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_conn()
                    continue

            if failed:
                print(f"WARNING: {failed}/{len(games)} games failed analysis for "
                      f"{player['user_display_name']} — check logs above for game IDs and errors")
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

        log_pipeline_run(conn, status="completed", games_analyzed=total_analyzed, run_id=run_id)
    except Exception as e:
        print(f"[{ts()}] Pipeline run {run_id} failed: {e}")
        try:
            log_pipeline_run(conn, status="failed", error_message=str(e)[:500], run_id=run_id)
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
