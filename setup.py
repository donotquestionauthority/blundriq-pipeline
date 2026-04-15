import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_active_lines_for_player
from pipeline.matching import compute_matches, insert_results
from pipeline.import_chesscom import import_chesscom_games
from pipeline.import_lichess import import_lichess_games
from utils import ts

def main():
    print("=" * 50)
    print(f"[{ts()}] INITIATIVE - Initial Setup")
    print("=" * 50)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, u.email, u.display_name as user_display_name
            FROM players p
            JOIN users u ON u.id = p.user_id
            WHERE p.active = TRUE
              AND u.active = TRUE
              AND p.is_initialized = FALSE
        """)
        players = cur.fetchall()
    print(f"[{ts()}] Found {len(players)} uninitialized players.")

    if not players:
        conn.close()
        return

    for player in players:
        print(f"\n[{ts()}] Setting up {player['user_display_name']}...")

        # ── Step 1: Import games ──────────────────────────────────────────
        print(f"[{ts()}] Step 1: Importing games...")
        chesscom_count = import_chesscom_games(conn, player)
        lichess_count  = import_lichess_games(conn, player)
        print(f"[{ts()}] Imported {chesscom_count} Chess.com + {lichess_count} Lichess games.")

        # ── Step 2: Match repertoire ──────────────────────────────────────
        print(f"[{ts()}] Step 2: Matching against repertoire...")
        t0 = time.time()
        active_lines = get_active_lines_for_player(conn, player["id"])
        print(f"[{ts()}] Loaded {len(active_lines)} active lines.")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT g.id, g.moves, g.fen_sequence, g.player_color, g.opening_eco
                FROM   games g
                LEFT JOIN game_repertoire_results grr ON grr.game_id = g.id
                WHERE  g.player_id = %s
                  AND  grr.id IS NULL
                ORDER BY g.played_at ASC
            """, (player["id"],))
            unmatched = cur.fetchall()

        print(f"[{ts()}] Found {len(unmatched)} unmatched games.")

        if unmatched:
            result_rows, lines_by_game_id = compute_matches(unmatched, active_lines)
            print(f"[{ts()}] Computed {len(result_rows)} matches in {time.time()-t0:.2f}s")
            insert_results(conn, result_rows, lines_by_game_id)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM game_repertoire_results grr
                JOIN games g ON g.id = grr.game_id
                WHERE g.player_id = %s
            """, (player["id"],))
            total = cur.fetchone()["count"]
            cur.execute("""
                SELECT deviation_by, COUNT(*)
                FROM game_repertoire_results grr
                JOIN games g ON g.id = grr.game_id
                WHERE g.player_id = %s
                GROUP BY deviation_by
            """, (player["id"],))
            breakdown = cur.fetchall()

        print(f"[{ts()}] Total matched games: {total}")
        for row in breakdown:
            print(f"  {row['deviation_by']}: {row['count']}")

        # ── Step 3: Stockfish analysis ────────────────────────────────────
        print(f"[{ts()}] Step 3: Running parallel Stockfish analysis...")
        print(f"[{ts()}] This may take several hours for a new player.")

        try:
            from multiprocessing import Pool
            import json
            import chess
            import chess.engine
            from analyze_parallel import analyze_single_game, save_results, STOCKFISH_PATH, NUM_WORKERS

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT g.*
                    FROM games g
                    WHERE g.player_id = %s
                      AND g.stockfish_analyzed = FALSE
                    ORDER BY g.played_at ASC
                """, (player["id"],))
                games = cur.fetchall()

            game_dicts = [dict(g) for g in games]
            print(f"[{ts()}] Found {len(game_dicts)} games to analyze.")

            if game_dicts:
                total_games = len(game_dicts)
                done = 0
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

                        elapsed = time.time() - start_time
                        rate = done / elapsed * 60 if elapsed > 0 else 0
                        remaining = (total_games - done) / rate / 60 if rate > 0 else 0
                        print(
                            f"[{ts()}] {done}/{total_games} games | "
                            f"{issues} issues | "
                            f"~{remaining:.1f} hrs remaining"
                        )

                print(f"[{ts()}] Stockfish complete. Total time: {(time.time()-start_time)/3600:.1f} hours")

        except Exception as e:
            print(f"[{ts()}] Stockfish analysis failed: {e}")
            print(f"[{ts()}] Player {player['user_display_name']} NOT marked as initialized — will retry on next run.")
            continue

        # Verify all games analyzed before marking initialized
        # Get a fresh connection — original may have timed out during long Stockfish run
        try:
            conn.close()
        except Exception:
            pass
        conn = get_conn()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM games
                WHERE player_id = %s
                  AND stockfish_analyzed = FALSE
            """, (player["id"],))
            remaining = cur.fetchone()["count"]

        if remaining > 0:
            print(f"[{ts()}] {remaining} games still unanalyzed — NOT marking as initialized.")
            print(f"[{ts()}] Will retry on next cron run.")
            continue

        # ── Mark as initialized ───────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE players SET is_initialized = TRUE WHERE id = %s
            """, (player["id"],))
        conn.commit()
        print(f"[{ts()}] Player {player['user_display_name']} marked as initialized.")

    conn.close()
    print(f"\n[{ts()}] Setup complete.")

if __name__ == "__main__":
    main()