import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_all_active_players, get_active_lines_for_player, get_analysis_game_limit, log_pipeline_run, cancel_stale_gh_runs
from pipeline.matching import compute_matches, insert_results
from utils import ts

def get_unmatched_games(conn, player_id: int, limit: int) -> list:
    """
    Returns unmatched games within the analysis window only.
    No point matching games outside the window — they will never be analyzed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.id, g.moves, g.fen_sequence, g.player_color, g.opening_eco
            FROM   games g
            LEFT JOIN game_repertoire_results grr ON grr.game_id = g.id
            WHERE  g.player_id = %s
            AND    grr.id IS NULL
            AND    g.no_repertoire_match = FALSE
            AND    g.id IN (
                SELECT id FROM games
                WHERE player_id = %s
                ORDER BY played_at DESC
                LIMIT %s
            )
            ORDER BY g.played_at ASC
        """, (player_id, player_id, limit))
        return cur.fetchall()

def mark_no_match(conn, game_ids: list):
    if not game_ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE games
            SET no_repertoire_match = TRUE
            WHERE id = ANY(%s)
        """, (game_ids,))
    conn.commit()

def main():
    conn = get_conn()
    cancel_stale_gh_runs(conn)
    run_id = log_pipeline_run(conn, status="running", script_name="match_repertoire")
    print(f"[{ts()}] Pipeline run {run_id} started (match_repertoire).")

    try:
        players = get_all_active_players(conn)
        print(f"[{ts()}] Found {len(players)} active players.")
        total_matched = 0

        for player in players:
            print(f"\n[{ts()}] Processing {player['user_display_name']}...")

            t0 = time.time()
            limit = get_analysis_game_limit(conn, player["id"])
            active_lines = get_active_lines_for_player(conn, player["id"])
            print(f"[{ts()}] Loaded {len(active_lines)} active lines in {time.time()-t0:.2f}s")

            unmatched = get_unmatched_games(conn, player["id"], limit)
            print(f"[{ts()}] Found {len(unmatched)} unmatched games (within {limit}-game window).")

            if not unmatched:
                print(f"[{ts()}] Nothing to do.")
                continue

            print(f"[{ts()}] Computing matches...")
            result_rows, lines_by_game_id = compute_matches(unmatched, active_lines)
            print(f"[{ts()}] Computed {len(result_rows)} matches.")

            matched_game_ids = set(row[0] for row in result_rows)
            no_match_ids = [g["id"] for g in unmatched if g["id"] not in matched_game_ids]
            if no_match_ids:
                mark_no_match(conn, no_match_ids)
                print(f"[{ts()}] Marked {len(no_match_ids)} games as no repertoire match.")

            print(f"[{ts()}] Writing to database...")
            insert_results(conn, result_rows, lines_by_game_id)
            total_matched += len(result_rows)

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
            print(f"[{ts()}] Time: {time.time()-t0:.2f}s")

        log_pipeline_run(conn, status="completed", games_matched=total_matched, run_id=run_id)
    except Exception as e:
        print(f"[{ts()}] Pipeline run {run_id} failed: {e}")
        try:
            log_pipeline_run(conn, status="failed", error_message=str(e)[:500], run_id=run_id)
        except Exception:
            pass
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
