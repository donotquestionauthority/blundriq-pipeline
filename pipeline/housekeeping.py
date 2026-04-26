import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_analysis_game_limit
from utils import ts


def cleanup_expired_refresh_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM refresh_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def cleanup_expired_password_reset_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM password_reset_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def cleanup_expired_email_verification_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM email_verification_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def get_active_players(conn) -> list:
    """Return all players with active accounts for per-player housekeeping."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, u.display_name AS user_display_name
            FROM players p
            JOIN users u ON u.id = p.user_id
            WHERE p.active = TRUE
              AND u.active = TRUE
        """)
        return cur.fetchall()


def cleanup_analysis_beyond_limit(conn, player_id: int, limit: int) -> tuple[int, int]:
    """
    Delete blunders and nullify analysis metadata for games ranked beyond
    the analysis window for this player.

    Returns (games_cleaned, blunders_deleted).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM games
            WHERE player_id = %s
              AND id NOT IN (
                  SELECT id FROM games
                  WHERE player_id = %s
                  ORDER BY played_at DESC
                  LIMIT %s
              )
              AND (analysis_engine IS NOT NULL
                   OR analysis_depth IS NOT NULL
                   OR stockfish_analyzed = TRUE)
        """, (player_id, player_id, limit))
        aged_out_ids = [row["id"] for row in cur.fetchall()]

    if not aged_out_ids:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute("DELETE FROM blunders WHERE game_id = ANY(%s)", (aged_out_ids,))
        blunders_deleted = cur.rowcount

        cur.execute("""
            UPDATE games
            SET analysis_engine    = NULL,
                analysis_depth     = NULL,
                stockfish_analyzed = FALSE
            WHERE id = ANY(%s)
        """, (aged_out_ids,))
        games_cleaned = cur.rowcount

    conn.commit()
    return games_cleaned, blunders_deleted


def cleanup_repertoire_beyond_limit(conn, player_id: int, limit: int) -> tuple[int, int]:
    """
    Delete game_result_lines and game_repertoire_results for games ranked
    beyond the analysis window for this player.

    Repertoire match data beyond the window is never surfaced (deviations
    page is scoped to the analysis window) so keeping it is pure waste.

    Returns (results_deleted, lines_deleted).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT grr.id
            FROM game_repertoire_results grr
            JOIN games g ON g.id = grr.game_id
            WHERE g.player_id = %s
              AND g.id NOT IN (
                  SELECT id FROM games
                  WHERE player_id = %s
                  ORDER BY played_at DESC
                  LIMIT %s
              )
        """, (player_id, player_id, limit))
        aged_out_result_ids = [row["id"] for row in cur.fetchall()]

    if not aged_out_result_ids:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM game_result_lines
            WHERE game_repertoire_result_id = ANY(%s)
        """, (aged_out_result_ids,))
        lines_deleted = cur.rowcount

        cur.execute("""
            DELETE FROM game_repertoire_results
            WHERE id = ANY(%s)
        """, (aged_out_result_ids,))
        results_deleted = cur.rowcount

    conn.commit()
    return results_deleted, lines_deleted


def cleanup_opponent_fens_beyond_limit(conn, player_id: int, limit: int) -> int:
    """
    Delete opponent_game_fens rows for opponent games ranked beyond the
    analysis window for this player's opponents.

    Returns number of fen rows deleted.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM opponent_game_fens
            WHERE opponent_game_id IN (
                SELECT og.id
                FROM opponent_games og
                JOIN opponent_profiles op ON op.id = og.opponent_profile_id
                WHERE op.player_id = %s
                  AND og.id NOT IN (
                      SELECT id FROM (
                          SELECT id,
                                 ROW_NUMBER() OVER (
                                     PARTITION BY opponent_profile_id
                                     ORDER BY played_at DESC
                                 ) AS rn
                          FROM opponent_games
                          WHERE opponent_profile_id = op.id
                      ) ranked
                      WHERE rn <= %s
                  )
            )
        """, (player_id, limit))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def run_housekeeping(conn):
    print(f"[{ts()}] Running housekeeping...")

    n = cleanup_expired_refresh_tokens(conn)
    print(f"[{ts()}]   Refresh tokens:              {n} expired row(s) removed")

    n = cleanup_expired_password_reset_tokens(conn)
    print(f"[{ts()}]   Password reset tokens:       {n} expired row(s) removed")

    n = cleanup_expired_email_verification_tokens(conn)
    print(f"[{ts()}]   Email verification tokens:   {n} expired row(s) removed")

    players = get_active_players(conn)
    total_games_cleaned   = 0
    total_blunders        = 0
    total_results         = 0
    total_lines           = 0
    total_opponent_fens   = 0

    for player in players:
        pid   = player["id"]
        name  = player["user_display_name"]
        limit = get_analysis_game_limit(conn, pid)

        games_cleaned, blunders_deleted = cleanup_analysis_beyond_limit(conn, pid, limit)
        results_deleted, lines_deleted  = cleanup_repertoire_beyond_limit(conn, pid, limit)
        opponent_fens_deleted           = cleanup_opponent_fens_beyond_limit(conn, pid, limit)

        if any([games_cleaned, blunders_deleted, results_deleted,
                lines_deleted, opponent_fens_deleted]):
            print(f"[{ts()}]   {name} (limit={limit}): "
                  f"{games_cleaned} game(s) aged out, "
                  f"{blunders_deleted} blunder(s), "
                  f"{results_deleted} repertoire result(s), "
                  f"{lines_deleted} line row(s), "
                  f"{opponent_fens_deleted} opponent FEN(s) deleted")

        total_games_cleaned += games_cleaned
        total_blunders      += blunders_deleted
        total_results       += results_deleted
        total_lines         += lines_deleted
        total_opponent_fens += opponent_fens_deleted

    print(f"[{ts()}]   Analysis cleanup totals:     "
          f"{total_games_cleaned} game(s), "
          f"{total_blunders} blunder(s), "
          f"{total_results} repertoire result(s), "
          f"{total_lines} line row(s), "
          f"{total_opponent_fens} opponent FEN(s)")

    print(f"[{ts()}] Housekeeping complete.")


if __name__ == "__main__":
    conn = get_conn()
    run_housekeeping(conn)
    conn.close()
