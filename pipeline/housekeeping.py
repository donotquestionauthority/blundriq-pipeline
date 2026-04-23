import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from config import ANALYSIS_GAME_LIMIT
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


def cleanup_analysis_beyond_limit(conn) -> tuple[int, int]:
    """
    For each active player, delete blunders rows and nullify analysis metadata
    on games ranked beyond ANALYSIS_GAME_LIMIT by recency.

    Returns (games_cleaned, blunders_deleted).
    """
    with conn.cursor() as cur:
        # Find all game IDs outside the active window across all players
        cur.execute("""
            SELECT id FROM games
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY player_id
                               ORDER BY played_at DESC
                           ) AS rn
                    FROM games
                    WHERE moves IS NOT NULL
                ) ranked
                WHERE rn <= %s
            )
            AND (analysis_engine IS NOT NULL OR analysis_depth IS NOT NULL)
        """, (ANALYSIS_GAME_LIMIT,))
        aged_out_ids = [row["id"] for row in cur.fetchall()]

    if not aged_out_ids:
        return 0, 0

    with conn.cursor() as cur:
        # Delete blunders for aged-out games
        cur.execute("""
            DELETE FROM blunders WHERE game_id = ANY(%s)
        """, (aged_out_ids,))
        blunders_deleted = cur.rowcount

        # Nullify analysis metadata — signals these games need reanalysis
        # if they ever re-enter the active window (e.g. limit increase)
        cur.execute("""
            UPDATE games
            SET analysis_engine = NULL,
                analysis_depth  = NULL,
                stockfish_analyzed = FALSE
            WHERE id = ANY(%s)
        """, (aged_out_ids,))
        games_cleaned = cur.rowcount

    conn.commit()
    return games_cleaned, blunders_deleted


def cleanup_opponent_fens_beyond_limit(conn) -> int:
    """
    For each opponent profile, delete opponent_game_fens rows for games
    ranked beyond ANALYSIS_GAME_LIMIT by recency.

    opponent_game_fens drives the Scout feature — keeping only the active
    window prevents unbounded growth while preserving scout accuracy.

    Returns number of fen rows deleted.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM opponent_game_fens
            WHERE opponent_game_id IN (
                SELECT og.id
                FROM opponent_games og
                WHERE og.id NOT IN (
                    SELECT id FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY opponent_profile_id
                                   ORDER BY played_at DESC
                               ) AS rn
                        FROM opponent_games
                    ) ranked
                    WHERE rn <= %s
                )
            )
        """, (ANALYSIS_GAME_LIMIT,))
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

    games_cleaned, blunders_deleted = cleanup_analysis_beyond_limit(conn)
    print(f"[{ts()}]   Analysis cleanup:            {games_cleaned} game(s) aged out, "
          f"{blunders_deleted} blunder row(s) deleted")

    n = cleanup_opponent_fens_beyond_limit(conn)
    print(f"[{ts()}]   Opponent FEN cleanup:        {n} row(s) deleted")

    print(f"[{ts()}] Housekeeping complete.")


if __name__ == "__main__":
    conn = get_conn()
    run_housekeeping(conn)
    conn.close()
