import os
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DATABASE_URL


def get_conn():
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL environment variable must be set."
        )
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def get_all_active_players(conn):
    """Returns all active, initialized player records."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, u.email, u.display_name as user_display_name
            FROM players p
            JOIN users u ON u.id = p.user_id
            WHERE p.active = TRUE
              AND u.active = TRUE
              AND p.is_initialized = TRUE
        """)
        return cur.fetchall()


def get_active_lines_for_player(conn, player_id: int):
    """Returns active repertoire lines for a specific player."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                rl.id           AS line_id,
                rl.line_name,
                rl.moves,
                rl.fen_sequence,
                rl.is_alternative,
                ch.id           AS chapter_id,
                ch.title        AS chapter_title,
                bk.id           AS book_id,
                bk.title        AS book_title,
                bk.color        AS color
            FROM repertoire_lines rl
            JOIN chapters ch ON ch.id = rl.chapter_id
            JOIN books    bk ON bk.id = ch.book_id
            WHERE rl.active  = TRUE
              AND ch.active  = TRUE
              AND bk.active  = TRUE
              AND bk.player_id = %s
        """, (player_id,))
        return cur.fetchall()


def get_unanalyzed_games_for_player(conn, player_id: int):
    """Returns unanalyzed games for a specific player, newest first."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.*
            FROM   games g
            WHERE  g.player_id = %s
              AND  g.stockfish_analyzed = FALSE
            ORDER BY g.played_at DESC
        """, (player_id,))
        return cur.fetchall()


def log_pipeline_run(conn, status, games_imported=0,
                     games_matched=0, games_analyzed=0,
                     error_message=None, run_id=None):
    with conn.cursor() as cur:
        if run_id is None:
            cur.execute("""
                INSERT INTO pipeline_runs (status)
                VALUES (%s)
                RETURNING id
            """, (status,))
            run_id = cur.fetchone()["id"]
        else:
            cur.execute("""
                UPDATE pipeline_runs
                SET    finished_at    = NOW(),
                       status         = %s,
                       games_imported = %s,
                       games_matched  = %s,
                       games_analyzed = %s,
                       error_message  = %s
                WHERE  id = %s
            """, (status, games_imported, games_matched,
                  games_analyzed, error_message, run_id))
        conn.commit()
    return run_id