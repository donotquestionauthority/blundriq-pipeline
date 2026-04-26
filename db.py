import os
import psycopg2
from psycopg2.extras import RealDictCursor
from config import (
    DATABASE_URL,
    ANALYSIS_GAME_LIMIT,
    FREE_IMPORT_LIMIT,
    FAST_PASS_DEPTH,
    STOCKFISH_DEPTH,
    STOCKFISH_VERSION,
    INACCURACY_THRESHOLD,
    MISTAKE_THRESHOLD,
    BLUNDER_THRESHOLD,
    MISS_THRESHOLD,
    MISS_CONTESTED_GATE,
    MAX_CP_DISPLAY,
    LOST_WINS_PEAK_THRESHOLD,
    LOST_WINS_SUSTAINED_MOVES,
)


def get_conn():
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL environment variable must be set."
        )
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# ─── App settings ─────────────────────────────────────────────────────────────

# Canonical defaults — used when app_settings row is absent or unparseable.
# Keep in sync with pipeline/config.py and the migration seed values.
_SETTINGS_DEFAULTS = {
    "analysis_game_limit":       ANALYSIS_GAME_LIMIT,
    "free_import_limit":         FREE_IMPORT_LIMIT,
    "fast_pass_depth":           FAST_PASS_DEPTH,
    "stockfish_depth":           STOCKFISH_DEPTH,
    "inaccuracy_threshold":      INACCURACY_THRESHOLD,
    "mistake_threshold":         MISTAKE_THRESHOLD,
    "blunder_threshold":         BLUNDER_THRESHOLD,
    "miss_threshold":            MISS_THRESHOLD,
    "miss_contested_gate":       MISS_CONTESTED_GATE,
    "max_cp_display":            MAX_CP_DISPLAY,
    "lost_wins_peak_threshold":  LOST_WINS_PEAK_THRESHOLD,
    "lost_wins_sustained_moves": LOST_WINS_SUSTAINED_MOVES,
}


def get_app_settings(conn) -> dict:
    """
    Fetch all configurable pipeline settings from app_settings in one query.
    Falls back to config.py constants for any key not present or unparseable.

    Returns a plain dict of int values, e.g.:
        {
            "analysis_game_limit":  1000,
            "free_import_limit":    500,
            "fast_pass_depth":      12,
            "stockfish_depth":      18,
            "inaccuracy_threshold": 50,
            "mistake_threshold":    100,
            "blunder_threshold":    200,
            "miss_threshold":       300,
            "miss_contested_gate":  300,
            "max_cp_display":       500,
        }
    """
    keys = list(_SETTINGS_DEFAULTS.keys())
    with conn.cursor() as cur:
        cur.execute(
            "SELECT key, value FROM app_settings WHERE key = ANY(%s)",
            (keys,)
        )
        rows = {row["key"]: row["value"] for row in cur.fetchall()}

    result = {}
    for key, default in _SETTINGS_DEFAULTS.items():
        raw = rows.get(key)
        if raw is not None:
            try:
                result[key] = int(raw)
            except (TypeError, ValueError):
                result[key] = default
        else:
            result[key] = default

    # stockfish_version is not int — pass through from config, not app_settings
    result["stockfish_version"] = STOCKFISH_VERSION

    return result


def get_analysis_game_limit(conn, player_id: int) -> int:
    """
    Resolve the effective analysis game limit for a player.

    Resolution order:
      1. player_settings.analysis_game_limit  (per-player override, if set)
      2. app_settings 'analysis_game_limit'   (global default)
      3. ANALYSIS_GAME_LIMIT constant         (hardcoded fallback)
    """
    with conn.cursor() as cur:
        # Per-player override
        cur.execute("""
            SELECT analysis_game_limit
            FROM player_settings
            WHERE player_id = %s
        """, (player_id,))
        row = cur.fetchone()
        if row and row["analysis_game_limit"] is not None:
            return int(row["analysis_game_limit"])

        # Global app_settings default
        cur.execute(
            "SELECT value FROM app_settings WHERE key = 'analysis_game_limit'"
        )
        row = cur.fetchone()
        if row:
            try:
                return int(row["value"])
            except (TypeError, ValueError):
                pass

    return ANALYSIS_GAME_LIMIT


# ─── Players ──────────────────────────────────────────────────────────────────

def get_all_active_players(conn):
    """
    Returns players eligible for the hourly pipeline:
    - paid subscribers with fast pass complete AND is_initialized (incremental import + analysis)
    - excludes players mid-onboarding (is_initialized=FALSE) — they are owned by Fargate/Dell
    Excludes free/trial users and cancelled subscribers.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, u.email, u.display_name as user_display_name,
                   u.is_paid
            FROM players p
            JOIN users u ON u.id = p.user_id
            WHERE p.active = TRUE
              AND u.active = TRUE
              AND u.registration_approved = TRUE
              AND u.is_paid = TRUE
              AND p.fast_pass_complete = TRUE
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
    """Returns unanalyzed games within the analysis window for a player, newest first."""
    limit = get_analysis_game_limit(conn, player_id)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.*
            FROM games g
            WHERE g.player_id = %s
              AND g.stockfish_analyzed = FALSE
              AND g.id IN (
                  SELECT id FROM games
                  WHERE player_id = %s
                  ORDER BY played_at DESC
                  LIMIT %s
              )
            ORDER BY g.played_at DESC
        """, (player_id, player_id, limit))
        return cur.fetchall()


# ─── Pipeline runs ────────────────────────────────────────────────────────────

def cancel_stale_gh_runs(conn):
    """
    Mark any GH Actions pipeline_runs rows still in 'running' state as 'cancelled'.
    Called at the start of each GH Actions script to clean up rows left behind
    by previously cancelled or killed workflow runs.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET    status      = 'cancelled',
                   finished_at = NOW()
            WHERE  status     = 'running'
              AND  player_id IS NULL
        """)
        count = cur.rowcount
    conn.commit()
    if count:
        print(f"[pipeline] Cleaned up {count} stale GH Actions run(s).")


def log_pipeline_run(conn, status, player_id=None, games_imported=0,
                     games_matched=0, games_analyzed=0,
                     error_message=None, run_id=None):
    with conn.cursor() as cur:
        if run_id is None:
            cur.execute("""
                INSERT INTO pipeline_runs (status, player_id)
                VALUES (%s, %s)
                RETURNING id
            """, (status, player_id))
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
