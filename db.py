import os
import psycopg2
from psycopg2.extras import RealDictCursor
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY, DATABASE_URL


def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_conn():
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL environment variable must be set."
        )
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def get_user_preference(conn, user_id: int, key: str, default=None):
    """Get a user preference value."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT value FROM user_preferences
            WHERE user_id = %s AND key = %s
        """, (user_id, key))
        row = cur.fetchone()
    if row:
        return row["value"]
    return default

def set_user_preference(conn, user_id: int, key: str, value):
    """Set a user preference value."""
    import json
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO user_preferences (user_id, key, value, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id, key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = NOW()
        """, (user_id, key, json.dumps(value)))
    conn.commit()

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

def get_user_by_email(conn, email: str):
    """Returns user record by email."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE email = %s AND active = TRUE", (email,))
        return cur.fetchone()

def get_player_by_user(conn, user_id: int):
    """Returns player record for a given user."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM players WHERE user_id = %s AND active = TRUE", (user_id,))
        return cur.fetchone()

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
    """Returns unanalyzed games for a specific player."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.*
            FROM   games g
            WHERE  g.player_id = %s
              AND  g.stockfish_analyzed = FALSE
            ORDER BY g.played_at DESC
        """, (player_id,))
        return cur.fetchall()

def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its hash."""
    import bcrypt
    return bcrypt.checkpw(
        password.encode('utf-8'),
        password_hash.encode('utf-8')
    )

def create_user(conn, email: str, password: str, display_name: str) -> int:
    """Create a new user and return their id."""
    import bcrypt
    password_hash = bcrypt.hashpw(
        password.encode('utf-8'),
        bcrypt.gensalt()
    ).decode('utf-8')
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (email, password_hash, display_name)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (email, password_hash, display_name))
        user_id = cur.fetchone()["id"]
    conn.commit()
    return user_id

def generate_registration_code(conn, admin_user_id: int, email: str = None, days_valid: int = 7) -> str:
    """Generate a registration code."""
    import secrets
    from datetime import datetime, timezone, timedelta
    code = secrets.token_urlsafe(16)
    expires_at = datetime.now(timezone.utc) + timedelta(days=days_valid)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO registration_codes 
                (code, email, created_by, expires_at)
            VALUES (%s, %s, %s, %s)
        """, (code, email, admin_user_id, expires_at))
    conn.commit()
    return code

def validate_registration_code(conn, code: str) -> dict:
    """Check if a registration code is valid and unused."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM registration_codes
            WHERE code = %s
              AND used = FALSE
              AND (expires_at IS NULL OR expires_at > NOW())
        """, (code,))
        return cur.fetchone()

def use_registration_code(conn, code: str, user_id: int):
    """Mark a registration code as used."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE registration_codes
            SET used = TRUE, used_by = %s, used_at = NOW()
            WHERE code = %s
        """, (user_id, code))
    conn.commit()

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

# ─── Blunders ────────────────────────────────────────────────────────────────

def get_dismissed_fens(conn, player_id: int) -> set:
    """Return the set of FENs dismissed by a player."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fen FROM dismissed_blunder_fens WHERE player_id = %s",
            (player_id,)
        )
        return {row["fen"] for row in cur.fetchall()}


def dismiss_fen(player_id: int, fen: str):
    """Dismiss a blunder FEN for a player."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO dismissed_blunder_fens (player_id, fen)
               VALUES (%s, %s) ON CONFLICT (player_id, fen) DO NOTHING""",
            (player_id, fen)
        )
    conn.commit()
    conn.close()


def undismiss_fen(player_id: int, fen: str):
    """Remove a FEN dismissal for a player."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dismissed_blunder_fens WHERE player_id = %s AND fen = %s",
            (player_id, fen)
        )
    conn.commit()
    conn.close()


def get_blunder_positions(player_id: int, classifications: list,
                          since=None, phase_filter: str = "All",
                          last_n_games: int = 0) -> list:
    """
    Return one row per FEN ranked by weighted blunder score.
    No moves data — fast aggregation query suitable for ranking/pagination.
    """
    from config import CLASSIFICATION_WEIGHTS

    conn = get_conn()

    weight_case = " ".join(
        f"WHEN b.classification = '{k}' THEN {v}"
        for k, v in CLASSIFICATION_WEIGHTS.items()
    )

    game_filter = ""
    params = {"player_id": player_id, "cls": list(classifications)}

    if last_n_games > 0:
        game_filter = """
          AND g.id IN (
              SELECT id FROM games
              WHERE player_id = %(player_id)s
              ORDER BY played_at DESC
              LIMIT %(last_n)s
          )
        """
        params["last_n"] = last_n_games
    elif since:
        game_filter = " AND g.played_at >= %(since)s"
        params["since"] = since

    phase_clause = ""
    if phase_filter != "All":
        phase_clause = " AND cls_agg.phase = %(phase)s"
        params["phase"] = phase_filter.lower()

    query = f"""
        SELECT
            cls_agg.fen,
            MODE() WITHIN GROUP (ORDER BY cls_agg.phase)        AS phase,
            MODE() WITHIN GROUP (ORDER BY cls_agg.player_color) AS color,
            MODE() WITHIN GROUP (ORDER BY cls_agg.opening_name) AS opening_name,
            SUM(cls_agg.cls_count)                               AS count,
            SUM(cls_agg.cls_score)                               AS score,
            jsonb_object_agg(cls_agg.classification, cls_agg.cls_count) AS classifications
        FROM (
            SELECT
                b.fen,
                b.phase,
                g.player_color,
                g.opening_name,
                b.classification,
                COUNT(*)                              AS cls_count,
                SUM(CASE {weight_case} ELSE 0 END)   AS cls_score
            FROM blunders b
            JOIN games g ON g.id = b.game_id
            WHERE g.player_id = %(player_id)s
              AND b.classification = ANY(%(cls)s)
              {game_filter}
            GROUP BY b.fen, b.phase, g.player_color, g.opening_name, b.classification
        ) cls_agg
        {phase_clause}
        GROUP BY cls_agg.fen
        ORDER BY score DESC
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_blunder_details(player_id: int, fens: list,
                        since=None, phase_filter: str = "All",
                        last_n_games: int = 0) -> list:
    """
    Return per-game blunder detail rows for a specific set of FENs.
    Includes moves, game URLs, repertoire context.
    """
    conn = get_conn()

    params = [player_id, fens]
    game_filter = ""

    if last_n_games > 0:
        game_filter = """
          AND g.id IN (
              SELECT id FROM games
              WHERE player_id = %s
              ORDER BY played_at DESC
              LIMIT %s
          )
        """
        params += [player_id, last_n_games]
    elif since:
        game_filter = " AND g.played_at >= %s"
        params.append(since)

    phase_clause = ""
    if phase_filter != "All":
        phase_clause = " AND b.phase = %s"
        params.append(phase_filter.lower())

    query = f"""
        SELECT
            b.fen               AS fen,
            b.ply               AS ply,
            b.move_played       AS move_played,
            b.best_move         AS best_move,
            b.centipawn_loss    AS cp_loss,
            b.classification    AS classification,
            g.id                AS game_id,
            g.moves             AS moves,
            g.player_color      AS color,
            g.url               AS game_url,
            g.opening_name      AS opening_name,
            bk.title            AS book_title,
            ch.title            AS chapter_title
        FROM blunders b
        JOIN games g ON g.id = b.game_id
        LEFT JOIN game_repertoire_results grr ON grr.game_id = g.id
        LEFT JOIN books bk ON bk.id = grr.book_id
        LEFT JOIN chapters ch ON ch.id = grr.chapter_id
        WHERE g.player_id = %s
          AND b.fen = ANY(%s)
          {game_filter}
          {phase_clause}
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Health ──────────────────────────────────────────────────────────────────

def get_health_stats(conn, player_id: int) -> dict:
    """Return import stats, analysis counts, and repertoire info for the health page."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source, MAX(played_at) as last_game, COUNT(*) as total
            FROM games WHERE player_id = %s GROUP BY source
        """, (player_id,))
        import_stats = {row["source"]: dict(row) for row in cur.fetchall()}

        cur.execute("""
            SELECT COUNT(*) FROM games
            WHERE player_id = %s AND stockfish_analyzed = FALSE
        """, (player_id,))
        unanalyzed = cur.fetchone()["count"]

        cur.execute("""
            SELECT COUNT(*) FROM blunders b
            JOIN games g ON g.id = b.game_id WHERE g.player_id = %s
        """, (player_id,))
        total_blunders = cur.fetchone()["count"]

        cur.execute("""
            SELECT COUNT(*) FROM game_repertoire_results grr
            JOIN games g ON g.id = grr.game_id WHERE g.player_id = %s
        """, (player_id,))
        total_matched = cur.fetchone()["count"]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.title, b.color, b.active,
                   COUNT(DISTINCT ch.id) as chapters,
                   COUNT(rl.id) as lines
            FROM books b
            LEFT JOIN chapters ch ON ch.book_id = b.id
            LEFT JOIN repertoire_lines rl ON rl.chapter_id = ch.id
            WHERE b.player_id = %s
            GROUP BY b.id, b.title, b.color, b.active
            ORDER BY b.active DESC, b.title
        """, (player_id,))
        books = [dict(r) for r in cur.fetchall()]

    return {
        "import_stats":   import_stats,
        "unanalyzed":     unanalyzed,
        "total_blunders": total_blunders,
        "total_matched":  total_matched,
        "books":          books,
    }


# ─── Recent Games ────────────────────────────────────────────────────────────

def get_recent_games(conn, player_id: int, since=None, color: str = "All",
                     result: str = "All", platform: str = "All",
                     book: str = "All", chapter: str = "All",
                     deviation: str = "All") -> list:
    """Return game rows with repertoire join for the game log."""
    query = """
        SELECT
            g.id, g.url, g.source, g.played_at, g.player_color,
            g.opponent_username, g.opponent_rating, g.player_rating,
            g.result, g.time_control, g.opening_name, g.opening_eco,
            grr.deviated_at_ply, grr.deviation_by,
            grr.expected_move, grr.played_move,
            bk.title  AS book_title,
            ch.title  AS chapter_title,
            rl.line_name AS line_name
        FROM games g
        LEFT JOIN game_repertoire_results grr ON grr.game_id = g.id
        LEFT JOIN books bk ON bk.id = grr.book_id
        LEFT JOIN chapters ch ON ch.id = grr.chapter_id
        LEFT JOIN LATERAL (
            SELECT grl.line_id FROM game_result_lines grl
            WHERE grl.game_repertoire_result_id = grr.id
            ORDER BY grl.matched_ply DESC LIMIT 1
        ) best_line ON TRUE
        LEFT JOIN repertoire_lines rl ON rl.id = best_line.line_id
        WHERE g.player_id = %s
    """
    params = [player_id]

    if since:
        query += " AND g.played_at >= %s"; params.append(since)
    if color != "All":
        query += " AND g.player_color = %s"; params.append(color.lower())
    if result != "All":
        query += " AND g.result = %s"; params.append(result.lower())
    if platform != "All":
        source_map = {"Chess.com": "chesscom", "Lichess": "lichess"}
        query += " AND g.source = %s"; params.append(source_map[platform])
    if book != "All":
        query += " AND bk.title = %s"; params.append(book)
    if chapter != "All":
        query += " AND ch.title = %s"; params.append(chapter)
    if deviation != "All":
        dev_map = {"I deviated": "me", "Opponent deviated": "opponent", "Followed line": "none"}
        if deviation == "No match":
            query += " AND grr.id IS NULL"
        else:
            query += " AND grr.deviation_by = %s"; params.append(dev_map[deviation])

    query += " ORDER BY g.played_at DESC"
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def get_opening_stats(conn, player_id: int, since=None) -> dict:
    """Return matched and unmatched game rows for the opening stats tab."""
    total_params = [player_id]
    total_query = "SELECT COUNT(*) as total FROM games g WHERE g.player_id = %s"
    if since:
        total_query += " AND g.played_at >= %s"; total_params.append(since)
    with conn.cursor() as cur:
        cur.execute(total_query, total_params)
        total_games = cur.fetchone()["total"]

    query = """
        SELECT bk.title AS book, bk.color AS book_color, ch.title AS chapter,
               grr.deviation_by, grr.deviated_at_ply, g.result
        FROM game_repertoire_results grr
        JOIN games g   ON g.id   = grr.game_id
        JOIN books bk  ON bk.id  = grr.book_id
        JOIN chapters ch ON ch.id = grr.chapter_id
        WHERE g.player_id = %s
    """
    params = [player_id]
    if since:
        query += " AND g.played_at >= %s"; params.append(since)
    with conn.cursor() as cur:
        cur.execute(query, params)
        matched = [dict(r) for r in cur.fetchall()]

    unmatched_query = """
        SELECT g.opening_name, g.result FROM games g
        LEFT JOIN game_repertoire_results grr ON grr.game_id = g.id
        WHERE g.player_id = %s AND grr.id IS NULL AND g.no_repertoire_match = TRUE
    """
    unmatched_params = [player_id]
    if since:
        unmatched_query += " AND g.played_at >= %s"; unmatched_params.append(since)
    with conn.cursor() as cur:
        cur.execute(unmatched_query, unmatched_params)
        unmatched = [dict(r) for r in cur.fetchall()]

    return {"total_games": total_games, "matched": matched, "unmatched": unmatched}


def get_player_books(conn, player_id: int) -> list:
    """Return active book titles for a player (for filter dropdowns)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT title FROM books
            WHERE player_id = %s AND active = TRUE ORDER BY title
        """, (player_id,))
        return [r["title"] for r in cur.fetchall()]


def get_player_chapters(conn, player_id: int, book_title: str) -> list:
    """Return active chapter titles for a book (for filter dropdowns)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ch.title FROM chapters ch
            JOIN books bk ON bk.id = ch.book_id
            WHERE bk.player_id = %s AND bk.title = %s AND ch.active = TRUE
            ORDER BY ch.title
        """, (player_id, book_title))
        return [r["title"] for r in cur.fetchall()]


# ─── Deviations ──────────────────────────────────────────────────────────────

def get_deviation_rows(conn, player_id: int, since=None, color: str = "All",
                       min_ply: int = 1, last_n_games: int = 0) -> list:
    """Return deviation rows for ranking (no moves data)."""
    query = """
        SELECT
            bk.title             AS book,
            bk.color             AS color,
            ch.title             AS chapter,
            rl.chessable_line_id AS line_id,
            rl.line_name         AS line,
            grr.deviated_at_ply  AS ply,
            grr.expected_move    AS expected_move,
            grr.played_move      AS played_move,
            grr.book_id          AS book_id,
            grr.chapter_id       AS chapter_id,
            g.result             AS result,
            g.url                AS game_url
        FROM game_repertoire_results grr
        JOIN games g   ON g.id   = grr.game_id
        JOIN books bk  ON bk.id  = grr.book_id
        JOIN chapters ch ON ch.id = grr.chapter_id
        JOIN game_result_lines grl ON grl.game_repertoire_result_id = grr.id
        JOIN repertoire_lines rl ON rl.id = grl.line_id
        WHERE g.player_id = %s AND grr.deviation_by = 'me'
    """
    params = [player_id]
    if last_n_games > 0:
        query += """
          AND g.id IN (
              SELECT id FROM games WHERE player_id = %s
              ORDER BY played_at DESC LIMIT %s
          )
        """
        params += [player_id, last_n_games]
    elif since:
        query += " AND g.played_at >= %s"; params.append(since)
    if color != "All":
        query += " AND bk.color = %s"; params.append(color.lower())
    if min_ply > 1:
        query += " AND grr.deviated_at_ply >= %s"; params.append(min_ply)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def get_deviation_details(conn, player_id: int, combos: list,
                          since=None, last_n_games: int = 0) -> list:
    """
    Return deviation rows with moves for specific (ply, expected_move, book_id, chapter_id) combos.
    combos is a list of (ply, expected_move, book_id, chapter_id) tuples.
    """
    if not combos:
        return []
    combo_conditions = " OR ".join(
        "(grr.deviated_at_ply = %s AND grr.expected_move = %s AND grr.book_id = %s AND grr.chapter_id = %s)"
        for _ in combos
    )
    params = [player_id]
    for combo in combos:
        params.extend(combo)

    query = f"""
        SELECT
            bk.title             AS book,
            bk.color             AS color,
            ch.title             AS chapter,
            rl.chessable_line_id AS line_id,
            rl.line_name         AS line,
            grr.deviated_at_ply  AS ply,
            grr.expected_move    AS expected_move,
            grr.played_move      AS played_move,
            grr.book_id          AS book_id,
            grr.chapter_id       AS chapter_id,
            g.result             AS result,
            g.url                AS game_url,
            g.moves              AS moves
        FROM game_repertoire_results grr
        JOIN games g   ON g.id   = grr.game_id
        JOIN books bk  ON bk.id  = grr.book_id
        JOIN chapters ch ON ch.id = grr.chapter_id
        JOIN game_result_lines grl ON grl.game_repertoire_result_id = grr.id
        JOIN repertoire_lines rl ON rl.id = grl.line_id
        WHERE g.player_id = %s AND grr.deviation_by = 'me'
          AND ({combo_conditions})
    """
    if last_n_games > 0:
        query += """
          AND g.id IN (
              SELECT id FROM games WHERE player_id = %s
              ORDER BY played_at DESC LIMIT %s
          )
        """
        params += [player_id, last_n_games]
    elif since:
        query += " AND g.played_at >= %s"; params.append(since)

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


# ─── Study Queue ─────────────────────────────────────────────────────────────

def get_queue(conn, player_id: int, include_studied: bool = False) -> list:
    """Return study queue items for a player."""
    query = """
        SELECT id, fen, source_types, context, chessable_line_id,
               note, sort_order, added_at, studied_at,
               move_played, best_move, ply, color, moves
        FROM study_queue WHERE player_id = %s
    """
    if not include_studied:
        query += " AND studied_at IS NULL"
    else:
        query += " AND studied_at IS NOT NULL"
    query += " ORDER BY sort_order ASC, added_at ASC"
    with conn.cursor() as cur:
        cur.execute(query, (player_id,))
        return [dict(r) for r in cur.fetchall()]


def add_to_queue(player_id: int, fen: str, source_type: str,
                 context: str = None, chessable_line_id: int = None,
                 move_played: str = None, best_move: str = None,
                 ply: int = None, color: str = None, moves=None):
    """Add or update a position in the study queue."""
    import json as _json
    conn = get_conn()
    moves_json = _json.dumps(moves) if moves is not None else None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(sort_order), 0) FROM study_queue WHERE player_id = %s",
            (player_id,)
        )
        max_order = cur.fetchone()["coalesce"]
        cur.execute("""
            INSERT INTO study_queue
                (player_id, fen, source_types, context, chessable_line_id, sort_order,
                 move_played, best_move, ply, color, moves)
            VALUES (%s, %s, %s::text[], %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id, fen) DO UPDATE SET
                source_types = (
                    SELECT array_agg(DISTINCT elem)
                    FROM unnest(study_queue.source_types || %s::text[]) AS elem
                ),
                context = CASE WHEN EXCLUDED.context IS NOT NULL THEN EXCLUDED.context ELSE study_queue.context END,
                chessable_line_id = CASE WHEN EXCLUDED.chessable_line_id IS NOT NULL THEN EXCLUDED.chessable_line_id ELSE study_queue.chessable_line_id END,
                move_played = CASE WHEN EXCLUDED.move_played IS NOT NULL THEN EXCLUDED.move_played ELSE study_queue.move_played END,
                best_move = CASE WHEN EXCLUDED.best_move IS NOT NULL THEN EXCLUDED.best_move ELSE study_queue.best_move END,
                ply = CASE WHEN EXCLUDED.ply IS NOT NULL THEN EXCLUDED.ply ELSE study_queue.ply END,
                color = CASE WHEN EXCLUDED.color IS NOT NULL THEN EXCLUDED.color ELSE study_queue.color END,
                moves = CASE WHEN EXCLUDED.moves IS NOT NULL THEN EXCLUDED.moves ELSE study_queue.moves END
        """, (
            player_id, fen, [source_type], context, chessable_line_id, max_order + 1,
            move_played, best_move, ply, color, moves_json, [source_type]
        ))
    conn.commit()
    conn.close()


def update_note(player_id: int, item_id: int, note: str):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE study_queue SET note = %s WHERE id = %s AND player_id = %s",
                    (note, item_id, player_id))
    conn.commit(); conn.close()


def mark_studied(player_id: int, item_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE study_queue SET studied_at = now() WHERE id = %s AND player_id = %s",
                    (item_id, player_id))
    conn.commit(); conn.close()


def unmark_studied(player_id: int, item_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE study_queue SET studied_at = NULL WHERE id = %s AND player_id = %s",
                    (item_id, player_id))
    conn.commit(); conn.close()


def reorder_item(player_id: int, item_id: int, new_position: int, all_ids: list):
    conn = get_conn()
    remaining = [i for i in all_ids if i != item_id]
    new_order  = remaining[:new_position - 1] + [item_id] + remaining[new_position - 1:]
    with conn.cursor() as cur:
        for idx, qid in enumerate(new_order):
            cur.execute("UPDATE study_queue SET sort_order = %s WHERE id = %s AND player_id = %s",
                        (idx, qid, player_id))
    conn.commit(); conn.close()


def is_in_queue(conn, player_id: int, fen: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM study_queue WHERE player_id = %s AND fen = %s AND studied_at IS NULL",
            (player_id, fen)
        )
        return cur.fetchone() is not None


def get_blunder_history(conn, player_id: int, fen: str) -> list:
    """All blunder occurrences for a FEN, most recent first."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.move_played, b.best_move, b.centipawn_loss, b.classification,
                   b.phase, g.played_at, g.url, g.opening_name, g.result,
                   g.opponent_username, g.player_color
            FROM blunders b
            JOIN games g ON g.id = b.game_id
            WHERE g.player_id = %s AND b.fen = %s
            ORDER BY g.played_at DESC
        """, (player_id, fen))
        return [dict(r) for r in cur.fetchall()]


# ─── Repertoire ───────────────────────────────────────────────────────────────

def get_or_create_repertoire(conn, player_id: int, title: str, color: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM books WHERE player_id = %s AND title = %s AND active = TRUE",
            (player_id, title)
        )
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO books (player_id, title, color, active) VALUES (%s, %s, %s, TRUE) RETURNING id",
            (player_id, title, color)
        )
        book_id = cur.fetchone()["id"]
    conn.commit()
    return book_id


def get_or_create_section(conn, book_id: int, title: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM chapters WHERE book_id = %s AND title = %s",
            (book_id, title)
        )
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO chapters (book_id, title, active) VALUES (%s, %s, TRUE) RETURNING id",
            (book_id, title)
        )
        chapter_id = cur.fetchone()["id"]
    conn.commit()
    return chapter_id


def insert_lines(conn, chapter_id: int, lines: list) -> tuple:
    """Insert repertoire lines. Returns (inserted, skipped) counts."""
    from utils import moves_to_fen_sequence
    import json as _json
    inserted = skipped = 0
    with conn.cursor() as cur:
        for line in lines:
            move_list  = line["move_list"]
            fen_seq    = moves_to_fen_sequence(move_list)
            moves_json = _json.dumps(move_list)
            fen_json   = _json.dumps(fen_seq)
            try:
                cur.execute("""
                    INSERT INTO repertoire_lines
                        (chapter_id, line_name, moves, fen_sequence, active)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (chapter_id, moves) DO NOTHING
                """, (chapter_id, line["line_name"], moves_json, fen_json))
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
    conn.commit()
    return inserted, skipped


def rematch_unmatched(conn, player_id: int) -> int:
    """Clear no_repertoire_match flag so pipeline re-attempts matching."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE games SET no_repertoire_match = FALSE
            WHERE player_id = %s AND no_repertoire_match = TRUE
        """, (player_id,))
        count = cur.rowcount
    conn.commit()
    return count


def rematch_all(conn, player_id: int) -> int:
    """Delete all match results and clear flags for a full re-match."""
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM game_result_lines
            WHERE game_repertoire_result_id IN (
                SELECT grr.id FROM game_repertoire_results grr
                JOIN games g ON g.id = grr.game_id
                WHERE g.player_id = %s
            )
        """, (player_id,))
        cur.execute("""
            DELETE FROM game_repertoire_results grr
            USING games g WHERE g.id = grr.game_id AND g.player_id = %s
        """, (player_id,))
        cur.execute("""
            UPDATE games SET no_repertoire_match = FALSE
            WHERE player_id = %s
        """, (player_id,))
        count = cur.rowcount
    conn.commit()
    return count


# ─── Opponent Scout ───────────────────────────────────────────────────────────

def get_opponent_profiles(conn, player_id: int) -> list:
    """Return active, initialized opponent profiles for a player."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT op.id, op.name, op.is_initialized,
                   COUNT(og.id) AS game_count
            FROM opponent_profiles op
            LEFT JOIN opponent_games og ON og.opponent_profile_id = op.id
            WHERE op.player_id = %s
              AND op.active = TRUE
              AND op.is_initialized = TRUE
            GROUP BY op.id, op.name, op.is_initialized
            ORDER BY op.name
        """, (player_id,))
        return [dict(r) for r in cur.fetchall()]


def get_scout_rows(conn, player_id: int, profile_id: int,
                   my_since=None, opp_since=None,
                   min_freq: int = 2, tier_filter: str = "All") -> list:
    """
    Return scouting rows — repertoire/blunder/shared positions opponent has reached.
    Wrapped by a cached function in opponent_scout.py.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, u.display_name as user_display_name
            FROM players p JOIN users u ON u.id = p.user_id
            WHERE p.id = %s
        """, (player_id,))
        player = dict(cur.fetchone())

    rep_fens_query = """
        SELECT DISTINCT fen FROM (
            SELECT unnest(rl.fen_sequence::text[]) AS fen
            FROM repertoire_lines rl
            JOIN chapters ch ON ch.id = rl.chapter_id
            JOIN books bk ON bk.id = ch.book_id
            WHERE bk.player_id = %s AND rl.active = TRUE AND ch.active = TRUE AND bk.active = TRUE
        ) sub
    """

    params_base = {"player_id": player_id, "profile_id": profile_id,
                   "min_freq": min_freq}

    opp_filter = ""
    if opp_since:
        opp_filter = "AND ogf.played_at >= %(opp_since)s"
        params_base["opp_since"] = opp_since

    my_filter = ""
    if my_since:
        my_filter = "AND g.played_at >= %(my_since)s"
        params_base["my_since"] = my_since

    query = f"""
        WITH rep_fens AS ({rep_fens_query.replace('%s', '%(player_id)s')}),
        opp_agg AS (
            SELECT ogf.fen, COUNT(DISTINCT ogf.opponent_game_id) AS freq,
                   MODE() WITHIN GROUP (ORDER BY ogf.played_as) AS played_as
            FROM opponent_game_fens ogf
            WHERE ogf.opponent_profile_id = %(profile_id)s {opp_filter}
            GROUP BY ogf.fen
            HAVING COUNT(DISTINCT ogf.opponent_game_id) >= %(min_freq)s
        ),
        my_blunder_fens AS (
            SELECT DISTINCT b.fen FROM blunders b
            JOIN games g ON g.id = b.game_id
            WHERE g.player_id = %(player_id)s {my_filter}
        )
        SELECT
            opp_agg.fen,
            opp_agg.freq,
            opp_agg.played_as,
            CASE
                WHEN my_blunder_fens.fen IS NOT NULL THEN 'blunder'
                WHEN rep_fens.fen IS NOT NULL        THEN 'repertoire'
                ELSE                                      'shared'
            END AS tier
        FROM opp_agg
        LEFT JOIN rep_fens       ON rep_fens.fen       = opp_agg.fen
        LEFT JOIN my_blunder_fens ON my_blunder_fens.fen = opp_agg.fen
        ORDER BY
            CASE WHEN my_blunder_fens.fen IS NOT NULL THEN 1
                 WHEN rep_fens.fen IS NOT NULL         THEN 2
                 ELSE 3 END,
            opp_agg.freq DESC
    """

    with conn.cursor() as cur:
        cur.execute(query, params_base)
        rows = [dict(r) for r in cur.fetchall()]

    if tier_filter != "All":
        tier_map = {"Blunder positions": "blunder",
                    "Repertoire matches": "repertoire",
                    "Shared positions": "shared"}
        rows = [r for r in rows if r["tier"] == tier_map.get(tier_filter, tier_filter)]

    return rows


def get_scout_page_moves(conn, profile_id: int, fens: list) -> dict:
    """
    For each FEN, fetch moves from an example opponent game that contains it.
    Returns {fen: moves_list}.
    """
    if not fens:
        return {}
    import json as _json
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (ogf.fen)
                   ogf.fen, og.moves
            FROM   opponent_game_fens ogf
            JOIN   opponent_games og ON og.id = ogf.opponent_game_id
            WHERE  ogf.opponent_profile_id = %s
              AND  ogf.fen = ANY(%s)
              AND  og.moves IS NOT NULL
            ORDER  BY ogf.fen, og.played_at DESC
        """, (profile_id, fens))
        rows = cur.fetchall()
    result = {}
    for row in rows:
        moves = row["moves"]
        if isinstance(moves, str):
            try:
                moves = _json.loads(moves)
            except Exception:
                moves = []
        result[row["fen"]] = moves
    return result


def get_scout_page_game_links(conn, player_id: int, profile_id: int,
                              fens: list) -> dict:
    """
    For each FEN fetch player's games and opponent's games at that position.
    Returns {fen: {"my_games": [...], "opp_games": [...]}}.
    """
    if not fens:
        return {}

    result = {fen: {"my_games": [], "opp_games": []} for fen in fens}

    # Your games that produced a blunder at this FEN
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT b.fen, g.url, g.played_at,
                   g.opponent_username, g.player_color, b.classification
            FROM   blunders b
            JOIN   games g ON g.id = b.game_id
            WHERE  g.player_id = %s AND b.fen = ANY(%s)
            ORDER  BY b.fen, g.played_at DESC
        """, (player_id, fens))
        for row in cur.fetchall():
            result[row["fen"]]["my_games"].append({
                "date":     row["played_at"].strftime("%Y-%m-%d") if row["played_at"] else "",
                "opponent": row["opponent_username"] or "",
                "color":    row["player_color"] or "",
                "class":    row["classification"] or "",
                "url":      row["url"] or "",
            })

    # For FENs with no blunder games, fall back to GIN lookup on fen_sequence
    fens_without = [fen for fen in fens if not result[fen]["my_games"]]
    for fen in fens_without:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT g.url, g.played_at, g.opponent_username, g.player_color
                FROM   games g
                WHERE  g.player_id = %s
                  AND  g.fen_sequence @> jsonb_build_array(%s::text)
                ORDER  BY g.played_at DESC LIMIT 5
            """, (player_id, fen))
            for row in cur.fetchall():
                result[fen]["my_games"].append({
                    "date":     row["played_at"].strftime("%Y-%m-%d") if row["played_at"] else "",
                    "opponent": row["opponent_username"] or "",
                    "color":    row["player_color"] or "",
                    "class":    "",
                    "url":      row["url"] or "",
                })

    # Opponent games at each FEN
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (ogf.fen, og.id)
                   ogf.fen, og.played_at, og.opening_name,
                   og.source_type, og.external_game_id, og.played_as
            FROM   opponent_game_fens ogf
            JOIN   opponent_games og ON og.id = ogf.opponent_game_id
            WHERE  ogf.opponent_profile_id = %s AND ogf.fen = ANY(%s)
            ORDER  BY ogf.fen, og.id, og.played_at DESC
        """, (profile_id, fens))
        for row in cur.fetchall():
            ext_id = row["external_game_id"] or ""
            if row["source_type"] == "lichess":
                game_url = f"https://lichess.org/{ext_id}" if ext_id else ""
            elif row["source_type"] == "chesscom":
                game_url = f"https://www.chess.com/game/live/{ext_id}" if ext_id else ""
            else:
                game_url = ""
            result[row["fen"]]["opp_games"].append({
                "date":    row["played_at"].strftime("%Y-%m-%d") if row["played_at"] else "",
                "opening": row["opening_name"] or "",
                "as":      row["played_as"] or "",
                "url":     game_url,
            })

    return result