import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import get_conn
from utils import ts, moves_to_fen_sequence
from config import MIN_MATCH_PLY

def subsequence_match_length(game_fens: list, rep_fens: list) -> int:
    """
    Find how many positions from the repertoire line appear
    in the game in order (subsequence matching).
    Handles transpositions and move order differences correctly.
    Returns number of matched positions (excluding starting position).
    """
    game_index = 0
    matched    = 0
    for rep_fen in rep_fens[1:]:  # skip starting position
        while game_index < len(game_fens):
            if game_fens[game_index] == rep_fen:
                matched += 1
                game_index += 1
                break
            game_index += 1
    return matched

def who_deviated(game_moves: list, rep_moves: list,
                 matched_ply: int, player_color: str) -> tuple:
    if matched_ply >= len(rep_moves):
        return "none", None, None
    if matched_ply >= len(game_moves):
        return "none", None, None
    expected_move = rep_moves[matched_ply]
    played_move   = game_moves[matched_ply]
    mover = "white" if matched_ply % 2 == 0 else "black"
    if mover == player_color:
        return "me", expected_move, played_move
    else:
        return "opponent", expected_move, played_move

def compute_matches(games: list, active_lines: list) -> tuple:
    """Subsequence FEN matching — handles transpositions correctly."""
    lines_by_color = {"white": [], "black": []}
    for line in active_lines:
        lines_by_color[line["color"]].append(line)

    result_rows      = []
    lines_by_game_id = {}

    for i, game in enumerate(games):
        game_moves = game["moves"]
        if isinstance(game_moves, str):
            game_moves = json.loads(game_moves)

        game_fens = game.get("fen_sequence")
        if game_fens is None:
            game_fens = moves_to_fen_sequence(game_moves)
        elif isinstance(game_fens, str):
            game_fens = json.loads(game_fens)

        player_color = game["player_color"]
        color_lines  = lines_by_color.get(player_color, [])

        if not color_lines:
            continue

        best_ply   = 0
        best_lines = []

        for line in color_lines:
            rep_fens = line.get("fen_sequence")
            if rep_fens is None:
                rep_moves = line["moves"]
                if isinstance(rep_moves, str):
                    rep_moves = json.loads(rep_moves)
                rep_fens = moves_to_fen_sequence(rep_moves)
            elif isinstance(rep_fens, str):
                rep_fens = json.loads(rep_fens)

            ply = subsequence_match_length(game_fens, rep_fens)

            if ply > best_ply:
                best_ply   = ply
                best_lines = [line]
            elif ply == best_ply and ply > 0:
                best_lines.append(line)

        if best_ply < MIN_MATCH_PLY:
            continue

        ref_line  = best_lines[0]
        ref_moves = ref_line["moves"]
        if isinstance(ref_moves, str):
            ref_moves = json.loads(ref_moves)

        deviation_by, expected_move, played_move = who_deviated(
            game_moves, ref_moves, best_ply, player_color
        )

        result_rows.append((
            game["id"],
            ref_line["book_id"],
            ref_line["chapter_id"],
            best_ply,
            deviation_by,
            expected_move,
            played_move
        ))

        lines_by_game_id[game["id"]] = (best_ply, best_lines)

        if (i + 1) % 100 == 0:
            print(f"[{ts()}] Computed {i + 1}/{len(games)} games...")

    return result_rows, lines_by_game_id

def insert_chunk(chunk: list, chunk_num: int, total_chunks: int) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO game_result_lines
                    (game_repertoire_result_id, line_id, matched_ply)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, chunk)
        conn.commit()
        print(f"[{ts()}] Chunk {chunk_num}/{total_chunks} committed ({len(chunk)} rows)")
        return len(chunk)
    except Exception as e:
        conn.rollback()
        print(f"[{ts()}] Chunk {chunk_num} failed: {e}")
        return 0
    finally:
        conn.close()

def insert_results(conn, result_rows: list, lines_by_game_id: dict, workers: int = 5):
    print(f"[{ts()}] Inserting {len(result_rows)} result rows...")

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO game_repertoire_results
                (game_id, book_id, chapter_id, deviated_at_ply,
                 deviation_by, expected_move, played_move)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE
            SET book_id         = EXCLUDED.book_id,
                chapter_id      = EXCLUDED.chapter_id,
                deviated_at_ply = EXCLUDED.deviated_at_ply,
                deviation_by    = EXCLUDED.deviation_by,
                expected_move   = EXCLUDED.expected_move,
                played_move     = EXCLUDED.played_move
        """, result_rows)

        game_ids = [row[0] for row in result_rows]
        cur.execute("""
            SELECT id, game_id FROM game_repertoire_results
            WHERE game_id = ANY(%s)
        """, (game_ids,))
        result_id_map = {row["game_id"]: row["id"] for row in cur.fetchall()}

    conn.commit()
    print(f"[{ts()}] Committed {len(result_rows)} result rows.")

    line_rows = []
    for game_id, (best_ply, best_lines) in lines_by_game_id.items():
        result_id = result_id_map.get(game_id)
        if result_id:
            for line in best_lines:
                line_rows.append((result_id, line["line_id"], best_ply))

    if not line_rows:
        print(f"[{ts()}] No line rows to insert.")
        return

    print(f"[{ts()}] Inserting {len(line_rows)} line rows with {workers} parallel workers...")

    chunk_size = 500
    chunks = [line_rows[i:i + chunk_size] for i in range(0, len(line_rows), chunk_size)]
    total_chunks = len(chunks)

    total_inserted = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(insert_chunk, chunk, i + 1, total_chunks): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            total_inserted += future.result()

    print(f"[{ts()}] Done - {total_inserted} line rows inserted.")