import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import chess
from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from utils import ts

def moves_to_fen_sequence(moves: list) -> list:
    """Convert a list of SAN moves to a list of FEN strings."""
    board = chess.Board()
    fens = [board.fen()]  # include starting position
    for san in moves:
        try:
            move = board.parse_san(san)
            board.push(move)
            fens.append(board.fen())
        except Exception:
            break
    return fens

def backfill_repertoire_lines(conn):
    print(f"[{ts()}] Backfilling repertoire lines...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, moves FROM repertoire_lines
            WHERE fen_sequence IS NULL
        """)
        rows = cur.fetchall()

    print(f"[{ts()}] Found {len(rows)} lines to process.")

    updated = 0
    for i, row in enumerate(rows):
        moves = row["moves"]
        if isinstance(moves, str):
            moves = json.loads(moves)

        fen_seq = moves_to_fen_sequence(moves)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repertoire_lines
                SET fen_sequence = %s
                WHERE id = %s
            """, (json.dumps(fen_seq), row["id"]))
        updated += 1

        if (i + 1) % 100 == 0:
            conn.commit()
            print(f"[{ts()}] {i + 1}/{len(rows)} lines processed...")

    conn.commit()
    print(f"[{ts()}] Done - {updated} repertoire lines updated.")

def backfill_games(conn):
    print(f"\n[{ts()}] Backfilling games...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, moves FROM games
            WHERE fen_sequence IS NULL
        """)
        rows = cur.fetchall()

    print(f"[{ts()}] Found {len(rows)} games to process.")

    updated = 0
    for i, row in enumerate(rows):
        moves = row["moves"]
        if isinstance(moves, str):
            moves = json.loads(moves)

        fen_seq = moves_to_fen_sequence(moves)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE games
                SET fen_sequence = %s
                WHERE id = %s
            """, (json.dumps(fen_seq), row["id"]))
        updated += 1

        if (i + 1) % 100 == 0:
            conn.commit()
            print(f"[{ts()}] {i + 1}/{len(rows)} games processed...")

    conn.commit()
    print(f"[{ts()}] Done - {updated} games updated.")

def main():
    conn = get_conn()

    backfill_repertoire_lines(conn)
    backfill_games(conn)

    conn.close()
    print(f"\n[{ts()}] All done!")

if __name__ == "__main__":
    main()