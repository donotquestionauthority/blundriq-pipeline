"""
backfill_opponent_game_fens.py

One-time migration: populate opponent_game_fens for all opponent_games rows
that existed before this table was created.

Safe to re-run — skips games that already have FEN rows.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from utils import ts

# Conservative worker count — 16 simultaneous connections overwhelms Supabase
NUM_WORKERS = 4


def _process_chunk(chunk: list, chunk_num: int, total_chunks: int) -> dict:
    """
    Worker: build all FEN rows for a chunk of games, insert in one executemany.
    Retries the DB connection once if SSL drops.
    """
    all_fen_rows = []
    for game in chunk:
        fen_sequence = game["fen_sequence"]
        if isinstance(fen_sequence, str):
            try:
                fen_sequence = json.loads(fen_sequence)
            except Exception:
                continue
        for depth, fen in enumerate(fen_sequence):
            if depth >= 1:
                all_fen_rows.append((
                    game["id"], game["opponent_profile_id"], fen, depth,
                    game["played_at"], game["played_as"]
                ))

    if not all_fen_rows:
        return {"chunk": chunk_num, "inserted": 0, "error": None}

    for attempt in range(2):  # retry once on connection failure
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO opponent_game_fens
                        (opponent_game_id, opponent_profile_id, fen,
                         depth, played_at, played_as)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, all_fen_rows)
            conn.commit()
            conn.close()
            return {"chunk": chunk_num, "inserted": len(all_fen_rows), "error": None}
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            if attempt == 0:
                time.sleep(2)  # brief pause before retry
                continue
            return {"chunk": chunk_num, "inserted": 0, "error": str(e)}


def backfill_all(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT og.opponent_profile_id, op.name
            FROM   opponent_games og
            JOIN   opponent_profiles op ON op.id = og.opponent_profile_id
            WHERE  og.fen_sequence IS NOT NULL
              AND  NOT EXISTS (
                  SELECT 1 FROM opponent_game_fens ogf
                  WHERE  ogf.opponent_game_id = og.id
              )
        """)
        profiles = cur.fetchall()

    if not profiles:
        print(f"[{ts()}] Nothing to backfill — opponent_game_fens is up to date.")
        return

    print(f"[{ts()}] Found {len(profiles)} profile(s) with games to backfill.")

    for profile in profiles:
        profile_id   = profile["opponent_profile_id"]
        profile_name = profile["name"]
        print(f"\n[{ts()}] Profile: {profile_name} (id={profile_id})")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT og.id, og.fen_sequence, og.played_at,
                       og.played_as, og.opponent_profile_id
                FROM   opponent_games og
                WHERE  og.opponent_profile_id = %s
                  AND  og.fen_sequence IS NOT NULL
                  AND  NOT EXISTS (
                      SELECT 1 FROM opponent_game_fens ogf
                      WHERE  ogf.opponent_game_id = og.id
                  )
            """, (profile_id,))
            games = cur.fetchall()

        total_games = len(games)
        if not total_games:
            print(f"[{ts()}] Nothing to backfill for {profile_name}.")
            continue

        workers    = min(NUM_WORKERS, total_games)
        chunk_size = max(1, total_games // workers)
        chunks     = [
            [dict(g) for g in games[i:i + chunk_size]]
            for i in range(0, total_games, chunk_size)
        ]
        total_chunks = len(chunks)

        print(f"[{ts()}] {total_games} games → {total_chunks} chunks "
              f"across {workers} workers.")

        total      = 0
        done       = 0
        errors     = 0
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_chunk, chunk, i + 1, total_chunks): i
                for i, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                result = future.result()
                done  += 1
                if result["error"]:
                    errors += 1
                    print(f"[{ts()}]   Chunk {result['chunk']} failed: "
                          f"{result['error']}")
                else:
                    total += result["inserted"]

                elapsed   = time.time() - start_time
                remaining = (elapsed / done) * (total_chunks - done) if done else 0
                print(
                    f"[{ts()}]   {done}/{total_chunks} chunks | "
                    f"{total:,} FEN rows inserted | "
                    f"~{remaining:.0f}s remaining"
                )

        elapsed_total = time.time() - start_time
        print(f"[{ts()}] {profile_name}: {total:,} FEN rows inserted "
              f"in {elapsed_total:.1f}s "
              f"({'no errors' if not errors else f'{errors} chunk(s) failed'}).")

    print(f"\n[{ts()}] Backfill complete.")


if __name__ == "__main__":
    conn = get_conn()
    backfill_all(conn)
    conn.close()