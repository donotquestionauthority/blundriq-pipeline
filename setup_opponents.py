import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time
from datetime import datetime, timezone, timedelta
from multiprocessing import Pool
from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from utils import ts
from pipeline.import_opponent_games import (
    fetch_opponent_chesscom,
    fetch_opponent_lichess,
    compute_fen_for_game,
    insert_opponent_games,
)

# How far back to fetch games on first initialization
BACKFILL_DAYS = 365

from analyze_parallel import NUM_WORKERS


def main():
    print("=" * 50)
    print(f"[{ts()}] INITIATIVE - Opponent Setup")
    print("=" * 50)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT op.*
            FROM   opponent_profiles op
            WHERE  op.active          = TRUE
              AND  op.is_initialized  = FALSE
        """)
        profiles = cur.fetchall()

    print(f"[{ts()}] Found {len(profiles)} uninitialized opponent profile(s).")

    if not profiles:
        conn.close()
        print(f"[{ts()}] Nothing to do.")
        return

    since_dt = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)

    for profile in profiles:
        print(f"\n[{ts()}] ── Opponent: {profile['name']} (profile_id={profile['id']}) ──")

        # Load active, non-manual sources for this profile
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM opponent_sources
                WHERE  opponent_profile_id = %s
                  AND  active              = TRUE
                  AND  source_type        != 'manual'
            """, (profile["id"],))
            sources = cur.fetchall()

        if not sources:
            print(f"[{ts()}] No active sources found — skipping.")
            continue

        profile_total = 0
        all_ok        = True

        for source in sources:
            source_type = source["source_type"]
            username    = source["username"]
            print(f"\n[{ts()}] Source: {source_type} / {username}")

            # ── Step 1: Fetch raw games from API ──────────────────────────
            t0 = time.time()
            try:
                if source_type == "chesscom":
                    raw_games = fetch_opponent_chesscom(username, since_dt)
                elif source_type == "lichess":
                    raw_games = fetch_opponent_lichess(username, since_dt)
                else:
                    print(f"[{ts()}] Unknown source_type '{source_type}' — skipping.")
                    continue
            except Exception as e:
                print(f"[{ts()}] Fetch failed: {e}")
                all_ok = False
                continue

            fetch_secs = time.time() - t0
            print(f"[{ts()}] Fetched {len(raw_games)} games in {fetch_secs:.1f}s.")

            if not raw_games:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE opponent_sources SET last_fetched = NOW() WHERE id = %s",
                        (source["id"],)
                    )
                conn.commit()
                continue

            # ── Step 2: Parallel FEN computation ──────────────────────────
            workers = min(NUM_WORKERS, len(raw_games))
            print(f"[{ts()}] Computing FEN sequences "
                  f"({len(raw_games)} games, {workers} workers)...")
            t1 = time.time()
            try:
                with Pool(processes=workers) as pool:
                    processed = pool.map(compute_fen_for_game, raw_games)
            except Exception as e:
                print(f"[{ts()}] Parallel FEN computation failed: {e}")
                print(f"[{ts()}] Falling back to single-threaded...")
                processed = [compute_fen_for_game(g) for g in raw_games]

            fen_secs = time.time() - t1
            errors   = sum(1 for g in processed if g.get("_fen_error"))
            print(f"[{ts()}] FEN computation done in {fen_secs:.1f}s "
                  f"({errors} error(s)).")

            # ── Step 3: Insert into DB ────────────────────────────────────
            print(f"[{ts()}] Inserting into opponent_games...")
            t2 = time.time()
            try:
                inserted = insert_opponent_games(
                    conn, profile["id"], source_type, processed
                )
            except Exception as e:
                print(f"[{ts()}] Insert failed: {e}")
                all_ok = False
                continue

            insert_secs = time.time() - t2
            print(f"[{ts()}] Inserted {inserted} games in {insert_secs:.1f}s.")
            profile_total += inserted

            # Update last_fetched regardless of insert count
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE opponent_sources SET last_fetched = NOW() WHERE id = %s",
                    (source["id"],)
                )
            conn.commit()

        # ── Mark initialized (only if all sources succeeded) ─────────────
        if all_ok:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE opponent_profiles SET is_initialized = TRUE WHERE id = %s",
                    (profile["id"],)
                )
            conn.commit()
            print(f"\n[{ts()}] {profile['name']} marked as initialized. "
                  f"Total games stored: {profile_total}.")
        else:
            print(f"\n[{ts()}] {profile['name']} NOT marked as initialized "
                  f"due to errors above — will retry on next run.")

    conn.close()
    print(f"\n[{ts()}] Opponent setup complete.")


if __name__ == "__main__":
    main()