"""
onboarding_pass.py — New user onboarding job.

Imports games and runs fast pass analysis for a single player.
Behaviour differs by subscription status:

  Free user  (is_paid=FALSE):
    - Import last FREE_IMPORT_LIMIT games from Chess.com + Lichess
    - Run fast pass (depth 12) on those games
    - Does NOT enqueue deep pass

  Paid user  (is_paid=TRUE):
    - Import ALL games from Chess.com + Lichess (no cap)
    - Run fast pass (depth 12)
    - Auto-enqueues deep pass via SQS on completion

Sets players.fast_pass_complete = TRUE on completion.

Usage (via worker.py / Fargate env vars):
    JOB_TYPE=onboarding_pass PLAYER_ID=1

Usage (direct):
    python onboarding_pass.py --player-id 1
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from dotenv import load_dotenv
load_dotenv()

from db import get_conn, get_app_settings, get_analysis_game_limit
from utils import ts


def get_player_and_user(conn, player_id: int) -> tuple:
    """Return (player dict, is_paid bool) for the given player_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.*, u.is_paid, u.display_name AS user_display_name, u.email
            FROM players p
            JOIN users u ON u.id = p.user_id
            WHERE p.id = %s
        """, (player_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No player found for player_id={player_id}")
    return dict(row)


def enqueue_deep_pass(player_id: int):
    """Send deep_pass message to SQS. Import here to avoid hard dep in tests."""
    import boto3, json
    sqs_url = os.environ.get("SQS_DEEP_PASS_URL")
    if not sqs_url:
        print(f"[{ts()}] WARNING: SQS_DEEP_PASS_URL not set — cannot auto-enqueue deep pass")
        return
    client = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    client.send_message(
        QueueUrl=sqs_url,
        MessageBody=json.dumps({"job_type": "deep_pass", "player_id": player_id}),
    )
    print(f"[{ts()}] deep_pass enqueued for player {player_id}")


def main():
    parser = argparse.ArgumentParser(description="Onboarding pass — import games + fast pass")
    parser.add_argument("--player-id", type=int, required=True, help="Player ID to onboard")
    parser.add_argument("--workers",   type=int, default=16,    help="Parallel workers for fast pass")
    args = parser.parse_args()

    player_id = args.player_id
    print(f"[{ts()}] onboarding_pass starting for player_id={player_id}")

    conn   = get_conn()
    player = get_player_and_user(conn, player_id)
    conn.close()

    is_paid = bool(player.get("is_paid", False))
    print(f"[{ts()}] Player: {player['user_display_name']} | is_paid={is_paid}")

    # ── Step 1: Import games ───────────────────────────────────────────────────
    print(f"[{ts()}] === Step 1: Import games ===")

    from pipeline.import_chesscom import import_chesscom_games
    from pipeline.import_lichess  import import_lichess_games

    conn     = get_conn()
    settings = get_app_settings(conn)
    free_limit = settings["free_import_limit"]

    if is_paid:
        # Import all games — no cap, newest-first (game_limit=None triggers all_history mode)
        print(f"[{ts()}] Paid user — importing all games")
        cc_imported = import_chesscom_games(conn, player, game_limit=None)
        li_imported = import_lichess_games(conn, player, since_ms=0)
    else:
        # Free user — import last free_import_limit games only
        print(f"[{ts()}] Free user — importing last {free_limit} games")
        cc_imported = import_chesscom_games(conn, player, game_limit=free_limit)
        # For Lichess, since_ms=0 imports all but we rely on game_limit enforcement.
        # Lichess streams in chronological order so we pass since_ms=0 and stop
        # after free_limit total across both sources.
        remaining = max(0, free_limit - cc_imported)
        li_imported = import_lichess_games(conn, player, since_ms=0, game_limit=remaining)

    conn.close()
    print(f"[{ts()}] Imported {cc_imported} Chess.com + {li_imported} Lichess games")

    # ── Step 2: Match repertoire ───────────────────────────────────────────────
    print(f"[{ts()}] === Step 2: Match repertoire ===")
    conn = get_conn()
    from pipeline.match_repertoire import get_unmatched_games, mark_no_match
    from pipeline.matching import compute_matches, insert_results
    from db import get_active_lines_for_player

    limit = get_analysis_game_limit(conn, player_id)
    unmatched = get_unmatched_games(conn, player_id, limit)
    print(f"[{ts()}] {len(unmatched)} unmatched games to process (within {limit}-game window)")
    if unmatched:
        lines = get_active_lines_for_player(conn, player_id)
        result_rows, lines_by_game_id = compute_matches(unmatched, lines)
        no_match_ids = [g["id"] for g in unmatched if g["id"] not in lines_by_game_id]
        if result_rows:
            insert_results(conn, result_rows, lines_by_game_id)
        if no_match_ids:
            mark_no_match(conn, no_match_ids)
        conn.commit()
    conn.close()
    print(f"[{ts()}] Repertoire matching complete")

    # ── Step 3: Fast pass analysis ─────────────────────────────────────────────
    # Skip if already complete — avoids overwriting depth-18 blunders with depth-12
    if player.get("fast_pass_complete"):
        print(f"[{ts()}] fast_pass_complete already TRUE — skipping fast pass")
    else:
        print(f"[{ts()}] === Step 3: Fast pass ===")
        sys.argv = ["fast_pass.py", "--player-id", str(player_id), "--workers", str(args.workers)]
        from fast_pass import main as run_fast_pass
        run_fast_pass()

    # ── Step 4: Auto-enqueue deep pass for paid users ──────────────────────────
    if is_paid:
        if player.get("deep_pass_complete"):
            print(f"[{ts()}] deep_pass_complete already TRUE — skipping deep pass enqueue")
        else:
            print(f"[{ts()}] === Step 4: Enqueue deep pass ===")
            enqueue_deep_pass(player_id)
    else:
        print(f"[{ts()}] Free user — skipping deep pass enqueue")

    print(f"[{ts()}] onboarding_pass complete for player_id={player_id}")


if __name__ == "__main__":
    main()
