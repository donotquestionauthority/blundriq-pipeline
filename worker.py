"""
worker.py — Fargate container entry point.

Reads JOB_TYPE and PLAYER_ID from environment variables and dispatches
to the appropriate analysis script. This is the CMD in the Dockerfile.

Environment variables:
    JOB_TYPE   — "fast_pass" or "deep_pass" (required)
    PLAYER_ID  — integer player ID (required)
    WORKERS    — number of parallel workers (optional, defaults per script)

    Plus all standard pipeline env vars:
    DATABASE_URL, SUPABASE_URL, SUPABASE_KEY
"""

import os
import sys

def main():
    job_type = os.environ.get("JOB_TYPE", "").strip()
    player_id_str = os.environ.get("PLAYER_ID", "").strip()

    if not job_type:
        print("ERROR: JOB_TYPE environment variable is required (fast_pass or deep_pass)")
        sys.exit(1)

    if not player_id_str:
        print("ERROR: PLAYER_ID environment variable is required")
        sys.exit(1)

    try:
        player_id = int(player_id_str)
    except ValueError:
        print(f"ERROR: PLAYER_ID must be an integer, got: {player_id_str!r}")
        sys.exit(1)

    workers = os.environ.get("WORKERS", "").strip()

    print(f"[worker] JOB_TYPE={job_type} PLAYER_ID={player_id}")

    if job_type == "onboarding_pass":
        argv = ["onboarding_pass.py", "--player-id", str(player_id)]
        if workers:
            argv += ["--workers", workers]
        sys.argv = argv

        from onboarding_pass import main as run
        run()

    elif job_type == "fast_pass":
        # Patch sys.argv so fast_pass.py's argparse sees the right args
        argv = ["fast_pass.py", "--player-id", str(player_id)]
        if workers:
            argv += ["--workers", workers]
        sys.argv = argv

        from fast_pass import main as run
        run()

    elif job_type == "deep_pass":
        argv = ["deep_pass.py", "--run", "--player-id", str(player_id)]
        if workers:
            argv += ["--workers", workers]
        sys.argv = argv

        from deep_pass import main as run
        run()

    else:
        print(f"ERROR: Unknown JOB_TYPE: {job_type!r}. Expected 'onboarding_pass', 'fast_pass', or 'deep_pass'")
        sys.exit(1)


if __name__ == "__main__":
    main()
