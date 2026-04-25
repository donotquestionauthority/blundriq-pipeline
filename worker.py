"""
worker.py — Fargate container entry point.

Reads JOB_TYPE and PLAYER_ID from environment variables and dispatches
to the appropriate analysis script. This is the CMD in the Dockerfile.

A SIGALRM-based timeout (JOB_TIMEOUT_HOURS, default 4) is installed before
dispatch. If the job exceeds the ceiling the process exits with code 2.
The AWS-side watchdog (blundriq_ecs_watchdog Lambda) provides a second,
independent kill at 5 hours in case the signal never fires.

Exit codes:
    0  — success
    1  — bad environment / unknown job type
    2  — job timed out

Environment variables:
    JOB_TYPE          — "onboarding_pass", "fast_pass", or "deep_pass" (required)
    PLAYER_ID         — integer player ID (required)
    WORKERS           — number of parallel workers (optional, defaults per script)
    JOB_TIMEOUT_HOURS — hard ceiling in hours (optional, default 4)

    Plus all standard pipeline env vars:
    DATABASE_URL, SUPABASE_URL, SUPABASE_KEY
"""

import os
import signal
import sys

# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

_DEFAULT_TIMEOUT_HOURS = 4


class JobTimeoutError(Exception):
    """Raised when SIGALRM fires — job exceeded the hard time ceiling."""


def _timeout_handler(signum, frame):
    raise JobTimeoutError()


def _install_timeout(hours: float) -> None:
    """
    Register a SIGALRM that fires after `hours` hours.

    SIGALRM is only available on Unix. On any other platform this is a
    no-op — the AWS watchdog Lambda still provides the external ceiling.
    """
    if not hasattr(signal, "SIGALRM"):
        print(f"[worker] WARNING: SIGALRM not available on this platform — timeout not installed")
        return
    seconds = int(hours * 3600)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(seconds)
    print(f"[worker] Timeout set: {hours:.1f} hours ({seconds}s)")


def _cancel_timeout() -> None:
    """Disarm the alarm after a clean finish."""
    if hasattr(signal, "SIGALRM"):
        signal.alarm(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    job_type = os.environ.get("JOB_TYPE", "").strip()
    player_id_str = os.environ.get("PLAYER_ID", "").strip()

    if not job_type:
        print("ERROR: JOB_TYPE environment variable is required (onboarding_pass, fast_pass, or deep_pass)")
        sys.exit(1)

    if not player_id_str:
        print("ERROR: PLAYER_ID environment variable is required")
        sys.exit(1)

    try:
        player_id = int(player_id_str)
    except ValueError:
        print(f"ERROR: PLAYER_ID must be an integer, got: {player_id_str!r}")
        sys.exit(1)

    try:
        timeout_hours = float(os.environ.get("JOB_TIMEOUT_HOURS", str(_DEFAULT_TIMEOUT_HOURS)))
    except ValueError:
        print(f"WARNING: Invalid JOB_TIMEOUT_HOURS value — using default {_DEFAULT_TIMEOUT_HOURS}h")
        timeout_hours = _DEFAULT_TIMEOUT_HOURS

    workers = os.environ.get("WORKERS", "").strip()

    print(f"[worker] JOB_TYPE={job_type} PLAYER_ID={player_id}")

    _install_timeout(timeout_hours)

    try:
        if job_type == "onboarding_pass":
            argv = ["onboarding_pass.py", "--player-id", str(player_id)]
            if workers:
                argv += ["--workers", workers]
            sys.argv = argv

            from onboarding_pass import main as run
            run()

        elif job_type == "fast_pass":
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

    except JobTimeoutError:
        print(
            f"[worker] TIMEOUT: {job_type} for player_id={player_id} exceeded "
            f"{timeout_hours:.1f}h ceiling — exiting with code 2"
        )
        sys.exit(2)

    finally:
        _cancel_timeout()


if __name__ == "__main__":
    main()
