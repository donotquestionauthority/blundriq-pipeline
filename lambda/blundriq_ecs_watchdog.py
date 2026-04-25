"""
blundriq_ecs_watchdog.py — Lambda watchdog that kills runaway Fargate tasks.

Triggered by EventBridge Scheduler every 30 minutes. Lists all RUNNING tasks
in the BlundrIQ ECS cluster, and stops any task that has been running longer
than MAX_TASK_AGE_HOURS (default 5). This provides an AWS-side safety net
independent of the in-process SIGALRM timeout in worker.py.

Why 5 hours: worker.py has a 4-hour SIGALRM ceiling. The extra hour ensures
we only kill tasks where the in-process timeout itself has failed — i.e. true
hangs where Stockfish or a subprocess is wedged at the OS level.

Exit codes / error handling:
  - Individual task stop failures are logged but do not abort the handler.
  - list_tasks / describe_tasks failures are fatal (Lambda retries via EventBridge).

Environment variables:
    ECS_CLUSTER         — ECS cluster name (e.g. "blundriq")
    MAX_TASK_AGE_HOURS  — kill threshold in hours (optional, default 5)

IAM permissions required on the Lambda execution role:
    ecs:ListTasks
    ecs:DescribeTasks
    ecs:StopTask
    (all scoped to the blundriq cluster ARN)
"""

import os
from datetime import datetime, timezone

import boto3

ecs = boto3.client("ecs", region_name="us-east-1")

CLUSTER           = os.environ["ECS_CLUSTER"]
MAX_TASK_AGE_HOURS = float(os.environ.get("MAX_TASK_AGE_HOURS", "5"))
MAX_AGE_SECONDS   = MAX_TASK_AGE_HOURS * 3600

STOP_REASON = (
    f"blundriq_ecs_watchdog: task exceeded {MAX_TASK_AGE_HOURS:.1f}h ceiling — "
    "likely hung. worker.py SIGALRM timeout did not fire cleanly."
)


def handler(event, context):
    now = datetime.now(timezone.utc)
    print(f"[watchdog] Starting. Cluster={CLUSTER} MAX_AGE={MAX_TASK_AGE_HOURS:.1f}h UTC={now.isoformat()}")

    # --- list all running task ARNs ---
    task_arns = []
    paginator = ecs.get_paginator("list_tasks")
    for page in paginator.paginate(cluster=CLUSTER, desiredStatus="RUNNING"):
        task_arns.extend(page.get("taskArns", []))

    if not task_arns:
        print("[watchdog] No running tasks found — nothing to do.")
        return

    print(f"[watchdog] Found {len(task_arns)} running task(s) — checking ages.")

    # describe_tasks accepts max 100 ARNs per call; fine for our scale
    response = ecs.describe_tasks(cluster=CLUSTER, tasks=task_arns)

    stopped_count = 0
    for task in response.get("tasks", []):
        task_arn    = task["taskArn"]
        short_arn   = task_arn.split("/")[-1]
        created_at  = task.get("createdAt")
        last_status = task.get("lastStatus", "UNKNOWN")

        if created_at is None:
            print(f"[watchdog] Task {short_arn}: no createdAt — skipping.")
            continue

        age_seconds = (now - created_at).total_seconds()
        age_hours   = age_seconds / 3600

        # Pull job context from container environment for clearer log messages
        job_label = _extract_job_label(task)

        print(
            f"[watchdog] Task {short_arn} ({job_label}): "
            f"age={age_hours:.2f}h status={last_status}"
        )

        if age_seconds > MAX_AGE_SECONDS:
            print(
                f"[watchdog] STOPPING task {short_arn} ({job_label}): "
                f"age {age_hours:.2f}h exceeds {MAX_TASK_AGE_HOURS:.1f}h ceiling."
            )
            try:
                ecs.stop_task(
                    cluster=CLUSTER,
                    task=task_arn,
                    reason=STOP_REASON,
                )
                stopped_count += 1
                print(f"[watchdog] Successfully stopped {short_arn}.")
            except Exception as e:
                # Log and continue — don't let one failure abort the whole run
                print(f"[watchdog] ERROR stopping task {short_arn}: {e}")

    print(
        f"[watchdog] Done. Checked {len(task_arns)} task(s), "
        f"stopped {stopped_count}."
    )


def _extract_job_label(task: dict) -> str:
    """
    Pull JOB_TYPE and PLAYER_ID from the container environment override so log
    lines identify what was actually running, not just the task ARN.
    Returns a human-readable string like 'deep_pass/player_id=1', or 'unknown'
    if the env vars aren't present (e.g. manually launched tasks).
    """
    try:
        overrides = task.get("overrides", {})
        for container in overrides.get("containerOverrides", []):
            env = {e["name"]: e["value"] for e in container.get("environment", [])}
            job_type  = env.get("JOB_TYPE", "")
            player_id = env.get("PLAYER_ID", "")
            if job_type:
                return f"{job_type}/player_id={player_id}" if player_id else job_type
    except Exception:
        pass
    return "unknown"
