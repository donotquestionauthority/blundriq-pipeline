"""
blundriq_sqs_trigger.py — Lambda function that reads SQS messages and launches
Fargate ECS tasks for BlundrIQ analysis jobs.

Replaces the broken EventBridge Pipe approach. Triggered by SQS event source
mappings on both blundriq-fast-pass and blundriq-deep-pass queues.

Message format:
    {"job_type": "onboarding_pass", "player_id": 1}
    {"job_type": "fast_pass", "player_id": 1}
    {"job_type": "deep_pass", "player_id": 1}

Environment variables (set on the Lambda function):
    ECS_CLUSTER         — blundriq
    ECS_TASK_DEFINITION — blundriq-pipeline:4
    ECS_SUBNET          — subnet-0856d384b229087e0
    ECS_CONTAINER_NAME  — pipeline
"""

import json
import os
import boto3

ecs = boto3.client("ecs", region_name="us-east-1")

CLUSTER         = os.environ["ECS_CLUSTER"]
TASK_DEFINITION = os.environ["ECS_TASK_DEFINITION"]
SUBNET          = os.environ["ECS_SUBNET"]
CONTAINER_NAME  = os.environ["ECS_CONTAINER_NAME"]


def handler(event, context):
    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[ERROR] Failed to parse message body: {e} | body: {record.get('body')}")
            raise  # Re-raise so SQS retries the message

        job_type  = body.get("job_type")
        player_id = body.get("player_id")

        if job_type not in ("onboarding_pass", "fast_pass", "deep_pass"):
            print(f"[ERROR] Unknown job_type: {job_type!r} — dropping message")
            return  # Don't retry — message is malformed

        if not isinstance(player_id, int):
            print(f"[ERROR] Invalid player_id: {player_id!r} — dropping message")
            return

        print(f"[INFO] Launching {job_type} for player_id={player_id}")

        response = ecs.run_task(
            cluster=CLUSTER,
            taskDefinition=TASK_DEFINITION,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [SUBNET],
                    "assignPublicIp": "ENABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": CONTAINER_NAME,
                        "environment": [
                            {"name": "JOB_TYPE",   "value": job_type},
                            {"name": "PLAYER_ID",  "value": str(player_id)},
                        ],
                    }
                ]
            },
        )

        failures = response.get("failures", [])
        if failures:
            print(f"[ERROR] ECS run_task failures: {failures}")
            raise RuntimeError(f"ECS run_task failed: {failures}")

        task_arn = response["tasks"][0]["taskArn"]
        print(f"[INFO] Launched task {task_arn} for {job_type} player_id={player_id}")
