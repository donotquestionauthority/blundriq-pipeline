#!/bin/bash
# deploy_docker.sh — Build and push blundriq-pipeline image to ECR
#
# Run from: blundriq-pipeline/ repo root
# Requires: AWS CLI configured, Docker running, buildx available
#
# Usage:
#   cd /Users/roberttavoularis/Documents/Chess/Automation/blundriq/blundriq-pipeline
#   bash deploy_docker.sh

set -e  # Exit immediately on any error

ECR_ACCOUNT="376761749621"
ECR_REGION="us-east-1"
ECR_REPO="blundriq-pipeline"
ECR_URI="${ECR_ACCOUNT}.dkr.ecr.${ECR_REGION}.amazonaws.com/${ECR_REPO}"

echo "=== Logging in to ECR ==="
aws ecr get-login-password --region "${ECR_REGION}" \
    | docker login --username AWS --password-stdin "${ECR_ACCOUNT}.dkr.ecr.${ECR_REGION}.amazonaws.com"

echo "=== Building image (linux/amd64) ==="
docker buildx build \
    --platform linux/amd64 \
    --provenance=false \
    --no-cache \
    -t "${ECR_URI}:latest" \
    --push \
    .

echo "=== Done ==="
echo "Image pushed to: ${ECR_URI}:latest"
echo ""
echo "NOTE: The ECS task definition still points to :latest — no task def update needed."
echo "New Fargate tasks will pull the updated image automatically."
