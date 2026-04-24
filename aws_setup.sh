#!/bin/bash
# aws_setup.sh — One-time BlundrIQ Fargate infrastructure setup
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure) as blundriq-admin
#   - Docker installed and running
#   - .env file in blundriq-pipeline/ with DATABASE_URL, SUPABASE_URL, SUPABASE_KEY
#
# Run from blundriq-pipeline/ directory:
#   chmod +x aws_setup.sh && ./aws_setup.sh
#
# Takes ~15-20 minutes. Safe to re-run — all commands are idempotent.
#
# What this creates:
#   Secrets Manager (3) — DATABASE_URL, SUPABASE_URL, SUPABASE_KEY (~$0.12/mo each)
#   ECR repo            — stores Docker image (~$0.01/mo)
#   SQS queues (2)      — fast-pass and deep-pass job queues ($0)
#   IAM roles (3)       — exec role, task role, pipe role
#   IAM user (1)        — blundriq-render-api, send-only SQS for Render
#   ECS cluster         — blundriq ($0 at rest)
#   ECS task def        — blundriq-pipeline (4 vCPU / 8GB)
#   EventBridge Pipes   — SQS -> ECS trigger, one per queue ($0 at rest)
#
# Secrets are stored in Secrets Manager and injected at runtime — they never
# appear in the task definition, CloudWatch logs, or ECS console.
#
# Nothing here runs compute until the API enqueues a job.
# Set a $5 billing alert in AWS console after running this.

set -e

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION="us-east-1"
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO="blundriq-pipeline"
ECS_CLUSTER="blundriq"
TASK_FAMILY="blundriq-pipeline"
SQS_FAST="blundriq-fast-pass"
SQS_DEEP="blundriq-deep-pass"
TASK_ROLE="blundriq-pipeline-task"
EXEC_ROLE="blundriq-pipeline-exec"
PIPE_ROLE="blundriq-pipe"
API_USER="blundriq-render-api"

# Secrets Manager secret names
SECRET_DB="blundriq/DATABASE_URL"
SECRET_SUPA_URL="blundriq/SUPABASE_URL"
SECRET_SUPA_KEY="blundriq/SUPABASE_KEY"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "BlundrIQ Fargate Setup"
echo "Account: $AWS_ACCOUNT  Region: $AWS_REGION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Load .env ─────────────────────────────────────────────────────────────────
source .env 2>/dev/null || true
if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL not found in .env"
    echo "Add DATABASE_URL, SUPABASE_URL, SUPABASE_KEY to .env and re-run."
    exit 1
fi

# ── 1. Secrets Manager ────────────────────────────────────────────────────────
echo ""
echo "▶ 1/11  Secrets Manager"

store_secret() {
    local secret_name=$1
    local secret_value=$2

    if aws secretsmanager describe-secret --secret-id "$secret_name" \
        --region $AWS_REGION > /dev/null 2>&1; then
        # Secret exists — update value in case it changed
        aws secretsmanager put-secret-value \
            --secret-id "$secret_name" \
            --secret-string "$secret_value" \
            --region $AWS_REGION > /dev/null
        echo "   updated: $secret_name"
    else
        aws secretsmanager create-secret \
            --name "$secret_name" \
            --secret-string "$secret_value" \
            --region $AWS_REGION > /dev/null
        echo "   created: $secret_name"
    fi
}

store_secret "$SECRET_DB"       "$DATABASE_URL"
store_secret "$SECRET_SUPA_URL" "${SUPABASE_URL:-}"
store_secret "$SECRET_SUPA_KEY" "${SUPABASE_KEY:-}"

# Build ARNs for use in task definition and IAM policy
SECRET_DB_ARN="arn:aws:secretsmanager:$AWS_REGION:$AWS_ACCOUNT:secret:$SECRET_DB"
SECRET_SUPA_URL_ARN="arn:aws:secretsmanager:$AWS_REGION:$AWS_ACCOUNT:secret:$SECRET_SUPA_URL"
SECRET_SUPA_KEY_ARN="arn:aws:secretsmanager:$AWS_REGION:$AWS_ACCOUNT:secret:$SECRET_SUPA_KEY"

# ── 2. ECR repo ───────────────────────────────────────────────────────────────
echo ""
echo "▶ 2/11  ECR repository"
ECR_URI="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO"
aws ecr describe-repositories --repository-names $ECR_REPO --region $AWS_REGION \
    > /dev/null 2>&1 \
    || aws ecr create-repository \
        --repository-name $ECR_REPO \
        --region $AWS_REGION \
        --image-scanning-configuration scanOnPush=true \
        > /dev/null
echo "   $ECR_URI"

# ── 3. Build and push Docker image ────────────────────────────────────────────
echo ""
echo "▶ 3/11  Build + push Docker image (takes a few minutes)"
aws ecr get-login-password --region $AWS_REGION \
    | docker login --username AWS --password-stdin $ECR_URI
docker build -t $ECR_REPO:latest .
docker tag $ECR_REPO:latest $ECR_URI:latest
docker push $ECR_URI:latest
echo "   pushed: $ECR_URI:latest"

# ── 4. SQS queues ─────────────────────────────────────────────────────────────
echo ""
echo "▶ 4/11  SQS queues"

# VisibilityTimeout=21600 = 6 hours — long enough for a deep pass to complete
# without another worker picking up the same message
get_or_create_queue() {
    local name=$1
    local url
    url=$(aws sqs get-queue-url --queue-name "$name" --region $AWS_REGION \
        --query QueueUrl --output text 2>/dev/null) \
    || url=$(aws sqs create-queue \
        --queue-name "$name" \
        --region $AWS_REGION \
        --attributes VisibilityTimeout=21600,MessageRetentionPeriod=86400 \
        --query QueueUrl --output text)
    echo "$url"
}

FAST_QUEUE_URL=$(get_or_create_queue $SQS_FAST)
DEEP_QUEUE_URL=$(get_or_create_queue $SQS_DEEP)

FAST_QUEUE_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$FAST_QUEUE_URL" --attribute-names QueueArn \
    --region $AWS_REGION --query Attributes.QueueArn --output text)
DEEP_QUEUE_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$DEEP_QUEUE_URL" --attribute-names QueueArn \
    --region $AWS_REGION --query Attributes.QueueArn --output text)

echo "   fast-pass: $FAST_QUEUE_URL"
echo "   deep-pass: $DEEP_QUEUE_URL"

# ── 5. IAM — execution role ───────────────────────────────────────────────────
# Pulls ECR image, writes CloudWatch logs, reads Secrets Manager at task start
echo ""
echo "▶ 5/11  IAM execution role"
EXEC_ROLE_ARN="arn:aws:iam::$AWS_ACCOUNT:role/$EXEC_ROLE"
aws iam get-role --role-name $EXEC_ROLE > /dev/null 2>&1 || \
    aws iam create-role \
        --role-name $EXEC_ROLE \
        --assume-role-policy-document '{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow",
            "Principal":{"Service":"ecs-tasks.amazonaws.com"},
            "Action":"sts:AssumeRole"}]
        }' > /dev/null
aws iam attach-role-policy \
    --role-name $EXEC_ROLE \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy \
    2>/dev/null || true

# Grant execution role access to read the three secrets
# This is what allows ECS to inject secrets as env vars at container startup
aws iam put-role-policy \
    --role-name $EXEC_ROLE \
    --policy-name blundriq-secrets-read \
    --policy-document "{
        \"Version\":\"2012-10-17\",
        \"Statement\":[{
            \"Effect\":\"Allow\",
            \"Action\":[
                \"secretsmanager:GetSecretValue\"
            ],
            \"Resource\":[
                \"$SECRET_DB_ARN*\",
                \"$SECRET_SUPA_URL_ARN*\",
                \"$SECRET_SUPA_KEY_ARN*\"
            ]
        }]
    }" > /dev/null
echo "   $EXEC_ROLE"

# ── 6. IAM — task role (running container reads SQS) ─────────────────────────
echo ""
echo "▶ 6/11  IAM task role"
TASK_ROLE_ARN="arn:aws:iam::$AWS_ACCOUNT:role/$TASK_ROLE"
aws iam get-role --role-name $TASK_ROLE > /dev/null 2>&1 || \
    aws iam create-role \
        --role-name $TASK_ROLE \
        --assume-role-policy-document '{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow",
            "Principal":{"Service":"ecs-tasks.amazonaws.com"},
            "Action":"sts:AssumeRole"}]
        }' > /dev/null
aws iam put-role-policy \
    --role-name $TASK_ROLE \
    --policy-name blundriq-sqs-read \
    --policy-document "{
        \"Version\":\"2012-10-17\",
        \"Statement\":[{
            \"Effect\":\"Allow\",
            \"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",
                        \"sqs:GetQueueAttributes\"],
            \"Resource\":[\"$FAST_QUEUE_ARN\",\"$DEEP_QUEUE_ARN\"]
        }]
    }" > /dev/null
echo "   $TASK_ROLE"

# ── 7. IAM — pipe role (EventBridge reads SQS and launches ECS tasks) ─────────
echo ""
echo "▶ 7/11  IAM pipe role"
PIPE_ROLE_ARN="arn:aws:iam::$AWS_ACCOUNT:role/$PIPE_ROLE"
aws iam get-role --role-name $PIPE_ROLE > /dev/null 2>&1 || \
    aws iam create-role \
        --role-name $PIPE_ROLE \
        --assume-role-policy-document '{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow",
            "Principal":{"Service":"pipes.amazonaws.com"},
            "Action":"sts:AssumeRole"}]
        }' > /dev/null
aws iam put-role-policy \
    --role-name $PIPE_ROLE \
    --policy-name blundriq-pipe-permissions \
    --policy-document "{
        \"Version\":\"2012-10-17\",
        \"Statement\":[
            {
                \"Effect\":\"Allow\",
                \"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",
                            \"sqs:GetQueueAttributes\"],
                \"Resource\":[\"$FAST_QUEUE_ARN\",\"$DEEP_QUEUE_ARN\"]
            },
            {
                \"Effect\":\"Allow\",
                \"Action\":[\"ecs:RunTask\"],
                \"Resource\":\"arn:aws:ecs:$AWS_REGION:$AWS_ACCOUNT:task-definition/$TASK_FAMILY*\"
            },
            {
                \"Effect\":\"Allow\",
                \"Action\":[\"iam:PassRole\"],
                \"Resource\":[\"$EXEC_ROLE_ARN\",\"$TASK_ROLE_ARN\"]
            }
        ]
    }" > /dev/null
echo "   $PIPE_ROLE"

# ── 8. ECS cluster ────────────────────────────────────────────────────────────
echo ""
echo "▶ 8/11  ECS cluster"
CLUSTER_STATUS=$(aws ecs describe-clusters --clusters $ECS_CLUSTER \
    --region $AWS_REGION --query 'clusters[0].status' --output text 2>/dev/null)
if [ "$CLUSTER_STATUS" != "ACTIVE" ]; then
    aws ecs create-cluster \
        --cluster-name $ECS_CLUSTER \
        --region $AWS_REGION \
        --capacity-providers FARGATE \
        > /dev/null
fi
echo "   $ECS_CLUSTER"

# ── 9. VPC and subnet lookup ──────────────────────────────────────────────────
echo ""
echo "▶ 9/11  VPC and subnet lookup"
DEFAULT_VPC=$(aws ec2 describe-vpcs \
    --region $AWS_REGION \
    --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' --output text)

if [ -z "$DEFAULT_VPC" ] || [ "$DEFAULT_VPC" == "None" ]; then
    echo "   ERROR: No default VPC found in $AWS_REGION"
    echo "   Run: aws ec2 create-default-vpc --region $AWS_REGION"
    exit 1
fi

SUBNET_1=$(aws ec2 describe-subnets \
    --region $AWS_REGION \
    --filters Name=vpc-id,Values=$DEFAULT_VPC \
    --query 'Subnets[0].SubnetId' --output text)
SUBNET_2=$(aws ec2 describe-subnets \
    --region $AWS_REGION \
    --filters Name=vpc-id,Values=$DEFAULT_VPC \
    --query 'Subnets[1].SubnetId' --output text)

DEFAULT_SG=$(aws ec2 describe-security-groups \
    --region $AWS_REGION \
    --filters Name=vpc-id,Values=$DEFAULT_VPC Name=group-name,Values=default \
    --query 'SecurityGroups[0].GroupId' --output text)

echo "   VPC:            $DEFAULT_VPC"
echo "   Subnets:        $SUBNET_1, $SUBNET_2"
echo "   Security group: $DEFAULT_SG"

# ── 10. ECS task definition ───────────────────────────────────────────────────
# Secrets are injected via valueFrom — ECS fetches them from Secrets Manager
# at container startup. They appear as normal env vars inside the container
# but are never stored in the task definition or visible in the ECS console.
echo ""
echo "▶ 10/11  ECS task definition"
TASK_DEF_ARN=$(aws ecs register-task-definition \
    --family $TASK_FAMILY \
    --region $AWS_REGION \
    --requires-compatibilities FARGATE \
    --network-mode awsvpc \
    --cpu 4096 \
    --memory 8192 \
    --execution-role-arn "$EXEC_ROLE_ARN" \
    --task-role-arn "$TASK_ROLE_ARN" \
    --container-definitions "[
        {
            \"name\": \"pipeline\",
            \"image\": \"$ECR_URI:latest\",
            \"cpu\": 4096,
            \"memory\": 8192,
            \"essential\": true,
            \"environment\": [
                {\"name\": \"WORKERS\", \"value\": \"8\"}
            ],
            \"secrets\": [
                {\"name\": \"DATABASE_URL\",  \"valueFrom\": \"$SECRET_DB_ARN\"},
                {\"name\": \"SUPABASE_URL\",  \"valueFrom\": \"$SECRET_SUPA_URL_ARN\"},
                {\"name\": \"SUPABASE_KEY\",  \"valueFrom\": \"$SECRET_SUPA_KEY_ARN\"}
            ],
            \"logConfiguration\": {
                \"logDriver\": \"awslogs\",
                \"options\": {
                    \"awslogs-group\":         \"/ecs/blundriq-pipeline\",
                    \"awslogs-region\":        \"$AWS_REGION\",
                    \"awslogs-stream-prefix\": \"ecs\",
                    \"awslogs-create-group\":  \"true\"
                }
            }
        }
    ]" \
    --query 'taskDefinition.taskDefinitionArn' --output text)
echo "   $TASK_DEF_ARN"

# ── 11. EventBridge Pipes — SQS -> ECS ───────────────────────────────────────
echo ""
echo "▶ 11/11  EventBridge Pipes"

create_or_update_pipe() {
    local PIPE_NAME=$1
    local QUEUE_ARN=$2
    local JOB_TYPE=$3

    local TARGET_PARAMS
    TARGET_PARAMS=$(cat <<EOF
{
    "EcsTaskParameters": {
        "TaskDefinitionArn": "$TASK_DEF_ARN",
        "TaskCount": 1,
        "LaunchType": "FARGATE",
        "NetworkConfiguration": {
            "awsvpcConfiguration": {
                "Subnets": ["$SUBNET_1", "$SUBNET_2"],
                "SecurityGroups": ["$DEFAULT_SG"],
                "AssignPublicIp": "ENABLED"
            }
        },
        "Overrides": {
            "ContainerOverrides": [{
                "Name": "pipeline",
                "Environment": [
                    {"Name": "JOB_TYPE",  "Value": "$JOB_TYPE"},
                    {"Name": "PLAYER_ID", "Value": "<$.body.player_id>"}
                ]
            }]
        }
    }
}
EOF
)

    if aws pipes describe-pipe --name "$PIPE_NAME" --region $AWS_REGION \
        > /dev/null 2>&1; then
        aws pipes update-pipe \
            --name "$PIPE_NAME" \
            --region $AWS_REGION \
            --role-arn "$PIPE_ROLE_ARN" \
            --target "arn:aws:ecs:$AWS_REGION:$AWS_ACCOUNT:cluster/$ECS_CLUSTER" \
            --target-parameters "$TARGET_PARAMS" \
            > /dev/null
        echo "   updated: $PIPE_NAME"
    else
        aws pipes create-pipe \
            --name "$PIPE_NAME" \
            --region $AWS_REGION \
            --role-arn "$PIPE_ROLE_ARN" \
            --source "$QUEUE_ARN" \
            --source-parameters '{"SqsQueueParameters": {"BatchSize": 1}}' \
            --target "arn:aws:ecs:$AWS_REGION:$AWS_ACCOUNT:cluster/$ECS_CLUSTER" \
            --target-parameters "$TARGET_PARAMS" \
            > /dev/null
        echo "   created: $PIPE_NAME"
    fi
}

create_or_update_pipe "blundriq-fast-pass-pipe" "$FAST_QUEUE_ARN" "fast_pass"
create_or_update_pipe "blundriq-deep-pass-pipe" "$DEEP_QUEUE_ARN" "deep_pass"

# ── API IAM user — send-only SQS access for Render ────────────────────────────
echo ""
echo "▶ API IAM user"
aws iam get-user --user-name $API_USER > /dev/null 2>&1 || \
    aws iam create-user --user-name $API_USER > /dev/null
aws iam put-user-policy \
    --user-name $API_USER \
    --policy-name blundriq-sqs-send \
    --policy-document "{
        \"Version\":\"2012-10-17\",
        \"Statement\":[{
            \"Effect\":\"Allow\",
            \"Action\":[\"sqs:SendMessage\"],
            \"Resource\":[\"$FAST_QUEUE_ARN\",\"$DEEP_QUEUE_ARN\"]
        }]
    }" > /dev/null

KEY_COUNT=$(aws iam list-access-keys --user-name $API_USER \
    --query 'length(AccessKeyMetadata)' --output text)
if [ "$KEY_COUNT" == "0" ]; then
    echo ""
    echo "   ┌──────────────────────────────────────────────────────┐"
    echo "   │  NEW ACCESS KEY — save these now, shown only once    │"
    echo "   └──────────────────────────────────────────────────────┘"
    aws iam create-access-key --user-name $API_USER \
        --query 'AccessKey.[AccessKeyId,SecretAccessKey]' \
        --output table
else
    echo "   Access key already exists for $API_USER — skipping"
    echo "   To rotate: aws iam delete-access-key then re-run"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Setup complete."
echo ""
echo "Add these to Render env vars (blundriq-api):"
echo ""
echo "  AWS_REGION=$AWS_REGION"
echo "  SQS_FAST_PASS_URL=$FAST_QUEUE_URL"
echo "  SQS_DEEP_PASS_URL=$DEEP_QUEUE_URL"
echo "  AWS_ACCESS_KEY_ID=<from table above>"
echo "  AWS_SECRET_ACCESS_KEY=<from table above>"
echo ""
echo "Verify in AWS console before doing anything else:"
echo "  Secrets Manager -> blundriq/DATABASE_URL, SUPABASE_URL, SUPABASE_KEY"
echo "  ECS             -> clusters -> $ECS_CLUSTER (no running tasks)"
echo "  SQS             -> $SQS_FAST, $SQS_DEEP (both empty)"
echo "  Pipes           -> blundriq-fast-pass-pipe, blundriq-deep-pass-pipe (RUNNING)"
echo "  IAM users       -> $API_USER (one access key, send-only policy)"
echo "  Billing         -> Budgets -> create a \$5 alert"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
