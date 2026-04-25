# BlundrIQ — Fargate analysis container
#
# Installs Stockfish 18 (avx2 build) from the official GitHub release.
# Fargate runs on x86-64 Haswell+ hardware — avx2 is safe and faster than
# the compatibility build used in GH Actions.
#
# Build:
#   docker build -t blundriq-pipeline .
#
# Test locally (fast pass, player 1):
#   docker run --env-file .env \
#     -e JOB_TYPE=fast_pass \
#     -e PLAYER_ID=1 \
#     -e WORKERS=4 \
#     blundriq-pipeline
#
# Push to ECR (after aws ecr get-login-password):
#   docker tag blundriq-pipeline:latest <account>.dkr.ecr.us-east-1.amazonaws.com/blundriq-pipeline:latest
#   docker push <account>.dkr.ecr.us-east-1.amazonaws.com/blundriq-pipeline:latest

FROM --platform=linux/amd64 python:3.13-slim

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    wget \
    tar \
    && rm -rf /var/lib/apt/lists/*

# ── Stockfish 18 (avx2) ───────────────────────────────────────────────────────
# Official release from https://github.com/official-stockfish/Stockfish/releases
# avx2 binary requires Haswell (2013+) — all current Fargate hardware qualifies.
RUN wget -q https://github.com/official-stockfish/Stockfish/releases/download/sf_18/stockfish-ubuntu-x86-64-avx2.tar \
    && tar -xf stockfish-ubuntu-x86-64-avx2.tar \
    && mv stockfish/stockfish-ubuntu-x86-64-avx2 /usr/local/bin/stockfish \
    && chmod +x /usr/local/bin/stockfish \
    && rm -rf stockfish stockfish-ubuntu-x86-64-avx2.tar

# ── Python deps ───────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.fargate.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Pipeline code ─────────────────────────────────────────────────────────────
# Copy only what the pipeline needs — not research scripts, not UI, not API
COPY config.py           ./config.py
COPY db.py               ./db.py
COPY utils.py            ./utils.py
COPY fast_pass.py        ./fast_pass.py
COPY deep_pass.py        ./deep_pass.py
COPY onboarding_pass.py  ./onboarding_pass.py
COPY worker.py           ./worker.py
COPY pipeline/           ./pipeline/

# ── Runtime ───────────────────────────────────────────────────────────────────
# Environment variables required at runtime (injected by ECS task definition):
#   DATABASE_URL   — Supabase pooler connection string (port 6543)
#   SUPABASE_URL   — Supabase project URL
#   SUPABASE_KEY   — Supabase service role key
#   JOB_TYPE       — "fast_pass" or "deep_pass"
#   PLAYER_ID      — integer player ID
#   WORKERS        — optional, parallel worker count (default 8)

CMD ["python", "worker.py"]