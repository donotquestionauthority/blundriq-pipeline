import os

# ─── Supabase ───────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

# ─── Pipeline settings ──────────────────────────────────────────────────────
# How many months back to look for games on first import
INITIAL_IMPORT_MONTHS = 3

# Opening/middlegame/endgame phase boundaries (in plies)
OPENING_PLY_LIMIT    = 20
ENDGAME_MATERIAL     = 13  # total pieces remaining (excl. kings/pawns)

# ─── Chessable book IDs and their colors ────────────────────────────────────
# Add or remove books here when your repertoire changes
# Format: (bid, color)
CHESSABLE_BOOKS = [
    (170533, "black"),   # Scandinavian
    (90688,  "white"),   # Giri 1.e4 Part 1
    (192302, "black"),   # Slav
    (39554,  "black"),   # Black vs English/Réti
    (126535, "white"),   # Giri 1.e4 Part 2
    (159903, "white"),   # Giri 1.e4 Part 3 (inactive)
]

# ─── Stockfish ───────────────────────────────────────────────────────────────
STOCKFISH_DEPTH   = 18
STOCKFISH_VERSION = "stockfish_18"   # written to blunders.engine_version and games.analysis_engine

# ─── Analysis window ─────────────────────────────────────────────────────────
# Only the most recent N games per player are analyzed and stored.
# Games outside this window have their blunders deleted and analysis columns nullified.
ANALYSIS_GAME_LIMIT = 500

# Fast pass depth — used for two-phase onboarding (quick first pass)
FAST_PASS_DEPTH = 12

# ─── Centipawn loss thresholds ────────────────────────────────────────────────
# Industry-aligned thresholds (validated against 200-game dataset, Apr 2026).
# Prior values (25/50/100/200) over-classified by ~29%.
INACCURACY_THRESHOLD = 50
MISTAKE_THRESHOLD    = 100
BLUNDER_THRESHOLD    = 200
MISS_THRESHOLD       = 300

# Miss contested gate: a miss only fires if the position was roughly balanced
# before the error. If the player was already losing/winning by more than this,
# the large cp drop is noise from a decided game, not a meaningful mistake.
# Value is in centipawns from the player's perspective (absolute).
MISS_CONTESTED_GATE = 300

# UI display cap: blunders with cp_loss above this are mate-score contamination
# from decided games. Filter on the blunders API endpoint, not at storage time
# (we want the raw data in the DB).
MAX_CP_DISPLAY = 500

# ─── Priority scoring weights for blunder clustering ─────────────────────────
CLASSIFICATION_WEIGHTS = {
    'miss':       8,
    'blunder':    4,
    'mistake':    2,
    'inaccuracy': 1,
}

# Minimum number of moves that must match before accepting a repertoire match
MIN_MATCH_PLY = 6
