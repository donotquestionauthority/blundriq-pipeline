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

# Stockfish analysis depth
STOCKFISH_DEPTH = 18

# Centipawn loss thresholds
INACCURACY_THRESHOLD = 25
MISTAKE_THRESHOLD    = 50
BLUNDER_THRESHOLD    = 100
MISS_THRESHOLD       = 200

# Priority scoring weights for blunder clustering
CLASSIFICATION_WEIGHTS = {
    'miss':       8,
    'blunder':    4,
    'mistake':    2,
    'inaccuracy': 1,
}

# Minimum number of moves that must match before accepting a repertoire match
MIN_MATCH_PLY = 6