import os

# ─── Supabase ───────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

# ─── Pipeline settings ──────────────────────────────────────────────────────
# How many months back to look for games on first import
INITIAL_IMPORT_MONTHS = 3

# Opening/middlegame/endgame phase boundaries (in plies)
# Engineering constants — not exposed as admin settings.
OPENING_PLY_LIMIT = 20
# Endgame threshold: pieces <= 6 (queens + rooks + bishops + knights, excl. kings/pawns)
# Hardcoded in analysis_core.get_phase() — change there if adjusting.

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
# STOCKFISH_VERSION is tied to the Docker image — not a runtime setting.
STOCKFISH_VERSION = "stockfish_18"

# ─── Fallback constants ──────────────────────────────────────────────────────
# These are used ONLY as fallbacks when a value is absent from app_settings.
# Operational values are stored in app_settings and fetched via
# db.get_app_settings(conn) at runtime. Do not rely on these constants
# directly in analysis scripts — use the settings dict instead.

STOCKFISH_DEPTH      = 18
FAST_PASS_DEPTH      = 12

ANALYSIS_GAME_LIMIT  = 500   # emergency fallback; app_settings default is 1000
FREE_IMPORT_LIMIT    = 500   # games imported at onboarding for free users

INACCURACY_THRESHOLD = 50
MISTAKE_THRESHOLD    = 100
BLUNDER_THRESHOLD    = 200
MISS_THRESHOLD       = 300
MISS_CONTESTED_GATE  = 300
MAX_CP_DISPLAY       = 500

CLASSIFICATION_WEIGHTS = {
    'miss':       8,
    'blunder':    4,
    'mistake':    2,
    'inaccuracy': 1,
}

# Minimum number of moves that must match before accepting a repertoire match.
# Engineering constant — not exposed as admin setting.
MIN_MATCH_PLY = 6
