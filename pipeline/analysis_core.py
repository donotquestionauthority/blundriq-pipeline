"""
pipeline/analysis_core.py — Shared Stockfish analysis logic.

Single source of truth for classification, phase detection, PV capture,
and per-move analysis. Imported by fast_pass.py, deep_pass.py, and
analyze_blunders.py — the only delta between those callers is the
Stockfish depth used (fast_pass_depth vs stockfish_depth).

Classification rules:
  - Miss:   cp_loss >= miss_threshold AND position was contested
            (abs(player_eval) <= miss_contested_gate). If the position was
            already decided, the miss label is suppressed AND the move is
            dropped entirely — a large loss from a hopeless position is noise,
            not a pattern worth studying.
  - Blunder: cp_loss >= blunder_threshold
  - Mistake: cp_loss >= mistake_threshold
  - Inaccuracy: cp_loss >= inaccuracy_threshold
  - None:   below all thresholds, or miss suppressed due to decided position
"""

import chess
import chess.engine


def classify(cp_loss: int, eval_before_white: int, player_color: str,
             settings: dict) -> str | None:
    """
    Classify a move error by centipawn loss.

    Miss requires the position to have been contested (within miss_contested_gate
    from the player's perspective). If the position was already decided, the move
    is dropped entirely — a blunder from a hopeless position is noise, not a
    recurring pattern worth surfacing in the app.

    Args:
        cp_loss:           centipawn loss from the player's perspective (>= 0)
        eval_before_white: Stockfish eval before the move, from White's POV
        player_color:      'white' or 'black'
        settings:          dict from get_app_settings()
    """
    player_eval = eval_before_white if player_color == "white" else -eval_before_white

    if cp_loss >= settings["miss_threshold"]:
        if abs(player_eval) <= settings["miss_contested_gate"]:
            return "miss"
        # Position already decided — suppress entirely, not worth surfacing
        return None

    if cp_loss >= settings["blunder_threshold"]:
        return "blunder"
    if cp_loss >= settings["mistake_threshold"]:
        return "mistake"
    if cp_loss >= settings["inaccuracy_threshold"]:
        return "inaccuracy"
    return None


def get_phase(ply: int, board: chess.Board) -> str:
    """Classify game phase by ply and remaining pieces."""
    if ply < 20:
        return "opening"
    pieces = sum(
        len(board.pieces(pt, color))
        for color in chess.COLORS
        for pt in [chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT]
    )
    return "endgame" if pieces <= 6 else "middlegame"


def capture_pv_san(board: chess.Board, pv_moves: list, n: int = 5) -> str | None:
    """
    Convert the first N moves of a Stockfish PV into a SAN string.
    Returns None if the PV is empty. Used to populate best_line for AI explanations.
    """
    san_list = []
    b = board.copy()
    for move in pv_moves[:n]:
        try:
            san_list.append(b.san(move))
            b.push(move)
        except Exception:
            break
    return " ".join(san_list) if san_list else None



def analyze_game_full(engine: chess.engine.SimpleEngine, game: dict,
                      player_color: str, settings: dict, depth: int) -> tuple[list, int | None]:
    """
    Single-pass analysis: computes both blunders and peak_advantage in one
    traversal of the game moves. Avoids running Stockfish twice per game.

    Returns:
        (blunders, peak_advantage)
        blunders:       list of blunder dicts (see analyze_game docstring)
        peak_advantage: int or None (see compute_peak_advantage docstring)
    """
    import json

    moves = game.get("moves", [])
    if isinstance(moves, str):
        moves = json.loads(moves)
    if not moves:
        return [], None

    threshold     = settings.get("lost_wins_peak_threshold", 300)
    sustained_req = settings.get("lost_wins_sustained_moves", 3)

    board         = chess.Board()
    blunders      = []
    streak        = 0
    streak_max    = 0
    overall_peak  = None

    for ply, san in enumerate(moves):
        try:
            move = board.parse_san(san)
        except Exception:
            break

        info_before   = engine.analyse(board, chess.engine.Limit(depth=depth))
        score_before  = info_before["score"].white().score(mate_score=10000)
        pv            = info_before.get("pv", [])
        best_move_obj = pv[0] if pv else None
        best_move_san = board.san(best_move_obj) if best_move_obj else None
        best_line     = capture_pv_san(board, pv, n=5)

        board.push(move)

        info_after  = engine.analyse(board, chess.engine.Limit(depth=depth))
        score_after = info_after["score"].white().score(mate_score=10000)

        if score_before is None or score_after is None:
            continue

        is_player_move = (
            (ply % 2 == 0 and player_color == "white") or
            (ply % 2 == 1 and player_color == "black")
        )

        if is_player_move:
            # ── Peak advantage tracking ────────────────────────────────────
            player_eval = score_before if player_color == "white" else -score_before
            if player_eval >= threshold:
                streak    += 1
                streak_max = max(streak_max, player_eval)
                if streak >= sustained_req:
                    if overall_peak is None or streak_max > overall_peak:
                        overall_peak = streak_max
            else:
                streak     = 0
                streak_max = 0

            # ── Blunder classification ─────────────────────────────────────
            cp_loss = (score_before - score_after) if player_color == "white" \
                      else (score_after - score_before)

            # If the move played was already the best move, it's not an error —
            # cp_loss reflects a bad position, not a bad decision.
            if best_move_san and san == best_move_san:
                continue

            cp             = max(0, cp_loss)
            classification = classify(cp, score_before, player_color, settings)
            if classification is None:
                continue

            board.pop()
            fen   = board.fen()
            phase = get_phase(ply, board)
            board.push(move)

            blunders.append({
                "ply":            ply,
                "phase":          phase,
                "fen":            fen,
                "move_played":    san,
                "best_move":      best_move_san,
                "best_line":      best_line,
                "centipawn_loss": cp,
                "classification": classification,
                "opening_eco":    game.get("opening_eco", ""),
            })

    return blunders, overall_peak


