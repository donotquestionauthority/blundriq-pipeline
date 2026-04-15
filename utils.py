from datetime import datetime
import chess

def ts() -> str:
    """Returns current time as a string for log messages."""
    return datetime.now().strftime("%H:%M:%S")


def moves_to_fen_sequence(moves: list) -> list:
    """Convert a list of SAN moves to a list of FEN strings.
    Handles transpositions correctly since FEN captures board state.
    """
    board = chess.Board()
    fens = [board.fen()]  # include starting position
    for san in moves:
        try:
            move = board.parse_san(san)
            board.push(move)
            fens.append(board.fen())
        except Exception:
            break
    return fens

from datetime import timezone, timedelta

def get_date_filter(selection: str):
    """Convert a date filter label to a UTC datetime cutoff, or None for All time."""
    now = datetime.now(timezone.utc)
    days_map = {
        "Last 7 days":  7,
        "Last 10 days": 10,
        "Last 20 days": 20,
        "Last 30 days": 30,
        "Last 60 days": 60,
        "Last 90 days": 90,
    }
    if selection in days_map:
        return now - timedelta(days=days_map[selection])
    return None


def moves_to_pgn(moves: list, ply: int) -> str:
    """Convert a move list to a PGN string up to the given ply."""
    pgn_parts = []
    for i, san in enumerate(moves[:ply]):
        if i % 2 == 0:
            pgn_parts.append(f"{i // 2 + 1}. {san}")
        else:
            pgn_parts.append(san)
    return " ".join(pgn_parts)


# ─── Chess board / URL helpers ───────────────────────────────────────────────

import chess
import chess.svg

def pgn_to_lichess_url(pgn: str, color: str = "white") -> str:
    """Convert a PGN string to a Lichess analysis URL."""
    url = "https://lichess.org/analysis/pgn/" + pgn.replace(" ", "_")
    if color == "black":
        url += "?color=black"
    return url


def fen_to_chessable_url(fen: str) -> str:
    """Convert a FEN to a Chessable courses search URL."""
    return "https://www.chessable.com/courses/fen/" + fen.replace(" ", "%20")


def get_opp_last_move_squares(moves: list, ply: int):
    """Return (from_sq, to_sq) ints for opponent's last move before ply, or (None, None)."""
    if not moves or ply < 1 or len(moves) < ply:
        return None, None
    try:
        board = chess.Board()
        for i, san in enumerate(moves[:ply]):
            move = board.parse_san(san)
            if i == ply - 1:
                return move.from_square, move.to_square
            board.push(move)
    except Exception:
        pass
    return None, None


def get_fen_at_ply(moves: list, ply: int) -> str:
    """Return the FEN string after the given number of plies."""
    board = chess.Board()
    for i, san in enumerate(moves):
        if i >= ply:
            break
        try:
            move = board.parse_san(san)
            board.push(move)
        except Exception:
            break
    return board.fen()


def render_board_svg(fen: str, move_played: str, best_move: str,
                     opp_from: int, opp_to: int,
                     flipped: bool, size: int = 280) -> str:
    """
    Render a chess board as SVG with arrows.
    opp_from/opp_to are square ints (hashable for caching).
    Returns empty string on error.
    """
    try:
        board  = chess.Board(fen)
        arrows = []
        if opp_from is not None and opp_to is not None:
            arrows.append(chess.svg.Arrow(opp_from, opp_to, color="#4488ff"))
        if move_played:
            try:
                m = board.parse_san(move_played)
                arrows.append(chess.svg.Arrow(m.from_square, m.to_square, color="#cc0000"))
            except Exception:
                pass
        if best_move:
            try:
                m = board.parse_san(best_move)
                arrows.append(chess.svg.Arrow(m.from_square, m.to_square, color="#00aa00"))
            except Exception:
                pass
        return chess.svg.board(
            chess.Board(fen),
            arrows=arrows,
            size=size,
            coordinates=True,
            flipped=flipped
        )
    except Exception:
        return ""