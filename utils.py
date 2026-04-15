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