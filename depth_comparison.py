"""
depth_comparison.py — BlundrIQ Stockfish Depth Comparison Test

Pulls the last N games for a player from DB, analyzes each at depths 12, 15, 18
WITHOUT writing to the database. Uses multiprocessing (default 8 workers) —
each worker owns its own Stockfish instance. Depths run sequentially; games
within each depth run in parallel.

Produces an HTML report answering:
  - Is a two-phase onboarding feasible? (fast shallow pass first, deep pass later)
  - What % of depth-18 issues does depth-12 actually catch?
  - How does the current classification system compare to industry-aligned thresholds?

Usage:
    python depth_comparison.py --player-id 1
    python depth_comparison.py --player-id 1 --game-limit 200 --workers 8
    python depth_comparison.py --player-id 1 --depths 12 18 --workers 16
    python depth_comparison.py --player-id 1 --output my_report.html
"""

import sys
import os
import time
import json
import argparse
import shutil
from collections import defaultdict
from datetime import datetime
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import chess
import chess.engine
from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from config import CLASSIFICATION_WEIGHTS

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

CURRENT_THRESHOLDS = {
    "inaccuracy": 25,
    "mistake":    50,
    "blunder":    100,
    "miss":       200,
}

# Industry-aligned thresholds + position gate for miss.
# Miss only fires if position was contested: |eval_before (player POV)| <= CONTESTED_GATE.
# Outside that range (already winning/losing by 3+ pawns) a large cp drop is noise.
PROPOSED_THRESHOLDS = {
    "inaccuracy": 50,
    "mistake":    100,
    "blunder":    200,
    "miss":       300,
}
CONTESTED_GATE = 300  # centipawns

WEIGHTS = CLASSIFICATION_WEIGHTS

# Module-level globals set by main() before Pool is created.
# Workers inherit them via fork (Linux/macOS).
_STOCKFISH_PATH = None
_DEPTH = None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_current(cp_loss):
    t = CURRENT_THRESHOLDS
    if cp_loss >= t["miss"]:        return "miss"
    if cp_loss >= t["blunder"]:     return "blunder"
    if cp_loss >= t["mistake"]:     return "mistake"
    if cp_loss >= t["inaccuracy"]:  return "inaccuracy"
    return None


def classify_proposed(cp_loss, eval_before_white, player_color):
    t           = PROPOSED_THRESHOLDS
    player_eval = eval_before_white if player_color == "white" else -eval_before_white
    if cp_loss >= t["miss"]:
        if abs(player_eval) <= CONTESTED_GATE:
            return "miss"
        # Fall through and reclassify as blunder/mistake/inaccuracy
    if cp_loss >= t["blunder"]:     return "blunder"
    if cp_loss >= t["mistake"]:     return "mistake"
    if cp_loss >= t["inaccuracy"]:  return "inaccuracy"
    return None


# ---------------------------------------------------------------------------
# Phase
# ---------------------------------------------------------------------------

def get_phase(ply, board):
    if ply < 20:
        return "opening"
    pieces = sum(
        len(board.pieces(pt, color))
        for color in chess.COLORS
        for pt in [chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT]
    )
    return "endgame" if pieces <= 6 else "middlegame"


# ---------------------------------------------------------------------------
# Stockfish path
# ---------------------------------------------------------------------------

def find_stockfish():
    candidates = [
        "/usr/local/bin/stockfish",
        "/opt/homebrew/bin/stockfish",
        "/usr/bin/stockfish",
        "/usr/games/stockfish",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    path = shutil.which("stockfish")
    if path:
        return path
    raise FileNotFoundError("Stockfish not found. Install: sudo apt install stockfish")


# ---------------------------------------------------------------------------
# Worker — runs in subprocess, owns its own Stockfish engine
# ---------------------------------------------------------------------------

def _analyze_worker(game_dict):
    """
    Multiprocessing worker. Each call spawns its own Stockfish process.
    Reads _STOCKFISH_PATH and _DEPTH from inherited module globals.
    Returns a fully serializable dict (no chess objects).
    """
    game_id      = game_dict["id"]
    player_color = game_dict["player_color"]
    moves        = game_dict["moves"]
    depth        = _DEPTH

    if isinstance(moves, str):
        moves = json.loads(moves)
    if not moves:
        return {"game_id": game_id, "blunders": [], "wall_time": 0.0,
                "move_count": 0, "success": True}

    t_start = time.time()
    engine  = None
    try:
        engine = chess.engine.SimpleEngine.popen_uci(_STOCKFISH_PATH)
        engine.configure({"Threads": 1})  # 1 thread per worker

        board    = chess.Board()
        blunders = []

        for ply, san in enumerate(moves):
            try:
                move = board.parse_san(san)
            except Exception:
                break

            info_before   = engine.analyse(board, chess.engine.Limit(depth=depth))
            score_before  = info_before["score"].white().score(mate_score=10000)
            best_move_obj = info_before.get("pv", [None])[0]
            best_move_san = board.san(best_move_obj) if best_move_obj else None

            # Capture PV as SAN (up to 6 moves) — stored for AI explanations later
            pv_san_list = []
            pv_board    = board.copy()
            for pv_move in info_before.get("pv", [])[:6]:
                try:
                    pv_san_list.append(pv_board.san(pv_move))
                    pv_board.push(pv_move)
                except Exception:
                    break
            best_line = " ".join(pv_san_list) if pv_san_list else None

            board.push(move)

            info_after  = engine.analyse(board, chess.engine.Limit(depth=depth))
            score_after = info_after["score"].white().score(mate_score=10000)

            if score_before is None or score_after is None:
                continue

            if player_color == "white":
                cp_loss = score_before - score_after
            else:
                cp_loss = score_after - score_before

            is_player_move = (
                (ply % 2 == 0 and player_color == "white") or
                (ply % 2 == 1 and player_color == "black")
            )
            if not is_player_move:
                continue

            # Best move played — cp_loss reflects bad position, not bad move
            if best_move_san and san == best_move_san:
                continue

            cp           = max(0, cp_loss)
            cls_current  = classify_current(cp)
            cls_proposed = classify_proposed(cp, score_before, player_color)

            if cls_current is None and cls_proposed is None:
                continue

            board.pop()
            fen   = board.fen()
            phase = get_phase(ply, board)
            board.push(move)

            blunders.append({
                "ply":               ply,
                "phase":             phase,
                "fen":               fen,
                "move_played":       san,
                "best_move":         best_move_san,
                "best_line":         best_line,
                "centipawn_loss":    cp,
                "eval_before_white": score_before,
                "cls_current":       cls_current,
                "cls_proposed":      cls_proposed,
                "game_id":           game_dict["id"],
                "game_url":          game_dict.get("url", ""),
                "opening_name":      game_dict.get("opening_name", ""),
                "played_at":         str(game_dict.get("played_at", "")),
            })

        engine.quit()
        return {
            "game_id":    game_id,
            "blunders":   blunders,
            "wall_time":  time.time() - t_start,
            "move_count": len(moves),
            "success":    True,
        }

    except Exception as e:
        try:
            if engine:
                engine.quit()
        except Exception:
            pass
        return {
            "game_id":    game_id,
            "blunders":   [],
            "wall_time":  time.time() - t_start,
            "move_count": 0,
            "success":    False,
            "error":      str(e),
        }


# ---------------------------------------------------------------------------
# Parallel depth run
# ---------------------------------------------------------------------------

def run_depth(game_dicts, depth, workers, sf_path):
    """
    Run all games at a given depth using a process pool.
    Returns dict with blunders list, timing, and failure count.
    """
    global _STOCKFISH_PATH, _DEPTH
    _STOCKFISH_PATH = sf_path
    _DEPTH          = depth

    all_blunders = []
    failures     = 0
    done         = 0
    total        = len(game_dicts)
    t_start      = time.time()

    print(f"\n  {'─'*54}")
    print(f"  Depth {depth}  |  {workers} workers")
    print(f"  {'─'*54}")

    with Pool(processes=workers) as pool:
        for result in pool.imap_unordered(_analyze_worker, game_dicts):
            done   += 1
            elapsed = time.time() - t_start
            avg     = elapsed / done
            eta     = avg * (total - done)

            if result["success"]:
                all_blunders.extend(result["blunders"])
                n = len(result["blunders"])
            else:
                failures += 1
                n         = 0
                print(f"\n  WARNING game {result['game_id']} failed: {result.get('error','?')}")

            print(
                f"  [{done:3d}/{total}] game {result['game_id']:6d} | "
                f"{result['wall_time']:.1f}s | {n} issues | ETA {eta:.0f}s      ",
                end="\r"
            )

    total_s  = time.time() - t_start
    n_issues = sum(1 for b in all_blunders if b["cls_current"] is not None)
    print(f"\n  Done — {total_s:.1f}s total | {n_issues} issues | {failures} failures")

    return {
        "blunders":   all_blunders,
        "total_s":    total_s,
        "per_game_s": total_s / total if total else 0,
        "failures":   failures,
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_top_positions(blunders, top_n=20):
    by_fen = defaultdict(list)
    for b in blunders:
        if b["cls_current"] is not None:
            by_fen[b["fen"]].append(b)

    positions = []
    for fen, occs in by_fen.items():
        score    = sum(WEIGHTS.get(b["cls_current"], 0) for b in occs)
        best_occ = max(occs, key=lambda b: WEIGHTS.get(b["cls_current"] or "", 0))
        positions.append({
            "fen":         fen,
            "score":       score,
            "count":       len(occs),
            "best_cls":    best_occ["cls_current"],
            "occurrences": occs,
        })

    positions.sort(key=lambda p: p["score"], reverse=True)
    return positions[:top_n]


def compare_depths(results_by_depth):
    depths         = sorted(results_by_depth.keys())
    baseline_depth = max(depths)

    indexed = {}
    for d in depths:
        indexed[d] = {}
        for b in results_by_depth[d]["blunders"]:
            key = (b["game_id"], b["ply"])
            indexed[d][key] = b

    rows = []
    for key, b_base in indexed[baseline_depth].items():
        row = {
            "game_id":     b_base["game_id"],
            "ply":         b_base["ply"],
            "fen":         b_base["fen"],
            "move_played": b_base["move_played"],
            "best_move":   b_base["best_move"],
            "game_url":    b_base["game_url"],
            "opening":     b_base["opening_name"],
        }
        for d in depths:
            b              = indexed[d].get(key)
            row[f"cp_{d}"]  = b["centipawn_loss"] if b else None
            row[f"cls_{d}"] = b["cls_current"]    if b else None
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# HTML Report
# ---------------------------------------------------------------------------

CLS_COLOR = {
    "miss":       "#f43f5e",
    "blunder":    "#ef4444",
    "mistake":    "#f97316",
    "inaccuracy": "#eab308",
    None:         "#6b7280",
}


def cls_badge(cls):
    color = CLS_COLOR.get(cls, "#6b7280")
    label = cls if cls else "—"
    return (f'<span style="background:{color};color:white;padding:2px 7px;'
            f'border-radius:4px;font-size:0.8em;font-weight:600">{label}</span>')


def cls_breakdown_html(blunders, proposed=False):
    key    = "cls_proposed" if proposed else "cls_current"
    counts = defaultdict(int)
    for b in blunders:
        if b[key]:
            counts[b[key]] += 1
    out = ""
    for cls in ["miss", "blunder", "mistake", "inaccuracy"]:
        n     = counts.get(cls, 0)
        color = CLS_COLOR[cls]
        out  += (f'<span style="background:{color};color:white;padding:2px 8px;'
                 f'border-radius:4px;margin:2px;display:inline-block">'
                 f'{cls}: {n}</span> ')
    return out


def build_report(player_name, game_count, workers, depths,
                 results_by_depth, top_positions_by_depth,
                 comparison_rows, output_path):
    now            = datetime.now().strftime("%Y-%m-%d %H:%M")
    baseline       = max(depths)
    total_compared = len(comparison_rows)

    # Timing rows
    timing_rows = ""
    for d in depths:
        r    = results_by_depth[d]
        n_c  = sum(1 for b in r["blunders"] if b["cls_current"] is not None)
        n_p  = sum(1 for b in r["blunders"] if b["cls_proposed"] is not None)
        n100 = r["per_game_s"] * 100
        timing_rows += f"""
        <tr>
          <td><strong>Depth {d}</strong></td>
          <td>{r['total_s']:.1f}s</td>
          <td>{r['per_game_s']:.2f}s</td>
          <td>{r['total_s']/60:.1f} min</td>
          <td>~{n100:.0f}s</td>
          <td>{n_c}</td>
          <td>{n_p} <span style="color:#888;font-size:0.85em">(proposed)</span></td>
          <td>{r['failures']}</td>
        </tr>"""

    # Catch rate rows
    catch_rows = ""
    for d in depths:
        r = results_by_depth[d]
        if d == baseline:
            pct   = 100.0
            label = "Baseline"
        else:
            found = sum(1 for row in comparison_rows if row.get(f"cls_{d}") is not None)
            pct   = found / total_compared * 100 if total_compared else 0
            label = "Fast pass candidate" if d <= 12 else ""
        catch_rows += f"""
        <tr>
          <td><strong>Depth {d}</strong></td>
          <td>{pct:.1f}%</td>
          <td>{r['total_s']:.1f}s ({r['total_s']/60:.1f} min)</td>
          <td>~{r['per_game_s']*100:.0f}s for 100 games</td>
          <td style="color:#94a3b8">{label}</td>
        </tr>"""

    # Breakdown rows
    breakdown_rows = ""
    for d in depths:
        blunders = results_by_depth[d]["blunders"]
        breakdown_rows += f"""
        <tr>
          <td><strong>Depth {d}</strong></td>
          <td>{cls_breakdown_html(blunders, False)}</td>
          <td>{cls_breakdown_html(blunders, True)}</td>
        </tr>"""

    # Reclassification
    base_blunders  = results_by_depth[baseline]["blunders"]
    reclass_counts = defaultdict(lambda: defaultdict(int))
    for b in base_blunders:
        if b["cls_current"] != b["cls_proposed"]:
            reclass_counts[b["cls_current"] or "none"][b["cls_proposed"] or "none"] += 1

    reclass_rows = ""
    for from_cls, to_dict in sorted(reclass_counts.items()):
        for to_cls, count in sorted(to_dict.items(), key=lambda x: -x[1]):
            reclass_rows += f"""
            <tr>
              <td>{cls_badge(from_cls if from_cls != 'none' else None)}</td>
              <td style="text-align:center;color:#64748b">→</td>
              <td>{cls_badge(to_cls if to_cls != 'none' else None)}</td>
              <td style="text-align:center">{count}</td>
            </tr>"""

    n_only_current  = sum(1 for b in base_blunders if b["cls_current"] and not b["cls_proposed"])
    n_only_proposed = sum(1 for b in base_blunders if not b["cls_current"] and b["cls_proposed"])
    n_both          = sum(1 for b in base_blunders if b["cls_current"] and b["cls_proposed"])

    # Top positions
    top_pos_rows = ""
    for i, pos in enumerate(top_positions_by_depth[baseline][:20], 1):
        occ  = pos["occurrences"][0]
        link = (f'<a href="{occ["game_url"]}" target="_blank">🔗</a>'
                if occ.get("game_url") else "")
        bl   = (occ.get("best_line") or "—")[:50]
        top_pos_rows += f"""
        <tr>
          <td style="color:#64748b">{i}</td>
          <td style="font-family:monospace;font-size:0.78em;color:#a3e635">{pos['fen'][:44]}…</td>
          <td>{cls_badge(pos['best_cls'])}</td>
          <td style="text-align:center"><strong>{pos['score']}</strong></td>
          <td style="text-align:center">{pos['count']}×</td>
          <td style="font-family:monospace;font-size:0.85em">{occ.get('move_played','?')} → {occ.get('best_move','?')}</td>
          <td style="font-family:monospace;font-size:0.8em;color:#94a3b8">{bl}</td>
          <td style="font-size:0.85em">{occ.get('opening_name','')[:30]}</td>
          <td>{link}</td>
        </tr>"""

    # Cross-depth comparison
    comp_header    = "".join(f"<th>CP-{d}</th><th>Cls-{d}</th>" for d in depths)
    comp_rows_sorted = sorted(
        comparison_rows, key=lambda r: r.get(f"cp_{baseline}") or 0, reverse=True
    )[:100]

    comp_rows_html = ""
    for r in comp_rows_sorted:
        cells = ""
        clses = []
        for d in depths:
            cp  = r.get(f"cp_{d}")
            cls = r.get(f"cls_{d}")
            clses.append(cls)
            cp_str  = str(cp) if cp is not None else '<span style="color:#ef4444;font-size:0.85em">MISSED</span>'
            cells  += f"<td style='text-align:center'>{cp_str}</td><td>{cls_badge(cls)}</td>"
        agree     = len(set(c for c in clses if c is not None)) <= 1
        row_style = "" if agree else 'style="background:rgba(239,68,68,0.07)"'
        flag      = "" if agree else "⚠️"
        link      = (f'<a href="{r["game_url"]}" target="_blank" style="color:#60a5fa">🔗</a>'
                     if r.get("game_url") else "")
        comp_rows_html += f"""
        <tr {row_style}>
          <td style="color:#64748b;font-size:0.8em">{r['game_id']}</td>
          <td style="text-align:center">{r['ply']}</td>
          <td style="font-family:monospace;font-size:0.85em">{r.get('move_played','?')}</td>
          <td style="font-family:monospace;font-size:0.85em">{r.get('best_move','?')}</td>
          {cells}
          <td>{flag}</td>
          <td>{link}</td>
        </tr>"""

    # Agreement %
    if total_compared > 0:
        n_agree   = sum(
            1 for r in comparison_rows
            if len(set(r.get(f"cls_{d}") for d in depths if r.get(f"cls_{d}") is not None)) <= 1
        )
        agree_pct = n_agree / total_compared * 100
    else:
        agree_pct = 0.0
    agree_color = "#22c55e" if agree_pct >= 80 else "#f97316"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BlundrIQ Depth Comparison — {player_name}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f1117; color: #e2e8f0; font-family: system-ui, sans-serif; padding: 24px; line-height: 1.5; }}
  h1 {{ color: #60a5fa; font-size: 1.6em; margin-bottom: 4px; }}
  h2 {{ color: #94a3b8; font-size: 1.05em; margin: 28px 0 10px; border-bottom: 1px solid #1e2a3a; padding-bottom: 6px; }}
  .meta {{ color: #64748b; font-size: 0.88em; margin-bottom: 20px; }}
  .callout {{ background: #1e2a3a; border-left: 4px solid #60a5fa; padding: 12px 16px; border-radius: 4px; margin: 10px 0 18px; font-size: 0.9em; }}
  .callout.warn {{ border-color: #f97316; }}
  .callout.good {{ border-color: #22c55e; }}
  .section {{ background: #0d1520; border: 1px solid #1e2a3a; border-radius: 8px; padding: 20px; margin-bottom: 24px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.87em; }}
  th {{ background: #1e2a3a; color: #94a3b8; padding: 8px 10px; text-align: left; font-weight: 600; white-space: nowrap; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #131e2e; vertical-align: middle; }}
  tr:hover td {{ background: rgba(96,165,250,0.04); }}
  a {{ color: #60a5fa; text-decoration: none; }}
  .scroll {{ overflow-x: auto; }}
</style>
</head>
<body>

<h1>BlundrIQ Depth Comparison Report</h1>
<p class="meta">
  Player: <strong>{player_name}</strong> &nbsp;|&nbsp;
  Games: <strong>{game_count}</strong> &nbsp;|&nbsp;
  Depths: <strong>{', '.join(str(d) for d in depths)}</strong> &nbsp;|&nbsp;
  Workers: <strong>{workers}</strong> &nbsp;|&nbsp;
  Generated: {now}
</p>
<div class="callout">
  Answers whether two-phase onboarding is viable — shallow analysis on signup so users see results immediately, full depth-18 runs in background. Also compares current vs proposed (industry-aligned) classification thresholds.
</div>

<div class="section">
  <h2>Timing Summary</h2>
  <table>
    <tr><th>Depth</th><th>Total</th><th>Per Game</th><th>Total (min)</th><th>Est. 100 games</th><th>Issues (current)</th><th>Issues (proposed)</th><th>Failures</th></tr>
    {timing_rows}
  </table>
</div>

<div class="section">
  <h2>Catch Rate vs Depth-{baseline} Baseline</h2>
  <table>
    <tr><th>Depth</th><th>Catch Rate</th><th>Wall Time ({game_count} games)</th><th>Est. 100 games</th><th>Role</th></tr>
    {catch_rows}
  </table>
  <div class="callout good" style="margin-top:12px">
    Classification agreement across all depths: <strong style="color:{agree_color}">{agree_pct:.1f}%</strong> of {total_compared} positions.
    High agreement at depth 12 = two-phase onboarding is viable.
  </div>
</div>

<div class="section">
  <h2>Classification Breakdown by Depth</h2>
  <table>
    <tr><th>Depth</th><th>Current Thresholds (miss&ge;200, blunder&ge;100, mistake&ge;50, inaccuracy&ge;25)</th><th>Proposed (industry-aligned, with position gate on miss)</th></tr>
    {breakdown_rows}
  </table>
</div>

<div class="section">
  <h2>Current &rarr; Proposed Reclassification (depth {baseline})</h2>
  <div class="callout warn">
    <strong>Current:</strong> miss&ge;200cp, blunder&ge;100cp, mistake&ge;50cp, inaccuracy&ge;25cp — no position context.<br>
    <strong>Proposed:</strong> miss&ge;300cp <em>only when |eval| &le; 300cp (contested)</em>; blunder&ge;200cp; mistake&ge;100cp; inaccuracy&ge;50cp.
  </div>
  <p style="font-size:0.9em;margin-bottom:12px">
    Both agree: <strong>{n_both}</strong> &nbsp;|&nbsp;
    Current only (dropped): <strong>{n_only_current}</strong> &nbsp;|&nbsp;
    Proposed only (new): <strong>{n_only_proposed}</strong>
  </p>
  <table style="max-width:380px">
    <tr><th>Current</th><th></th><th>Proposed</th><th>Count</th></tr>
    {reclass_rows or '<tr><td colspan="4" style="color:#64748b">Systems agree on all positions</td></tr>'}
  </table>
</div>

<div class="section">
  <h2>Top 20 Recurring Positions (depth {baseline}, current thresholds)</h2>
  <p style="color:#64748b;font-size:0.85em;margin-bottom:10px">Weighted score: miss&times;8 + blunder&times;4 + mistake&times;2 + inaccuracy&times;1</p>
  <div class="scroll">
  <table>
    <tr><th>#</th><th>FEN</th><th>Classification</th><th>Score</th><th>Count</th><th>Move &rarr; Best</th><th>Best Line (PV)</th><th>Opening</th><th></th></tr>
    {top_pos_rows or '<tr><td colspan="9" style="color:#64748b">No recurring positions found</td></tr>'}
  </table>
  </div>
</div>

<div class="section">
  <h2>Cross-Depth Comparison — Top 100 by CP Loss at Depth {baseline}</h2>
  <p style="color:#64748b;font-size:0.85em;margin-bottom:10px">
    &#9888; = classification disagrees across depths &nbsp;|&nbsp; MISSED = not detected at that depth
  </p>
  <div class="scroll">
  <table>
    <tr><th>Game ID</th><th>Ply</th><th>Played</th><th>Best</th>{comp_header}<th></th><th></th></tr>
    {comp_rows_html or '<tr><td colspan="12" style="color:#64748b">No data</td></tr>'}
  </table>
  </div>
</div>

<p style="color:#1e2a3a;font-size:0.8em;margin-top:16px">BlundrIQ depth_comparison.py — no database writes performed</p>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nReport written to: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BlundrIQ depth comparison (no DB writes)")
    parser.add_argument("--player-id",  type=int, required=True)
    parser.add_argument("--game-limit", type=int, default=200)
    parser.add_argument("--depths",     type=int, nargs="+", default=[12, 15, 18])
    parser.add_argument("--workers",    type=int, default=8)
    parser.add_argument("--output",     type=str, default="depth_comparison_report.html")
    args = parser.parse_args()

    depths  = sorted(set(args.depths))
    workers = args.workers

    print(f"\n{'='*60}")
    print(f"  BlundrIQ Depth Comparison")
    print(f"  Player ID : {args.player_id}")
    print(f"  Games     : {args.game_limit}")
    print(f"  Depths    : {depths}")
    print(f"  Workers   : {workers}")
    print(f"{'='*60}\n")

    print("Connecting to database...")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT u.display_name
            FROM players p JOIN users u ON u.id = p.user_id
            WHERE p.id = %s
        """, (args.player_id,))
        row         = cur.fetchone()
        player_name = row["display_name"] if row else f"Player {args.player_id}"

        cur.execute("""
            SELECT id, moves, player_color, url, opening_name, opening_eco, played_at
            FROM games
            WHERE player_id = %s
              AND moves IS NOT NULL
            ORDER BY played_at DESC
            LIMIT %s
        """, (args.player_id, args.game_limit))
        games = [dict(g) for g in cur.fetchall()]
    conn.close()

    if not games:
        print("No games found.")
        sys.exit(1)

    print(f"Loaded {len(games)} games for {player_name}")

    sf_path = find_stockfish()
    print(f"Stockfish: {sf_path}")

    # Run each depth sequentially; games within each depth run in parallel
    results_by_depth = {}
    for depth in depths:
        results_by_depth[depth] = run_depth(games, depth, workers, sf_path)

    top_positions_by_depth = {
        d: aggregate_top_positions(results_by_depth[d]["blunders"])
        for d in depths
    }
    comparison_rows = compare_depths(results_by_depth)

    # Console summary
    baseline = max(depths)
    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    for d in depths:
        r    = results_by_depth[d]
        n    = sum(1 for b in r["blunders"] if b["cls_current"] is not None)
        n100 = r["per_game_s"] * 100
        print(f"  Depth {d}: {r['total_s']:.1f}s | {n} issues | ~{n100:.0f}s for 100 games")
    if len(depths) > 1:
        total = len(comparison_rows)
        for d in sorted(depths)[:-1]:
            found = sum(1 for r in comparison_rows if r.get(f"cls_{d}") is not None)
            pct   = found / total * 100 if total else 0
            print(f"  Depth {d} catches {pct:.1f}% of depth-{baseline} issues")

    build_report(
        player_name            = player_name,
        game_count             = len(games),
        workers                = workers,
        depths                 = depths,
        results_by_depth       = results_by_depth,
        top_positions_by_depth = top_positions_by_depth,
        comparison_rows        = comparison_rows,
        output_path            = args.output,
    )


if __name__ == "__main__":
    main()
