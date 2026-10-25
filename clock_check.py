#!/usr/bin/env python3
"""
Clock comparability check. Must pass before any lag number is computed.

Both feeds carry a server-side epoch-ms timestamp (Binance: T trade time, E event time;
Polymarket: timestamp). The collector stamps every message with the *same* local clock
(recv_ns). So:
    offset_binance = recv_ms - T          offset_poly = recv_ms - timestamp
Each offset = (that server's clock error vs local) + (network/processing delay). Delay is
positive and jittery; clock error is a constant. We cannot fully separate the two, so the
reported bound is: |median(offset_poly) - median(offset_binance)| + half the p5..p95 spreads.
This bounds the *relative* error between the two server clocks as seen from one machine;
it cannot prove either clock is right in absolute terms. Keep the local clock NTP-synced
(chrony) so "local" is a sane reference.

Usage: python3 clock_check.py data/binance_YYYYMMDD.jsonl data/poly_YYYYMMDD.jsonl [max_bound_ms]
Default gate (Decision B): bound <= 1000 ms.
"""
import json
import statistics as st
import sys


def load(path):
    with open(path) as f:
        for line in f:
            yield json.loads(line)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def offsets_binance(path):
    offs, e_minus_t = [], []
    for rec in load(path):
        if "raw" not in rec:
            continue
        m = json.loads(rec["raw"])
        if "T" not in m:
            continue
        offs.append(rec["recv_ns"] / 1e6 - m["T"])
        e_minus_t.append(m["E"] - m["T"])
    return offs, e_minus_t


def offsets_poly(path):
    offs = []
    for rec in load(path):
        if "raw" not in rec:
            continue
        m = json.loads(rec["raw"])
        for x in (m if isinstance(m, list) else [m]):
            ts = x.get("timestamp") if isinstance(x, dict) else None
            if ts is None:
                continue
            offs.append(rec["recv_ns"] / 1e6 - int(ts))
    return offs


def _report(name, offs):
    if len(offs) < 50:
        print(f"{name}: only {len(offs)} timestamped messages; not enough to judge")
        return None
    med, p5, p95 = st.median(offs), pct(offs, 0.05), pct(offs, 0.95)
    print(f"{name}: n={len(offs)}  median={med:.0f} ms  p5={p5:.0f}  p95={p95:.0f}  (recv - server)")
    return med, p5, p95


def check(binance_paths, poly_paths, max_bound=1000.0):
    """Returns (ok: bool, bound_ms: float|None). Prints the full report."""
    b_offs, e_minus_t = [], []
    for p in binance_paths:
        o, e = offsets_binance(p)
        b_offs += o
        e_minus_t += e
    p_offs = []
    for p in poly_paths:
        p_offs += offsets_poly(p)

    b = _report("binance", b_offs)
    if e_minus_t:
        print(f"binance E-T (internal emit delay): median={st.median(e_minus_t):.0f} ms p95={pct(e_minus_t, .95):.0f}")
    p = _report("poly", p_offs)
    if not (b and p):
        print("VERDICT: NOT COMPARABLE (insufficient data). Do not compute lags.")
        return False, None

    rel = p[0] - b[0]
    half_spread = ((b[2] - b[1]) + (p[2] - p[1])) / 2
    bound = abs(rel) + half_spread
    print(f"relative offset poly-binance = {rel:.0f} ms; half-spread = {half_spread:.0f} ms; error bound = {bound:.0f} ms")
    negatives = sum(1 for o in b_offs if o < -50) + sum(1 for o in p_offs if o < -50)
    if negatives:
        print(f"WARNING: {negatives} messages received before their server timestamp (<-50 ms): a clock is off")
    if bound <= max_bound and not negatives:
        print(f"VERDICT: COMPARABLE within ~{bound:.0f} ms (gate {max_bound:.0f} ms).")
        return True, bound
    print(f"VERDICT: NOT COMPARABLE (bound {bound:.0f} ms > gate {max_bound:.0f} ms, or negative offsets). Do not compute lags.")
    return False, bound


if __name__ == "__main__":
    max_bound = float(sys.argv[3]) if len(sys.argv) > 3 else 1000.0
    ok, _ = check([sys.argv[1]], [sys.argv[2]], max_bound)
    sys.exit(0 if ok else 1)
