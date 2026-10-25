#!/usr/bin/env python3
"""
analyze.py — v1 responsiveness measurement. Separate from collection; re-runnable from raw.

WHAT V1 MEASURES (scope of claims, bake into every reading of the output):
  Responsiveness only: lag to first detectable response, quiet time, and the frequency of
  detectable vs non-detectable responses. It does NOT measure efficiency, correctness, or
  "full absorption". "No response" means exactly: no detectable response within 300 s at
  1-tick resolution on this contract. Sub-tick response, inattention, and already-priced
  information are indistinguishable in this data. No fair value, no delta, no model.

PRE-REGISTERED PARAMETERS (dated note; do not change after looking at results):
  Event feed   : Binance BTCUSDT aggTrade. 1-second bars of last trade price (forward-filled).
  Jump         : |r_1s| > 4 * std(previous 300 one-second returns, excluding current)
                 AND |r_1s| >= 0.10%. 60 s refractory after a jump. Needs 300 prior returns.
  T0           : Binance trade time T of the last trade in the triggering second.
  Target       : Polymarket YES-token mid = (best_bid + best_ask) / 2 from best_bid_ask,
                 price_change (best_bid/best_ask) and book messages. Server timestamp used.
  Baseline     : last mid with ts <= T0. Baseline age = T0 - baseline ts.
  Response     : first mid with ts > T0 and |mid - baseline| >= 1 tick (0.01).
  Quiet time   : first change ts C after response with no further change in (C, C+30 s],
                 reported as (C + 30 s) - T0. Requires data coverage to C + 30 s.
  Window       : 300 s. Beyond it: "no detectable response within 300s at 1-tick resolution".
  Clock gate   : error bound <= 1000 ms (clock_check). Lag/error ratio printed with each lag.

Usage:
  python3 analyze.py --yes-token <YES_TOKEN_ID> data/binance_*.jsonl data/poly_*.jsonl
Outputs: results/events.jsonl (one row per jump), stdout summary, and a hand-verification
printout of the raw records behind the first computed lag.
"""
import argparse
import glob
import json
import math
import os
import statistics as st
import sys

import clock_check

P = dict(
    SIGMA_MULT=4.0, ABS_FLOOR=0.0010, LOOKBACK=300, REFRACTORY_MS=60_000,
    TICK=0.01, WINDOW_MS=300_000, QUIET_MS=30_000, CLOCK_GATE_MS=1000.0,
)
NO_RESPONSE = "no detectable response within 300s at 1-tick resolution on this contract"


# ---------------------------------------------------------------- loading
def iter_lines(path):
    with open(path) as f:
        for i, line in enumerate(f, 1):
            yield i, json.loads(line)


def read_raw_lines(path, first, last):
    out = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            if first <= i <= last:
                out.append(line.rstrip("\n"))
            if i > last:
                break
    return out


def load_binance_bars(paths):
    """Per-second bars: dict sec -> [close, T_last, file, first_line, last_line, n_trades]."""
    bars = {}
    for path in paths:
        for lineno, rec in iter_lines(path):
            if "raw" not in rec:
                continue
            m = json.loads(rec["raw"])
            if "T" not in m:
                continue
            sec = m["T"] // 1000
            b = bars.get(sec)
            if b is None:
                bars[sec] = [float(m["p"]), m["T"], path, lineno, lineno, 1]
            else:
                if m["T"] >= b[1]:
                    b[0], b[1] = float(m["p"]), m["T"]
                b[4] = lineno
                b[5] += 1
    return bars


def load_poly_mids(paths, yes_token):
    """List of (ts_ms, bid, ask, file, lineno), sorted by ts. Also returns last recv_ms."""
    rows, last_recv_ms = [], 0
    for path in paths:
        for lineno, rec in iter_lines(path):
            if "raw" not in rec:
                continue
            last_recv_ms = max(last_recv_ms, rec["recv_ns"] / 1e6)
            m = json.loads(rec["raw"])
            for x in (m if isinstance(m, list) else [m]):
                if not isinstance(x, dict) or x.get("timestamp") is None:
                    continue
                ts, et = int(x["timestamp"]), x.get("event_type")
                bid = ask = None
                if et == "best_bid_ask" and x.get("asset_id") == yes_token:
                    bid, ask = x.get("best_bid"), x.get("best_ask")
                elif et == "price_change":
                    for pc in x.get("price_changes", []):
                        if pc.get("asset_id") == yes_token:
                            bid, ask = pc.get("best_bid"), pc.get("best_ask")
                elif et == "book" and x.get("asset_id") == yes_token:
                    bids = [float(l["price"]) for l in x.get("bids", [])]
                    asks = [float(l["price"]) for l in x.get("asks", [])]
                    bid = max(bids) if bids else None
                    ask = min(asks) if asks else None
                if bid in (None, "") or ask in (None, ""):
                    continue
                rows.append((ts, float(bid), float(ask), path, lineno))
    rows.sort(key=lambda r: r[0])
    return rows, last_recv_ms


def mid(r):
    return round((r[1] + r[2]) / 2, 4)


# ---------------------------------------------------------------- jumps
def detect_jumps(bars):
    """Returns list of dicts. Uses only pre-registered rules; nothing adaptive."""
    secs = sorted(bars)
    if not secs:
        return []
    # forward-fill to a continuous 1-second series
    series, last = [], None
    for s in range(secs[0], secs[-1] + 1):
        if s in bars:
            last = bars[s]
        series.append((s, last[0], s in bars))
    rets = []  # (sec, r, had_trades)
    for i in range(1, len(series)):
        p0, p1 = series[i - 1][1], series[i][1]
        rets.append((series[i][0], math.log(p1 / p0) if p0 > 0 else 0.0, series[i][2]))
    jumps, last_jump_ms = [], -10**18
    for i in range(P["LOOKBACK"], len(rets)):
        sec, r, had = rets[i]
        if not had:
            continue
        window = [x[1] for x in rets[i - P["LOOKBACK"]:i]]
        sigma = st.pstdev(window)
        if sigma == 0 or abs(r) <= P["SIGMA_MULT"] * sigma or abs(r) < P["ABS_FLOOR"]:
            continue
        b = bars[sec]
        if b[1] - last_jump_ms < P["REFRACTORY_MS"]:
            continue
        last_jump_ms = b[1]
        prev = bars[max(s for s in bars if s < sec)]
        jumps.append(dict(sec=sec, t0=b[1], ret=r, sigma=sigma, z=r / sigma,
                          prev_close=prev[0], close=b[0], n_trades=b[5],
                          file=b[2], first_line=b[3], last_line=b[4],
                          prev_file=prev[2], prev_last_line=prev[4]))
    return jumps


# ---------------------------------------------------------------- lag
def measure(jump, rows, data_end_ms, bound_ms):
    t0 = jump["t0"]
    base_i = None
    for i, r in enumerate(rows):
        if r[0] <= t0:
            base_i = i
        else:
            break
    ev = dict(t0=t0, jump_ret=jump["ret"], jump_z=jump["z"], jump_sigma=jump["sigma"],
              btc_prev_close=jump["prev_close"], btc_close=jump["close"], clock_bound_ms=bound_ms)
    if base_i is None:
        ev.update(status="no baseline (no target quote before T0)")
        return ev, None
    base = rows[base_i]
    bm = mid(base)
    ev.update(baseline_mid=bm, baseline_bid=base[1], baseline_ask=base[2],
              baseline_spread=round(base[2] - base[1], 4), baseline_age_ms=t0 - base[0],
              baseline_ref=f"{base[3]}:{base[4]}")
    resp_i = None
    for i in range(base_i + 1, len(rows)):
        r = rows[i]
        if r[0] - t0 > P["WINDOW_MS"]:
            break
        if abs(mid(r) - bm) >= P["TICK"] - 1e-9:
            resp_i = i
            break
    coverage_ok = data_end_ms >= t0 + P["WINDOW_MS"]
    if resp_i is None:
        ev.update(status=NO_RESPONSE if coverage_ok else "window not covered by data; undetermined")
        return ev, None
    r = rows[resp_i]
    lag = r[0] - t0
    ev.update(status="detectable response", lag_first_ms=lag,
              lag_over_clock_error=round(lag / bound_ms, 2) if bound_ms else None,
              response_mid=mid(r), response_bid=r[1], response_ask=r[2],
              response_spread=round(r[2] - r[1], 4), response_ticks=round((mid(r) - bm) / P["TICK"], 2),
              direction_match=(math.copysign(1, mid(r) - bm) == math.copysign(1, jump["ret"])),
              response_ref=f"{r[3]}:{r[4]}")
    # quiet time
    last_change_ts, cur = r[0], mid(r)
    quiet_ts = None
    for i in range(resp_i + 1, len(rows)):
        rr = rows[i]
        if rr[0] - last_change_ts >= P["QUIET_MS"]:
            quiet_ts = last_change_ts + P["QUIET_MS"]
            break
        if mid(rr) != cur:
            last_change_ts, cur = rr[0], mid(rr)
    if quiet_ts is None and data_end_ms >= last_change_ts + P["QUIET_MS"]:
        quiet_ts = last_change_ts + P["QUIET_MS"]
    if quiet_ts is not None:
        ev.update(quiet_time_ms=quiet_ts - t0, quiet_over_clock_error=round((quiet_ts - t0) / bound_ms, 2))
    else:
        ev.update(quiet_time_ms=None, quiet_note="quiet not reached within available data")
    return ev, resp_i


# ---------------------------------------------------------------- verification printout
def print_verification(jump, ev, rows, resp_i):
    print("\n=== HAND-VERIFICATION: raw records behind the first computed lag ===")
    print(f"Jump second {jump['sec']} (T0={jump['t0']}), r={jump['ret']:+.5f}, sigma={jump['sigma']:.6f}, z={jump['z']:+.2f}")
    print(f"Previous second close trade  {jump['prev_file']}:{jump['prev_last_line']}")
    for l in read_raw_lines(jump["prev_file"], jump["prev_last_line"], jump["prev_last_line"]):
        print("  ", l)
    print(f"Trades in triggering second  {jump['file']}:{jump['first_line']}-{jump['last_line']} ({jump['n_trades']} trades)")
    lines = read_raw_lines(jump["file"], jump["first_line"], jump["last_line"])
    for l in lines[:5]:
        print("  ", l)
    if len(lines) > 10:
        print(f"   ... {len(lines) - 10} more ...")
    for l in lines[-5:] if len(lines) > 5 else []:
        print("  ", l)
    print(f"Target baseline record       {ev['baseline_ref']}")
    f, n = ev["baseline_ref"].rsplit(":", 1)
    for l in read_raw_lines(f, int(n), int(n)):
        print("  ", l)
    if resp_i is not None:
        print(f"Target first-response record {ev['response_ref']}")
        f, n = ev["response_ref"].rsplit(":", 1)
        for l in read_raw_lines(f, int(n), int(n)):
            print("  ", l)
        print(f"Check: response ts - T0 = {ev['lag_first_ms']} ms; |mid - baseline| = {abs(ev['response_mid'] - ev['baseline_mid']):.4f} >= {P['TICK']}")
    else:
        print(f"Status: {ev['status']}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes-token", required=True)
    ap.add_argument("files", nargs="+")
    a = ap.parse_args()
    files = sorted(set(f for pat in a.files for f in glob.glob(pat)))
    bfiles = [f for f in files if os.path.basename(f).startswith("binance_")]
    pfiles = [f for f in files if os.path.basename(f).startswith("poly_")]
    if not bfiles or not pfiles:
        sys.exit("need at least one binance_*.jsonl and one poly_*.jsonl")

    print("PARAMETERS (pre-registered):", json.dumps(P))
    print("\n--- clock gate ---")
    ok, bound = clock_check.check(bfiles, pfiles, P["CLOCK_GATE_MS"])
    if not ok:
        sys.exit("Clock gate failed. No lag numbers produced.")

    bars = load_binance_bars(bfiles)
    rows, data_end_ms = load_poly_mids(pfiles, a.yes_token)
    print(f"\nbinance seconds with trades: {len(bars)}; target mid updates (YES token): {len(rows)}")
    if not rows:
        sys.exit("No YES-token quotes found. Check --yes-token matches the asset_id in poly files.")

    jumps = detect_jumps(bars)
    print(f"jumps detected: {len(jumps)}")
    os.makedirs("results", exist_ok=True)
    events, first_verified = [], None
    with open("results/events.jsonl", "w") as out:
        for j in jumps:
            ev, resp_i = measure(j, rows, data_end_ms, bound)
            events.append(ev)
            out.write(json.dumps(ev) + "\n")
            if first_verified is None and ev["status"] == "detectable response":
                first_verified = (j, ev, resp_i)

    print("\n--- per-event ---")
    for ev in events:
        if ev["status"] == "detectable response":
            print(f"T0={ev['t0']} z={ev['jump_z']:+.1f} lag_first={ev['lag_first_ms']} ms "
                  f"(lag/clock_err={ev['lag_over_clock_error']}) quiet={ev['quiet_time_ms']} ms "
                  f"base_age={ev['baseline_age_ms']} ms spread@base={ev['baseline_spread']} "
                  f"spread@resp={ev['response_spread']} ticks={ev['response_ticks']:+} dir_match={ev['direction_match']}")
        else:
            print(f"T0={ev['t0']} z={ev['jump_z']:+.1f} -> {ev['status']}")

    det = [e for e in events if e["status"] == "detectable response"]
    nod = [e for e in events if e["status"] == NO_RESPONSE]
    print("\n--- summary (responsiveness only; not efficiency, not absorption) ---")
    print(f"jumps: {len(events)}  detectable: {len(det)}  {NO_RESPONSE}: {len(nod)}  "
          f"other/undetermined: {len(events) - len(det) - len(nod)}")
    if det:
        lags = [e["lag_first_ms"] for e in det]
        print(f"lag to first detectable response: median={st.median(lags):.0f} ms  min={min(lags)}  max={max(lags)}  "
              f"(clock error bound {bound:.0f} ms)")
        qs = [e["quiet_time_ms"] for e in det if e.get("quiet_time_ms") is not None]
        if qs:
            print(f"quiet time: median={st.median(qs):.0f} ms (stillness, not absorption)")
    if first_verified:
        j, ev, resp_i = first_verified
        print_verification(j, ev, rows, resp_i)
    elif jumps:
        j = jumps[0]
        ev, _ = measure(j, rows, data_end_ms, bound)
        print_verification(j, ev, rows, None)
    print("\nresults/events.jsonl written.")


if __name__ == "__main__":
    main()
