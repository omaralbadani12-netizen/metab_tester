#!/usr/bin/env python3
"""
Raw collector. Records two websocket feeds verbatim, one JSONL line per message:
  {"recv_ns": <local time_ns at receipt>, "src": "binance"|"poly", "raw": "<message text>"}
Connection events are logged as {"recv_ns", "src", "event": "connect|disconnect|error", "info"}.
No parsing of prices, no detection, no logic. Analysis is a separate step.

Run:  POLY_ASSET_IDS=<yes_token_id>,<no_token_id> python3 collector.py
Deps: pip install websockets
Files: data/binance_YYYYMMDD.jsonl, data/poly_YYYYMMDD.jsonl (UTC day, rotated automatically)
"""
import asyncio
import datetime as dt
import json
import os
import sys
import time

import websockets

BINANCE_URL = "wss://data-stream.binance.vision/ws/btcusdt@aggTrade"
POLY_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OUT_DIR = os.environ.get("OUT_DIR", "data")
ASSET_IDS = [a.strip() for a in os.environ.get("POLY_ASSET_IDS", "").split(",") if a.strip()]


class Writer:
    """Append-only JSONL writer, rotates per UTC day, flushes every line."""

    def __init__(self, src):
        self.src, self.day, self.f = src, None, None

    def _file(self, recv_ns):
        day = dt.datetime.fromtimestamp(recv_ns / 1e9, dt.timezone.utc).strftime("%Y%m%d")
        if day != self.day:
            if self.f:
                self.f.close()
            os.makedirs(OUT_DIR, exist_ok=True)
            self.f = open(os.path.join(OUT_DIR, f"{self.src}_{day}.jsonl"), "a", buffering=1)
            self.day = day
        return self.f

    def write(self, raw):
        recv_ns = time.time_ns()
        self._file(recv_ns).write(json.dumps({"recv_ns": recv_ns, "src": self.src, "raw": raw}) + "\n")

    def log(self, event, info=""):
        recv_ns = time.time_ns()
        self._file(recv_ns).write(
            json.dumps({"recv_ns": recv_ns, "src": self.src, "event": event, "info": info}) + "\n")
        print(f"{dt.datetime.now(dt.timezone.utc).isoformat()} {self.src} {event} {info}", flush=True)


async def run_binance(w):
    backoff = 1
    while True:
        try:
            # Binance sends ping frames; the websockets library answers them automatically.
            # Connections are dropped by Binance after 24h; the loop reconnects.
            async with websockets.connect(BINANCE_URL, max_queue=4096) as ws:
                w.log("connect", BINANCE_URL)
                backoff = 1
                async for msg in ws:
                    w.write(msg)
        except Exception as e:
            w.log("error", repr(e))
        w.log("disconnect")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def run_poly(w):
    if not ASSET_IDS:
        print("POLY_ASSET_IDS is empty; set it to the token id(s) from find_market.py", file=sys.stderr)
        sys.exit(2)
    sub = json.dumps({"assets_ids": ASSET_IDS, "type": "market", "custom_feature_enabled": True})
    backoff = 1
    while True:
        try:
            async with websockets.connect(POLY_URL, max_queue=4096) as ws:
                await ws.send(sub)
                w.log("connect", sub)
                backoff = 1

                async def pinger():
                    while True:  # application-level heartbeat required by Polymarket
                        await asyncio.sleep(10)
                        await ws.send("PING")

                ping_task = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if msg == "PONG":
                            continue
                        w.write(msg)
                finally:
                    ping_task.cancel()
        except Exception as e:
            w.log("error", repr(e))
        w.log("disconnect")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def main():
    await asyncio.gather(run_binance(Writer("binance")), run_poly(Writer("poly")))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
