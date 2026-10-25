#!/usr/bin/env python3
"""
Resolve a Polymarket URL slug to CLOB token ids (asset ids) for collector.py.
Usage: python3 find_market.py <slug>
The slug is the last path segment of the polymarket.com URL for the event or market.
"""
import json
import sys
import urllib.request

slug = sys.argv[1]
GAMMA = "https://gamma-api.polymarket.com"


def get(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)


markets = []
for ev in get(f"{GAMMA}/events?slug={slug}"):
    markets += ev.get("markets", [])
if not markets:
    markets = get(f"{GAMMA}/markets?slug={slug}")

for m in markets:
    tokens = m.get("clobTokenIds") or "[]"
    outcomes = m.get("outcomes") or "[]"
    tokens = json.loads(tokens) if isinstance(tokens, str) else tokens
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    print("question    :", m.get("question"))
    print("conditionId :", m.get("conditionId"))
    print("active/closed:", m.get("active"), "/", m.get("closed"))
    for o, t in zip(outcomes, tokens):
        print(f"  {o:>4} token: {t}")
    print("POLY_ASSET_IDS=" + ",".join(tokens))
    print()
