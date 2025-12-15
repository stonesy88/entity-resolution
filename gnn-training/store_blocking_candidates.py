#!/usr/bin/env python
"""
Load BLOCKING_CANDIDATE edges from Quine edges.jsonl into Postgres.

Input:
  - gnn-training/outputs/edges.jsonl

Output table:
  - blocking_candidates(src_customer_id, dst_customer_id, reason, weight)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_batch

# --------------------------------------------------
# Config
# --------------------------------------------------

EDGES_PATH = Path("./gnn-training/outputs/edges.jsonl")

PG_DSN = "postgresql://er:er@localhost:5432/er"
BATCH_SIZE = 1000

# --------------------------------------------------
# Load edges
# --------------------------------------------------

print("[load] Reading edges.jsonl...")

rows: List[Tuple[str, str, str, float]] = []

with EDGES_PATH.open("r", encoding="utf-8") as f:
    for line in f:
        e = json.loads(line)

        if e.get("type") != "BLOCKING_CANDIDATE":
            continue

        src = e["src"]
        dst = e["dst"]

        # Enforce canonical ordering (defensive)
        if src > dst:
            src, dst = dst, src

        rows.append(
            (
                src,
                dst,
                e.get("reason"),
                float(e.get("weight", 1.0)),
            )
        )

print(f"[load] Blocking candidate edges found: {len(rows)}")

if not rows:
    raise SystemExit("No BLOCKING_CANDIDATE edges found — aborting.")

# --------------------------------------------------
# Insert into Postgres
# --------------------------------------------------

print("[load] Connecting to Postgres...")

conn = psycopg2.connect(PG_DSN)
conn.autocommit = False

SQL = """
INSERT INTO blocking_candidates (
  src_customer_id,
  dst_customer_id,
  reason,
  weight
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (src_customer_id, dst_customer_id) DO NOTHING
"""

with conn.cursor() as cur:
    execute_batch(cur, SQL, rows, page_size=BATCH_SIZE)

conn.commit()
conn.close()

print("[load] Done.")
print("[load] blocking_candidates table populated.")
