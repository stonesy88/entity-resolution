#!/usr/bin/env python
"""
Emit deduplicated blocking-constrained customer match candidates to Kafka.

Guarantees:
- ONE message per (src, dst, run_id)
- Strips 'Customer:' prefix
- Aggregates blocking reasons
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterator

import psycopg2
from confluent_kafka import Producer

# --------------------------------------------------
# Config
# --------------------------------------------------

PG_DSN = "postgresql://er:er@localhost:5432/er"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "customer.match_candidates"

SIMILARITY_THRESHOLD = 0.80
DISTANCE_THRESHOLD = 1 - SIMILARITY_THRESHOLD

MODEL_NAME = "graphsage-v1"
RUN_ID = datetime.utcnow().isoformat()

# --------------------------------------------------
# Kafka
# --------------------------------------------------

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 50,
    "acks": "all",
})

def delivery_report(err, msg):
    if err:
        print(f"[kafka] X Delivery failed: {err}")

# --------------------------------------------------
# Database query (CRITICAL PART)
# --------------------------------------------------

SQL = """
WITH scored AS (
  SELECT
    bc.src_customer_id,
    bc.dst_customer_id,
    ARRAY_AGG(DISTINCT bc.reason)            AS blocking_reasons,
    SUM(bc.weight)                           AS blocking_weight,
    MIN(e1.embedding <=> e2.embedding)       AS distance
  FROM blocking_candidates bc
  JOIN customer_graph_embeddings e1
    ON e1.customer_id = bc.src_customer_id
  JOIN customer_graph_embeddings e2
    ON e2.customer_id = bc.dst_customer_id
  WHERE (e1.embedding <=> e2.embedding) < %s
  GROUP BY bc.src_customer_id, bc.dst_customer_id
)
SELECT
  REPLACE(src_customer_id, 'Customer:', '') AS src_customer_key,
  REPLACE(dst_customer_id, 'Customer:', '') AS dst_customer_key,
  1 - distance                               AS similarity,
  distance,
  blocking_reasons,
  blocking_weight
FROM scored
"""

# --------------------------------------------------
# Streaming generator
# --------------------------------------------------

def fetch_candidates() -> Iterator[dict]:
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(name="match_cursor")

    cur.execute(SQL, (DISTANCE_THRESHOLD,))

    for (
        src,
        dst,
        similarity,
        distance,
        reasons,
        weight,
    ) in cur:
        yield {
            "src_customer_key": src,
            "dst_customer_key": dst,
            "similarity": float(similarity),
            "distance": float(distance),
            "blocking_reasons": reasons,
            "blocking_weight": float(weight),
        }

    cur.close()
    conn.close()

# --------------------------------------------------
# Emit
# --------------------------------------------------

print("[emit] Streaming deduplicated match candidates...")

count = 0

for row in fetch_candidates():
    event = {
        "src_customer_key": row["src_customer_key"],
        "dst_customer_key": row["dst_customer_key"],
        "similarity": row["similarity"],
        "distance": row["distance"],
        "blocking_reasons": row["blocking_reasons"],
        "blocking_weight": row["blocking_weight"],
        "model": MODEL_NAME,
        "threshold": SIMILARITY_THRESHOLD,
        "run_id": RUN_ID,
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=f"{row['src_customer_key']}|{row['dst_customer_key']}",
        value=json.dumps(event),
        on_delivery=delivery_report,
    )

    count += 1
    if count % 500 == 0:
        producer.poll(0)

producer.flush()
print(f"[emit] ✅ Done. Total emitted: {count}")
