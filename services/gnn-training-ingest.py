#!/usr/bin/env python
"""
Emit blocking-constrained customer match candidates to Kafka
with CLEAN customer keys (no 'Customer:' prefix).

Source tables:
  - blocking_candidates
  - customer_graph_embeddings

Kafka topic:
  - customer.match_candidates
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

# --------------------------------------------------
# Kafka setup
# --------------------------------------------------

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 25,
    "acks": "all",
})

def delivery_report(err, msg):
    if err is not None:
        print(f"[kafka] ❌ Delivery failed: {err}")
    else:
        pass  # keep quiet on success for throughput

# --------------------------------------------------
# Database query (BLOCKING-CONSTRAINED ONLY)
# --------------------------------------------------

SQL = """
SELECT
  replace(bc.src_customer_id, 'Customer:', '') AS src_key,
  replace(bc.dst_customer_id, 'Customer:', '') AS dst_key,
  1 - (e1.embedding <=> e2.embedding) AS similarity,
  (e1.embedding <=> e2.embedding)     AS distance,
  bc.reason,
  bc.weight
FROM blocking_candidates bc
JOIN customer_graph_embeddings e1
  ON e1.customer_id = bc.src_customer_id
JOIN customer_graph_embeddings e2
  ON e2.customer_id = bc.dst_customer_id
WHERE (e1.embedding <=> e2.embedding) < %s
"""

def fetch_candidates() -> Iterator[dict]:
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(name="blocking_candidate_cursor")

    cur.execute(SQL, (DISTANCE_THRESHOLD,))

    for (
        src_key,
        dst_key,
        similarity,
        distance,
        reason,
        weight,
    ) in cur:
        yield {
            "src_customer_key": src_key,
            "dst_customer_key": dst_key,
            "similarity": float(similarity),
            "distance": float(distance),
            "blocking_reason": reason,
            "blocking_weight": float(weight),
        }

    cur.close()
    conn.close()

# --------------------------------------------------
# Emit to Kafka
# --------------------------------------------------

run_id = datetime.utcnow().isoformat()
count = 0

print("[emit] 🚀 Streaming blocking-constrained candidates to Kafka...")

for row in fetch_candidates():
    event = {
        "src_customer_key": row["src_customer_key"],
        "dst_customer_key": row["dst_customer_key"],
        "similarity": row["similarity"],
        "distance": row["distance"],
        "blocking_reason": row["blocking_reason"],
        "blocking_weight": row["blocking_weight"],
        "model": MODEL_NAME,
        "threshold": SIMILARITY_THRESHOLD,
        "run_id": run_id,
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=f"{row['src_customer_key']}|{row['dst_customer_key']}",
        value=json.dumps(event),
        on_delivery=delivery_report,
    )

    count += 1
    if count % 1000 == 0:
        producer.poll(0)

producer.flush()
print(f"[emit] ✅ Done. Total emitted: {count}")
