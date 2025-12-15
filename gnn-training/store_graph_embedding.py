#!/usr/bin/env python
"""
Store GraphSAGE customer embeddings into pgvector
"""

import json
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path

# -------------------------------------------------
# Config
# -------------------------------------------------

EMB_PATH = Path("gnn-training/graphsage/customer_embeddings.npy")
IDS_PATH = Path("gnn-training/graphsage/customer_embedding_ids.json")

PG_DSN = "postgresql://er:er@localhost:5432/er"
BATCH_SIZE = 500

# -------------------------------------------------
# Load data
# -------------------------------------------------

print("[pgvector] Loading GraphSAGE embeddings...")

embeddings = np.load(EMB_PATH).astype(np.float32)
customer_ids = json.loads(IDS_PATH.read_text())

assert embeddings.shape[0] == len(customer_ids)
assert embeddings.shape[1] == 128

rows = [
    (customer_ids[i], embeddings[i].tolist())
    for i in range(len(customer_ids))
]

print(f"[pgvector] Rows to insert: {len(rows)}")

# -------------------------------------------------
# Insert
# -------------------------------------------------

conn = psycopg2.connect(PG_DSN)
cur = conn.cursor()

sql = """
INSERT INTO customer_graph_embeddings (customer_id, embedding)
VALUES (%s, %s)
ON CONFLICT (customer_id)
DO UPDATE SET embedding = EXCLUDED.embedding;
"""

print("[pgvector] Inserting embeddings...")
execute_batch(cur, sql, rows, page_size=BATCH_SIZE)

conn.commit()
cur.close()
conn.close()

print("[pgvector] Done.")
