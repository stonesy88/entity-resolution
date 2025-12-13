import json
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch

EMBEDDINGS_PATH = "gnn-training/embeddings/customer_features.npy"
IDS_PATH = "gnn-training/embeddings/customer_ids.json"

DB_URL = "postgresql://er:er@localhost:5432/er"

print("[pgvector] Loading embeddings...")
X = np.load(EMBEDDINGS_PATH)
with open(IDS_PATH) as f:
    ids = json.load(f)

assert len(X) == len(ids)

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("[pgvector] Inserting embeddings...")

rows = [
    (cid, X[i].tolist())
    for i, cid in enumerate(ids)
]

execute_batch(
    cur,
    """
    INSERT INTO customer_embeddings (customer_id, embedding)
    VALUES (%s, %s)
    ON CONFLICT (customer_id)
    DO UPDATE SET embedding = EXCLUDED.embedding
    """,
    rows,
    page_size=500
)

conn.commit()
cur.close()
conn.close()

print(f"[pgvector] Stored {len(rows)} embeddings.")
