#!/usr/bin/env python
"""
Step 2 — Build Graph Structure for GraphSAGE Training

Inputs:
  - gnn-training/outputs/edges.jsonl

Outputs:
  - gnn-training/graph/node_index.json
  - gnn-training/graph/edge_index.npy
  - gnn-training/graph/edge_weight.npy
  - gnn-training/graph/edge_type.json
  - gnn-training/graph/customer_mask.npy
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import numpy as np

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

EDGES_PATH = Path("./gnn-training/outputs/edges.jsonl")
OUT_DIR = Path("./gnn-training/graph")

OUT_NODE_INDEX = OUT_DIR / "node_index.json"
OUT_EDGE_INDEX = OUT_DIR / "edge_index.npy"
OUT_EDGE_WEIGHT = OUT_DIR / "edge_weight.npy"
OUT_EDGE_TYPE = OUT_DIR / "edge_type.json"
OUT_CUSTOMER_MASK = OUT_DIR / "customer_mask.npy"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# Edge weighting rules
# ---------------------------------------------------------

DEFAULT_EDGE_WEIGHT = 1.0
TRANSIT_EDGE_WEIGHT = 0.4

# ---------------------------------------------------------
# Load edges and collect nodes
# ---------------------------------------------------------

edges: List[dict] = []
node_ids = set()

with open(EDGES_PATH, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        e = json.loads(line)
        edges.append(e)
        node_ids.add(e["src"])
        node_ids.add(e["dst"])

# ---------------------------------------------------------
# Build node index
# ---------------------------------------------------------

node_id_to_idx: Dict[str, int] = {
    node_id: idx for idx, node_id in enumerate(sorted(node_ids))
}

with open(OUT_NODE_INDEX, "w", encoding="utf-8") as f:
    json.dump(node_id_to_idx, f, indent=2)

num_nodes = len(node_id_to_idx)
num_edges = len(edges)

# ---------------------------------------------------------
# Build edge tensors
# ---------------------------------------------------------

edge_index = np.zeros((2, num_edges), dtype=np.int64)
edge_weight = np.zeros((num_edges,), dtype=np.float32)
edge_type: List[dict] = []

for i, e in enumerate(edges):
    src_idx = node_id_to_idx[e["src"]]
    dst_idx = node_id_to_idx[e["dst"]]

    edge_index[0, i] = src_idx
    edge_index[1, i] = dst_idx

    # Weight logic
    if e.get("type") == "BLOCKING_CANDIDATE":
        edge_weight[i] = float(e.get("weight", DEFAULT_EDGE_WEIGHT))
    elif e.get("type", "").startswith("IN_") or e.get("type", "").startswith("HAS_"):
        edge_weight[i] = TRANSIT_EDGE_WEIGHT
    else:
        edge_weight[i] = DEFAULT_EDGE_WEIGHT

    edge_type.append(
        {
            "type": e.get("type"),
            "reason": e.get("reason"),
        }
    )

# ---------------------------------------------------------
# Customer mask
# ---------------------------------------------------------

customer_mask = np.zeros((num_nodes,), dtype=np.bool_)

for node_id, idx in node_id_to_idx.items():
    if node_id.startswith("Customer:"):
        customer_mask[idx] = True

# ---------------------------------------------------------
# Write outputs
# ---------------------------------------------------------

np.save(OUT_EDGE_INDEX, edge_index)
np.save(OUT_EDGE_WEIGHT, edge_weight)
np.save(OUT_CUSTOMER_MASK, customer_mask)

with open(OUT_EDGE_TYPE, "w", encoding="utf-8") as f:
    json.dump(edge_type, f, indent=2)

print("[graph] Done.")
print(f"[graph] Nodes: {num_nodes}")
print(f"[graph] Edges: {num_edges}")
print(f"[graph] Customer nodes: {customer_mask.sum()}")
