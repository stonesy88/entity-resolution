#!/usr/bin/env python
"""
Trains GraphSAGE on Quine-exported graph + with feature embeddings

Inputs:
  Step 1:
    - gnn-training/embeddings/customer_features.npy
    - gnn-training/embeddings/customer_ids.json
  Step 2:
    - gnn-training/graph/node_index.json
    - gnn-training/graph/edge_index.npy
    - gnn-training/graph/edge_weight.npy
    - gnn-training/graph/edge_type.json
    - gnn-training/graph/customer_mask.npy

Outputs:
  - gnn-training/graphsage/customer_embeddings.npy
  - gnn-training/graphsage/customer_embedding_ids.json
  - gnn-training/graphsage/training_meta.json

Training objective:
  Weighted link prediction with negative sampling.


Notes:
  - Default: trains ONLY on Customer<->Customer edges of type BLOCKING_CANDIDATE.
  - Option: train on ALL edges (heterogeneous) by setting TRAIN_ON_ALL_EDGES = True.

"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

# ---------------------------
# Config
# ---------------------------

SEED = 42
EPOCHS = 15
BATCH_SIZE_EDGES = 20000
LR = 2e-3
WEIGHT_DECAY = 1e-5

HIDDEN_DIM = 256
OUT_DIM = 128
DROPOUT = 0.1

# If False: only trains on BLOCKING_CANDIDATE edges (recommended for ER signal)
TRAIN_ON_ALL_EDGES = False

# Negative samples per positive edge
NEGATIVE_RATIO = 1  # 1:1 is a good baseline; try 3 for stronger separation

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# Paths
NODE_INDEX_PATH = Path("./gnn-training/graph/node_index.json")
EDGE_INDEX_PATH = Path("./gnn-training/graph/edge_index.npy")
EDGE_WEIGHT_PATH = Path("./gnn-training/graph/edge_weight.npy")
EDGE_TYPE_PATH = Path("./gnn-training/graph/edge_type.json")
CUSTOMER_MASK_PATH = Path("./gnn-training/graph/customer_mask.npy")

CUSTOMER_FEATURES_PATH = Path("./gnn-training/embeddings/customer_features.npy")
CUSTOMER_IDS_PATH = Path("./gnn-training/embeddings/customer_ids.json")

OUT_DIR = Path("./gnn-training/graphsage")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_EMB = OUT_DIR / "customer_embeddings.npy"
OUT_IDS = OUT_DIR / "customer_embedding_ids.json"
OUT_META = OUT_DIR / "training_meta.json"

# ---------------------------
# Reproducibility
# ---------------------------

def seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)

seed_everything(SEED)

# ---------------------------
# Load graph artifacts
# ---------------------------

print("[train] Loading graph artifacts...")

node_id_to_idx: Dict[str, int] = json.loads(NODE_INDEX_PATH.read_text(encoding="utf-8"))
idx_to_node_id = {v: k for k, v in node_id_to_idx.items()}

edge_index_np = np.load(EDGE_INDEX_PATH)          # shape (2, E)
edge_weight_np = np.load(EDGE_WEIGHT_PATH)        # shape (E,)
customer_mask_np = np.load(CUSTOMER_MASK_PATH)    # shape (N,)
edge_type_list = json.loads(EDGE_TYPE_PATH.read_text(encoding="utf-8"))  # len E

assert edge_index_np.shape[0] == 2
E = edge_index_np.shape[1]
N = len(node_id_to_idx)
assert len(edge_type_list) == E

print(f"[train] Nodes: {N}, Edges: {E}, Device: {DEVICE}")

edge_index = torch.from_numpy(edge_index_np).long().to(DEVICE)
edge_weight = torch.from_numpy(edge_weight_np).float().to(DEVICE)
customer_mask = torch.from_numpy(customer_mask_np.astype(np.bool_)).to(DEVICE)

# ---------------------------
# Load customer features (Step 1) and align to global node index
# ---------------------------

print("[train] Loading customer features...")

customer_features = np.load(CUSTOMER_FEATURES_PATH).astype(np.float32)  # (Nc, D)
customer_ids: List[str] = json.loads(CUSTOMER_IDS_PATH.read_text(encoding="utf-8"))

Nc, D = customer_features.shape
assert len(customer_ids) == Nc
print(f"[train] Customer feature matrix: {customer_features.shape}")

# Build global X: non-customers get zeros; customers get Step-1 features
X = np.zeros((N, D), dtype=np.float32)

missing = 0
for row_i, cust_node_id in enumerate(customer_ids):
    idx = node_id_to_idx.get(cust_node_id)
    if idx is None:
        missing += 1
        continue
    X[idx] = customer_features[row_i]

if missing:
    print(f"[train] WARNING: {missing} customers from Step 1 not found in node_index")

x = torch.from_numpy(X).float().to(DEVICE)

# ---------------------------
# Select training edges
# ---------------------------

print("[train] Selecting training edges...")

def is_customer_idx(i: int) -> bool:
    return bool(customer_mask_np[i])

train_edge_ids: List[int] = []
for eid, meta in enumerate(edge_type_list):
    et = (meta or {}).get("type") or ""
    s = int(edge_index_np[0, eid])
    d = int(edge_index_np[1, eid])

    if TRAIN_ON_ALL_EDGES:
        # keep everything, but avoid edges where either endpoint is not in index (already true)
        train_edge_ids.append(eid)
    else:
        # Recommended: only explicit customer<->customer candidate edges
        if et == "BLOCKING_CANDIDATE" and is_customer_idx(s) and is_customer_idx(d):
            train_edge_ids.append(eid)

if not train_edge_ids:
    raise RuntimeError(
        "No training edges selected. If you have no BLOCKING_CANDIDATE edges, "
        "set TRAIN_ON_ALL_EDGES=True or fix the exporter to output derived customer<->customer edges."
    )

print(f"[train] Training edges selected: {len(train_edge_ids)} (TRAIN_ON_ALL_EDGES={TRAIN_ON_ALL_EDGES})")

train_edge_ids_t = torch.tensor(train_edge_ids, dtype=torch.long, device=DEVICE)

train_src = edge_index[0, train_edge_ids_t]
train_dst = edge_index[1, train_edge_ids_t]
train_w = edge_weight[train_edge_ids_t]

# ---------------------------
# Model (GraphSAGE)
# ---------------------------

try:
    from torch_geometric.nn import SAGEConv
except Exception as e:
    raise RuntimeError(
        "torch-geometric not available. Install with: pip install torch torch-geometric"
    ) from e

class GraphSAGE(nn.Module):
    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int, dropout: float):
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden_dim)
        self.conv2 = SAGEConv(hidden_dim, out_dim)
        self.dropout = dropout

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        h = self.conv1(x, edge_index)
        h = F.relu(h)
        h = F.dropout(h, p=self.dropout, training=self.training)
        h = self.conv2(h, edge_index)
        # L2 normalize embeddings (helps link prediction + retrieval)
        h = F.normalize(h, p=2, dim=1)
        return h

model = GraphSAGE(D, HIDDEN_DIM, OUT_DIM, DROPOUT).to(DEVICE)
opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)

# ---------------------------
# Negative sampling
# ---------------------------

# For ER we mostly care about Customer space negatives
customer_indices = torch.where(customer_mask)[0]
num_customer = int(customer_indices.numel())
if num_customer == 0:
    raise RuntimeError("customer_mask has 0 customers; Step 2 mask generation likely broken.")

def sample_negatives(num_pos: int) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Sample negatives as random customer-customer pairs.
    """
    # Pick random customers for src and dst
    src = customer_indices[torch.randint(0, num_customer, (num_pos,), device=DEVICE)]
    dst = customer_indices[torch.randint(0, num_customer, (num_pos,), device=DEVICE)]
    return src, dst

# ---------------------------
# Training loop
# ---------------------------

print("[train] Training...")

def bce_logits_loss(pos_logits: torch.Tensor, neg_logits: torch.Tensor, pos_weight: torch.Tensor | None = None) -> torch.Tensor:
    # targets
    pos_y = torch.ones_like(pos_logits)
    neg_y = torch.zeros_like(neg_logits)

    if pos_weight is None:
        loss_pos = F.binary_cross_entropy_with_logits(pos_logits, pos_y)
    else:
        # weight positive examples (edge weights) by multiplying their loss
        loss_pos = F.binary_cross_entropy_with_logits(pos_logits, pos_y, reduction="none")
        loss_pos = (loss_pos * pos_weight).mean()

    loss_neg = F.binary_cross_entropy_with_logits(neg_logits, neg_y)
    return loss_pos + loss_neg

# Shuffle edges each epoch
num_train = train_src.shape[0]
steps_per_epoch = math.ceil(num_train / BATCH_SIZE_EDGES)

for epoch in range(1, EPOCHS + 1):
    model.train()

    perm = torch.randperm(num_train, device=DEVICE)
    src_shuf = train_src[perm]
    dst_shuf = train_dst[perm]
    w_shuf = train_w[perm]

    total_loss = 0.0

    # Compute node embeddings once per epoch (full-batch message passing)
    z = model(x, edge_index)

    for step in range(steps_per_epoch):
        a = step * BATCH_SIZE_EDGES
        b = min((step + 1) * BATCH_SIZE_EDGES, num_train)

        pos_s = src_shuf[a:b]
        pos_d = dst_shuf[a:b]
        pos_w = w_shuf[a:b].clamp(min=0.0)

        # Positive score: dot product
        pos_logits = (z[pos_s] * z[pos_d]).sum(dim=1)

        # Negatives
        neg_count = (b - a) * NEGATIVE_RATIO
        neg_s, neg_d = sample_negatives(neg_count)
        neg_logits = (z[neg_s] * z[neg_d]).sum(dim=1)

        loss = bce_logits_loss(pos_logits, neg_logits, pos_weight=pos_w)

        opt.zero_grad(set_to_none=True)
        loss.backward()
        opt.step()

        total_loss += float(loss.detach().cpu())

    avg_loss = total_loss / steps_per_epoch
    print(f"[train] Epoch {epoch:02d}/{EPOCHS}  loss={avg_loss:.4f}")

print("[train] Training complete.")

# ---------------------------
# Export customer embeddings
# ---------------------------

print("[train] Exporting customer embeddings...")

model.eval()
with torch.no_grad():
    z = model(x, edge_index).detach().cpu().numpy().astype(np.float32)

# Extract only customers in global index order
cust_global_idxs = np.where(customer_mask_np)[0]
cust_emb = z[cust_global_idxs]

cust_ids_out = [idx_to_node_id[int(i)] for i in cust_global_idxs]

np.save(OUT_EMB, cust_emb)
OUT_IDS.write_text(json.dumps(cust_ids_out, indent=2), encoding="utf-8")

meta = {
    "seed": SEED,
    "epochs": EPOCHS,
    "batch_size_edges": BATCH_SIZE_EDGES,
    "lr": LR,
    "weight_decay": WEIGHT_DECAY,
    "hidden_dim": HIDDEN_DIM,
    "out_dim": OUT_DIM,
    "dropout": DROPOUT,
    "train_on_all_edges": TRAIN_ON_ALL_EDGES,
    "negative_ratio": NEGATIVE_RATIO,
    "num_nodes": N,
    "num_edges": E,
    "num_customer_nodes": int(customer_mask_np.sum()),
    "training_edges_used": int(len(train_edge_ids)),
}

OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")

print("[train] Done.")
print(f"[train] Wrote:")
print(f"  - {OUT_EMB}  (shape={cust_emb.shape})")
print(f"  - {OUT_IDS}")
print(f"  - {OUT_META}")
