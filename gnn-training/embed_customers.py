#!/usr/bin/env python
"""
Step 1 — Build Customer Feature Vectors for GraphSAGE

Input:
  - ./gnn-training/outputs/nodes.jsonl (from Quine exporter)

Output:
  - ./gnn-training/embeddings/customer_features.npy     (N x D float32)
  - ./gnn-training/embeddings/customer_ids.json         (index -> customer_id)
  - ./gnn-training/embeddings/feature_spec.json         (documentation of feature layout)

Features:
  - Name embedding (includes phonetic/metaphone hints)
  - Email embedding
  - Phone embedding
  - Address embedding (includes postcode sector/area if present)
  - Numeric attributes (DOB year normalized, ageBucket encoded)

Embedding model:
  sentence-transformers/all-MiniLM-L6-v2
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------
# Config (DEFINE PATHS BEFORE USING THEM)
# ---------------------------------------------------------

NODES_PATH = Path("./gnn-training/outputs/nodes.jsonl")

OUT_DIR = Path("./gnn-training/embeddings")
OUT_FEATURES = OUT_DIR / "customer_features.npy"
OUT_IDS = OUT_DIR / "customer_ids.json"
OUT_SPEC = OUT_DIR / "feature_spec.json"

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# Ensure output directory exists (AFTER paths are defined)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------


def normalize_dob_year(val: Any) -> float:
    """Normalize DOB year to roughly [0,1]."""
    try:
        year = int(str(val).strip())
        return float(year - 1900) / 150.0
    except Exception:
        return 0.0


AGE_BUCKET_MAP = {
    "0-17": 0,
    "18-24": 1,
    "25-34": 2,
    "35-44": 3,
    "45-54": 4,
    "55-64": 5,
    "65-74": 6,
    "75+": 7,
}


def encode_age_bucket(bucket: Any) -> float:
    """Encode age bucket to [0,1]. Unknown/missing -> 0.0."""
    b = str(bucket).strip() if bucket else ""
    if not b:
        return 0.0
    idx = AGE_BUCKET_MAP.get(b, 0)
    return float(idx) / float(max(AGE_BUCKET_MAP.values()) or 1)


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def join_nonempty(parts: List[str], sep: str = " ") -> str:
    return sep.join([p for p in (p.strip() for p in parts) if p])


# ---------------------------------------------------------
# Load Customer nodes
# ---------------------------------------------------------

print("[embed] Loading Customer nodes...")

customers: List[Dict[str, Any]] = []
with open(NODES_PATH, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        node = json.loads(line)
        if node.get("label") == "Customer":
            customers.append(node)

if not customers:
    print("[embed] ERROR: No Customer nodes found in nodes.jsonl", file=sys.stderr)
    sys.exit(1)

print(f"[embed] Customers loaded: {len(customers)}")

# ---------------------------------------------------------
# Build text fields + numeric features
# ---------------------------------------------------------

name_texts: List[str] = []
email_texts: List[str] = []
phone_texts: List[str] = []
address_texts: List[str] = []
numeric_features: List[List[float]] = []
customer_ids: List[str] = []

for node in customers:
    feats = node.get("features", {}) or {}
    customer_ids.append(node["id"])

    
    first = safe_str(feats.get("firstNameLower"))
    last = safe_str(feats.get("lastNameLower"))
    fmeta = safe_str(feats.get("firstNameMetaphone1"))
    lmeta = safe_str(feats.get("lastNameMetaphone1"))
    
    name_texts.append(
        join_nonempty(
            [
                first,
                last,
                f"first_metaphone:{fmeta}" if fmeta else "",
                f"last_metaphone:{lmeta}" if lmeta else "",
            ],
            sep=" ",
        )
    )

    # ---- EMAIL
    email_user = safe_str(feats.get("emailUserPartNormalized"))
    email_domain = safe_str(feats.get("emailDomain"))
    email_texts.append(join_nonempty([email_user, "@", email_domain], sep=" "))

    # ---- PHONE
    phone_cc = safe_str(feats.get("phoneCountryCode"))
    phone_digits = safe_str(feats.get("phoneDigitsOnly"))
    phone_texts.append(join_nonempty([f"+{phone_cc}" if phone_cc else "", phone_digits], sep=" "))

    # ---- ADDRESS
    house = safe_str(feats.get("houseNumber"))
    street = safe_str(feats.get("streetLower"))
    city = safe_str(feats.get("cityLower"))
    county = safe_str(feats.get("countyLower"))
    postcode = safe_str(feats.get("postcodeLower"))
    pc_sector = safe_str(feats.get("postcodeSector"))
    pc_area = safe_str(feats.get("postcodeArea"))

    address_texts.append(
        join_nonempty(
            [
                house,
                street,
                city,
                county,
                postcode,
                f"postcode_sector:{pc_sector}" if pc_sector else "",
                f"postcode_area:{pc_area}" if pc_area else "",
            ],
            sep=", ",
        )
    )

    # ---- NUMERIC
    numeric_features.append(
        [
            normalize_dob_year(feats.get("dobYearOnly")),
            encode_age_bucket(feats.get("ageBucket")),
        ]
    )

numeric_np = np.asarray(numeric_features, dtype=np.float32)

# ---------------------------------------------------------
# Embedding
# ---------------------------------------------------------

print("[embed] Loading sentence transformer...")
model = SentenceTransformer(MODEL_NAME)

print("[embed] Embedding name...")
name_emb = model.encode(name_texts, normalize_embeddings=True)

print("[embed] Embedding email...")
email_emb = model.encode(email_texts, normalize_embeddings=True)

print("[embed] Embedding phone...")
phone_emb = model.encode(phone_texts, normalize_embeddings=True)

print("[embed] Embedding address...")
address_emb = model.encode(address_texts, normalize_embeddings=True)

# ---------------------------------------------------------
# Concatenate final feature matrix
# ---------------------------------------------------------

print("[embed] Concatenating feature vectors...")
features = np.concatenate(
    [name_emb, email_emb, phone_emb, address_emb, numeric_np],
    axis=1,
).astype(np.float32)

# ---------------------------------------------------------
# Write outputs
# ---------------------------------------------------------

np.save(OUT_FEATURES, features)

with open(OUT_IDS, "w", encoding="utf-8") as f:
    json.dump(customer_ids, f, indent=2)

feature_spec = {
    "model": MODEL_NAME,
    "embedding_dims": {
        "name": int(name_emb.shape[1]),
        "email": int(email_emb.shape[1]),
        "phone": int(phone_emb.shape[1]),
        "address": int(address_emb.shape[1]),
    },
    "numeric_features": [
        "dobYearOnly_normalized",
    ],
    "text_fields": {
        "name": "firstNameLower, lastNameLower, firstNameMetaphone1, lastNameMetaphone1",
        "email": "emailUserPartNormalized, emailDomain",
        "phone": "phoneCountryCode, phoneDigitsOnly",
        "address": "houseNumber, streetLower, cityLower, countyLower, postcodeLower, postcodeSector, postcodeArea",
    },
    "total_dim": int(features.shape[1]),
    "num_customers": int(features.shape[0]),
}

with open(OUT_SPEC, "w", encoding="utf-8") as f:
    json.dump(feature_spec, f, indent=2)

print("[embed] Done.")
print(f"[embed] Feature matrix: {features.shape}")
print("[embed] Files written:")
print(f"  - {OUT_FEATURES}")
print(f"  - {OUT_IDS}")
print(f"  - {OUT_SPEC}")
