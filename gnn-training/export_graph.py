#!/usr/bin/env python
"""
Export graph structure + customer features from Quine
for downstream embedding + GraphSAGE training.

Outputs:
- nodes.jsonl
- edges.jsonl

Design principles:
- Blocking keys = graph topology ONLY
- Features = semantic, stable, non-identifying
- Embeddings computed AFTER export
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, Iterable, List

import requests

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

QUINE_URL = "http://localhost:8082/api/v1/query/cypher"
TIMEOUT_SECONDS = 120
PAGE_SIZE = 5000

NODES_OUT = "./gnn-training/outputs/nodes.jsonl"
EDGES_OUT = "./gnn-training/outputs/edges.jsonl"

# ---------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------


class QuineQueryError(RuntimeError):
    pass


# ---------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------


def post_cypher(query: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"text": query, "parameters": params}

    try:
        r = requests.post(QUINE_URL, json=payload, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as e:
        raise QuineQueryError(f"HTTP failure: {e}") from e

    if r.status_code != 200:
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise QuineQueryError(f"Quine HTTP {r.status_code}: {err}")

    return r.json()


def run_paged_query(query: str) -> Iterable[Dict[str, Any]]:
    skip = 0
    while True:
        data = post_cypher(query, {"skip": skip, "lim": PAGE_SIZE})

        cols = data.get("columns")
        rows = data.get("results")

        if not rows:
            break

        for row in rows:
            yield dict(zip(cols, row))

        skip += PAGE_SIZE
        time.sleep(0.02)


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


def nid(label: str, raw: Any) -> str:
    return f"{label}:{raw}"


# ---------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------

# -------------------------
# Customer nodes
# -------------------------
CUSTOMER_QUERY = """
MATCH (c:Customer)
RETURN
  id(c) AS id,

  /* Name */
  coalesce(c.firstNameLower, "") AS firstNameLower,
  coalesce(c.lastNameLower, "") AS lastNameLower,
  coalesce(c.firstNameInitial, "") AS firstNameInitial,
  coalesce(c.lastNameInitial, "") AS lastNameInitial,
  coalesce(c.firstNameMetaphone1, "") AS firstNameMetaphone1,
  coalesce(c.lastNameMetaphone1, "") AS lastNameMetaphone1,

  /* Demographics */
  coalesce(c.dobYearOnly, "") AS dobYearOnly,
  coalesce(c.ageBucket, "") AS ageBucket,

  /* Email */
  coalesce(c.emailUserPartNormalized, "") AS emailUserPartNormalized,
  coalesce(c.emailDomain, "") AS emailDomain,

  /* Phone */
  coalesce(c.phoneCountryCode, "") AS phoneCountryCode,
  coalesce(c.phoneLast4, "") AS phoneLast4,

  /* Location (coarse) */
  coalesce(c.postcodeSector, "") AS postcodeSector,
  coalesce(c.postcodeArea, "") AS postcodeArea,
  coalesce(c.cityLower, "") AS cityLower

SKIP $skip LIMIT $lim
""".strip()

# -------------------------
# BlockingKey nodes
# -------------------------
BLOCKING_KEY_QUERY = """
MATCH (bk:BlockingKey)
RETURN
  id(bk) AS id,
  coalesce(bk.value, "") AS value
SKIP $skip LIMIT $lim
""".strip()

# -------------------------
# Transit nodes (generic)
# -------------------------
TRANSIT_QUERY = """
MATCH (c:Customer)-[r]->(t)
WHERE type(r) IN [
  "HAS_HOUSE_NUMBER",
  "HAS_STREET",
  "IN_CITY",
  "IN_COUNTY",
  "IN_POSTCODE",
  "IN_POSTCODE_SECTOR",
  "IN_POSTCODE_AREA"
]
RETURN DISTINCT
  id(t) AS id
SKIP $skip LIMIT $lim
""".strip()

# -------------------------
# Customer → BlockingKey edges
# -------------------------
CUSTOMER_BK_EDGE_QUERY = """
MATCH (c:Customer)-[r]->(bk:BlockingKey)
RETURN
  id(c) AS src,
  id(bk) AS dst,
  type(r) AS type
SKIP $skip LIMIT $lim
""".strip()

# -------------------------
# Customer → Transit edges
# -------------------------
CUSTOMER_TRANSIT_EDGE_QUERY = """
MATCH (c:Customer)-[r]->(t)
WHERE type(r) IN [
  "HAS_HOUSE_NUMBER",
  "HAS_STREET",
  "IN_CITY",
  "IN_COUNTY",
  "IN_POSTCODE",
  "IN_POSTCODE_SECTOR",
  "IN_POSTCODE_AREA"
]
RETURN
  id(c) AS src,
  id(t) AS dst,
  type(r) AS type
SKIP $skip LIMIT $lim
""".strip()

# -------------------------
# Derived Customer ↔ Customer edges (blocking)
# -------------------------
DERIVED_EDGE_QUERY = """
MATCH (c1:Customer)-[:{REL}]->(:BlockingKey)<-[:{REL}]-(c2:Customer)
WHERE toString(id(c1)) < toString(id(c2))
RETURN
  id(c1) AS src,
  id(c2) AS dst
SKIP $skip LIMIT $lim
""".strip()


DERIVED_REASONS = [
    ("BLOCKS_ON_LNAME_DOB", "LNAME_DOB", 3.0),
    ("BLOCKS_ON_NAMEHASH_DOB", "NAMEHASH_DOB", 3.5),
    ("BLOCKS_ON_DOB_PHONE", "DOB_PHONE", 4.0),
    ("BLOCKS_ON_DOB_EMAIL", "DOB_EMAIL", 4.5),
    ("BLOCKS_ON_DOB_HOUSE_POSTCODE", "DOB_HOUSE_POSTCODE", 3.0),
]

# ---------------------------------------------------------------------
# Export logic
# ---------------------------------------------------------------------


def export_nodes() -> None:
    nodes: List[Dict[str, Any]] = []

    print("[export] Customers...")
    for r in run_paged_query(CUSTOMER_QUERY):
        nodes.append(
            {
                "id": nid("Customer", r["id"]),
                "label": "Customer",
                "features": {k: v for k, v in r.items() if k != "id"},
            }
        )

    print("[export] BlockingKeys...")
    for r in run_paged_query(BLOCKING_KEY_QUERY):
        nodes.append(
            {
                "id": nid("BlockingKey", r["id"]),
                "label": "BlockingKey",
                "features": {"value": r["value"]},
            }
        )

    print("[export] Transit nodes...")
    seen = set()
    for r in run_paged_query(TRANSIT_QUERY):
        tid = nid("Transit", r["id"])
        if tid not in seen:
            seen.add(tid)
            nodes.append(
                {"id": tid, "label": "Transit", "features": {}}
            )

    write_jsonl(NODES_OUT, nodes)
    print(f"[export] nodes.jsonl written ({len(nodes)})")


def export_edges() -> None:
    edges: List[Dict[str, Any]] = []

    print("[export] Customer → BlockingKey edges...")
    for r in run_paged_query(CUSTOMER_BK_EDGE_QUERY):
        edges.append(
            {
                "src": nid("Customer", r["src"]),
                "dst": nid("BlockingKey", r["dst"]),
                "type": r["type"],
            }
        )

    print("[export] Customer → Transit edges...")
    for r in run_paged_query(CUSTOMER_TRANSIT_EDGE_QUERY):
        edges.append(
            {
                "src": nid("Customer", r["src"]),
                "dst": nid("Transit", r["dst"]),
                "type": r["type"],
            }
        )

    print("[export] Derived Customer ↔ Customer edges...")
    for rel, reason, weight in DERIVED_REASONS:
        q = DERIVED_EDGE_QUERY.format(REL=rel)
        for r in run_paged_query(q):
            edges.append(
                {
                    "src": nid("Customer", r["src"]),
                    "dst": nid("Customer", r["dst"]),
                    "type": "BLOCKING_CANDIDATE",
                    "reason": reason,
                    "weight": weight,
                }
            )

    write_jsonl(EDGES_OUT, edges)
    print(f"[export] edges.jsonl written ({len(edges)})")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    try:
        export_nodes()
        export_edges()
        print("[export] DONE")
    except QuineQueryError as e:
        print(f"[export] ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
