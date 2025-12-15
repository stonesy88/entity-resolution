# Customer Entity Resolution (CER) — Proof of Concept

This repository demonstrates a **real-time, graph-native Entity Resolution (ER) pipeline** designed to identify when multiple systems refer to the **same underlying customer**, even in the presence of:

- typos and spelling variation  
- missing or partial attributes  
- address and contact drift over time  
- multi-system ingestion with inconsistent schemas  

The system combines **deterministic rules**, **semantic embeddings**, and **graph learning** to produce high-quality match candidates suitable for automated or human-in-the-loop resolution.

---

## Core Technologies

- **Kafka** — real-time event streaming backbone  
- **Cleaner.py** — normalization, metaphones, blocking keys  
- **Sentence / semantic embeddings** — names, email, phone, address  
- **Quine** — real-time identity graph construction  
- **GraphSAGE** — neighborhood-aware learned embeddings  
- **pgvector (PostgreSQL)** — fast vector similarity search  
- **ShadowTraffic** — synthetic multi-system customer data (optional)  

> ⚠️ Quine does not natively support vector similarity or GNN training, so **pgvector and GraphSAGE are used externally** and reintegrated via Kafka.

---

## Architecture Overview

```mermaid
flowchart TD
  ST[ShadowTraffic] --> K1[Kafka raw topics]
  K1 --> C[Cleaner.py<br/>standardisation + blocking keys]
  C --> K2[Kafka customers.enriched]
  K2 --> Q[Quine<br/>identity graph]
  Q --> EX[Graph Export]
  EX --> GNN[GraphSAGE Training]
  GNN --> PG[pgvector Similarity Search]
  PG --> K3[Kafka customer.match_candidates]
  K3 --> Q2[Quine Match Ingest]

## Pipeline Breakdown

### 1. Ingestion (ShadowTraffic → Kafka)

Synthetic customer data is produced from multiple simulated Systems of Record (SoRs) using ShadowTraffic.

Each event represents a single observation of a customer.

*ShadowTraffic is optional for the PoC and requires a license.*
*Ollama is used to generate  common misspellings and variations of names, emails, phones, and addresses. This can be finicky, adjust prompt to your needs.*

### 2. Standardisation & Enrichment (`Cleaner.py`)

The cleaner service prepares records for both graph construction and machine learning.

**Responsibilities:**

- Normalize names, emails, phones, and addresses
- Generate:
  - name metaphones
  - initials
  - normalized tokens
- Compute a rich set of **blocking keys** for candidate pruning
- Output a clean, enriched customer event

**Output Topic:** `customers.enriched`

This topic is the single source of truth for downstream identity processing.

### 3. Graph Construction (Quine)

Quine consumes `customers.enriched` and builds a real-time identity graph.

**Node Types**

| Node | Purpose |
|---|---|
| Customer | Canonical customer entity |
| Record | Event-level observation |
| BlockingKey | Candidate-generation anchors |
| Transit nodes | Address and location components |

**Key Properties**

- Customers are deterministically identified using `idFrom("customer", bk_deterministic_res)`
- If no deterministic key exists, a record-scoped fallback is used
- Customers can accumulate new transit values over time
- Blocking keys create structural graph connections but are not features

### 4. Graph Export for ML

Quine exports the graph into flat artifacts:

**Files Produced**

- `nodes.jsonl`
- `edges.jsonl`
- `edge_index.npy`
- `edge_weight.npy`
- `edge_type.json`
- `customer_mask.npy`

**Design Principles**

- Graph topology ≠ features
- Blocking keys are edges only
- Customer attributes become feature inputs
- Graph structure is preserved for GNN training

### 5. Feature Embedding (Step 1)

Customer nodes are converted into feature vectors:

**Feature Sources**

- Name (semantic embedding)
- Email (semantic embedding)
- Phone (semantic embedding)
- Address (semantic embedding)
- Numeric attributes:
  - DOB year
  - age bucket
  - country code

These are concatenated into a single feature matrix:

`customer_features.npy` → `(N_customers × D)`

### 6. GraphSAGE Training (Step 2)

GraphSAGE learns neighborhood-aware embeddings by combining:

- Customer semantic features
- Blocking-derived graph structure
- Multi-hop neighborhood context

**Training Objective**

- Link prediction
- Positive edges = `BLOCKING_CANDIDATE`
- Negatives = random customer pairs
- Edge weights bias training strength

**Output**

`customer_graph_embeddings.npy` → `(N_customers × 128)`

These vectors encode both who the customer is and who they are connected to.

### 7. Vector Similarity Search (pgvector)

GraphSAGE embeddings are stored in PostgreSQL using `pgvector`.

**Why pgvector?**

- Fast cosine similarity
- SQL-based filtering
- Easy integration with Kafka
- Transparent thresholds

Only blocking-constrained pairs are scored:

```sql
SELECT
  bc.src_customer_id,
  bc.dst_customer_id,
  1 - (e1.embedding <=> e2.embedding) AS similarity
FROM blocking_candidates bc
JOIN customer_graph_embeddings e1 ON ...
JOIN customer_graph_embeddings e2 ON ...
WHERE (e1.embedding <=> e2.embedding) < threshold;
```

This avoids O(N²) comparisons.

### 8. Match Candidate Emission (Kafka)

High-confidence candidate matches are published to: `customer.match_candidates`

**Message Schema**

```json
{
  "src_customer_key": "uuid",
  "dst_customer_key": "uuid",
  "similarity": 0.99,
  "distance": 0.01,
  "blocking_reason": "DOB_PHONE",
  "blocking_weight": 4.0,
  "model": "graphsage-v1",
  "threshold": 0.8,
  "run_id": "2025-12-15T12:01:02Z"
}
```

### 9. Match Ingestion Back into Quine

Quine consumes `customer.match_candidates` and creates:

- `PotentialMatch` nodes
- Directed edges:
  `(Customer)-[:HAS_POTENTIAL_MATCH]->(PotentialMatch)-[:MATCHES]->(Customer)`

This preserves:

- provenance
- scoring metadata
- model versioning
- re-runnability

### 10. Resolution Strategy (Future)

Planned resolution paths:

- Automatic merge (very high confidence)
- Human review (moderate confidence)
- Silent verification (SMS / email confirmation)
- Online learning from confirmed resolutions

## Summary

This PoC demonstrates a production-grade identity resolution architecture that:

- avoids naïve pairwise matching
- scales using blocking + graph structure
- fuses semantic and structural signals
- supports explainable, auditable matching
- cleanly separates ingestion, learning, and resolution

It is graph-native, ML-ready, and streaming-first.

### Future Enhancements

- Online GraphSAGE retraining
- Active learning from confirmed matches
- Custom embedding models (Ollama / domain-tuned LLMs)
- Canonical entity materialization
- Merge/unmerge lifecycle management

## Synthetic Data

Synthetic customer data generated using ShadowTraffic
🔗 https://shadowtraffic.io/

Used to simulate:

- cross-system duplication
- partial identifiers
- gradual data enrichment
- real-world noise patterns