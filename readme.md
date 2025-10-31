# Customer Entity Resolution (CER) Proof-of-Concept

This project demonstrates a **real-time entity resolution pipeline** for a hypothetical enterprise org. 

It combines **Kafka (KRaft)**, **Splink**, **Memgraph**, and **ShadowTraffic** to identify when records from multiple systems of record (SoRs) refer to the same entity.

---

## Architecture Overview

```mermaid
flowchart TD
  subgraph S[ShadowTraffic]
  end
  subgraph K[Kafka (KRaft)]
  end
  subgraph S1[Standardization Stream]
  end
  subgraph D[Deterministic Matching]
  end
  subgraph P[Probabilistic Matching (Splink)]
  end
  subgraph M[Graph Linking (Memgraph)]
  end
  subgraph PM[potential-matches topic]
  end

  S --> K --> S1 & D
  D --> P --> M --> PM

---

1. **Ingestion:** ShadowTraffic publishes synthetic customer records from multiple SoRs to Kafka topics.  
2. **Standardisation:** Cleans and normalises data into a canonical `staged.customer` topic.  
3. **Deterministic matching:** Exact rules (e.g., National ID + DOB) create `match.deterministic` events.  
4. **Probabilistic matching:** [Splink(https://moj-analytical-services.github.io/splink/index.html)] applies Fellegi-Sunter scoring to create `match.probabilistic` events.  
5. **Graph resolution:** Memgraph merges deterministic and probabilistic links into connected components.  
6. **Output:** When all phases complete, a `potential-matches` event is emitted for review or merge.

---

## Stack

| Component | Purpose |
|------------|----------|
| **Kafka (Bitnami, KRaft mode)** | Event backbone |
| **Schema Registry** | Avro schema contracts for all topics |
| **ShadowTraffic** | Synthetic SoR data generator |
| **Kafka-UI** | Browser UI to inspect topics and schemas |
| **Splink (DuckDB)** | Probabilistic record linkage |
| **Memgraph + MAGE** | Graph-based entity resolution and community detection |

---

## Synthetic Data Generation

The generator at generators/customers.json defines:

Multiple SoRs (CRM, Billing, Claims)

Family policies (raw.policy) with policy_id and family_id

Controlled duplication, nicknames, typos, address changes

Cross-system linkages for realistic entity resolution tests

You can tune the realism by editing the globals section:

"duplicationRate": 0.18,
"familyPolicyRate": 0.35,
"diffSurnameInFamilyRate": 0.22

ShadowTraffic auto-registers Avro schemas in the Schema Registry and publishes JSON/Avro events to Kafka.

