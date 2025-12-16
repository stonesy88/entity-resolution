{
  "type": "KafkaIngest",
  "bootstrapServers": "localhost:9092",
  "topics": ["customer.match_candidates"],
  "format": {
    "type": "CypherJson",
    "query": "WITH $that AS event MATCH (src),(dst),(m) WHERE strId(src)=event.src_customer_key AND strId(dst)=event.dst_customer_key AND id(m)=idFrom(\"potential_match\",event.src_customer_key+\"|\"+event.dst_customer_key+\"|\"+event.run_id) SET m:PotentialMatch,m.similarity=event.similarity,m.distance=event.distance,m.blocking_reasons=event.blocking_reasons,m.blocking_weight=event.blocking_weight,m.model=event.model,m.threshold=event.threshold,m.run_id=event.run_id,m.ingested_at=timestamp() CREATE (src)-[:HAS_POTENTIAL_MATCH]->(m) CREATE (m)-[:MATCHES]->(dst)"
  }
}
