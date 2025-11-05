#!/usr/bin/env python3
"""
Send a single customer change event from the Billing portal to Kafka.
- Topic: raw.customer.billing
- Key:   billing:<source_id>
"""

import json
import os
import sys
import time
import hashlib
from datetime import datetime, timezone
from confluent_kafka import Producer

def env(name, default=None):
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def sha256(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def make_event(
    source_id: str = "BILL-10001",
    given_name: str = "Liz",
    family_name: str = "Turner",
    dob: str = "1984-02-09",
    email: str = "liz.t@example.org",
    phone: str = "+44 7123 456789",
    address: str = "22 Market Street, Bristol BS1 1AA",
    postcode: str = "BS1 1AA",
    national_id: str = "AB123456C",
    policy_id: str = "POL-77ZQ",
):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "source": "billing",
        "source_system": "billing_portal",
        "source_id": source_id,
        "event_time": now_ms,         # business/event time in epoch ms
        "op_type": "u",               # c=create, u=update, d=delete
        "given_name": given_name,
        "family_name": family_name,
        "dob": dob,                   # keep raw; cleaner will normalise
        "email": email,
        "phone": phone,
        "address": address,
        "postcode": postcode,
        "national_id": national_id,   # raw here; cleaner will hash
        "policy_id": policy_id,
    }

def main():
    # ---- config -------------------------------------------------------------
    bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092")
    topic     = env("KAFKA_TOPIC", "raw.customer.billing")
    source_id = env("BILLING_SOURCE_ID", "BILL-10001")

    # Optional SASL/SSL (leave unset if not using auth)
    sasl_mech = env("KAFKA_SASL_MECHANISM")       # e.g., "PLAIN"
    security  = env("KAFKA_SECURITY_PROTOCOL")    # e.g., "SASL_SSL"
    sasl_user = env("KAFKA_SASL_USERNAME")
    sasl_pass = env("KAFKA_SASL_PASSWORD")

    conf = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "lz4"
    }
    if security:
        conf["security.protocol"] = security
    if sasl_mech:
        conf["sasl.mechanisms"] = sasl_mech
    if sasl_user:
        conf["sasl.username"] = sasl_user
    if sasl_pass:
        conf["sasl.password"] = sasl_pass

    p = Producer(conf)

    # Compose the event
    evt = make_event(source_id=source_id)

    # Kafka message key (stable per entity)
    key = f"billing:{evt['source_id']}".encode("utf-8")
    val = json.dumps(evt, ensure_ascii=False).encode("utf-8")

    # Some helpful headers for downstream services
    headers = [
        ("source", b"billing"),
        ("source_system", b"billing_portal"),
        ("schema", b"raw.customer.billing.v1")
    ]

    def delivery(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}", file=sys.stderr)
        else:
            print(f"Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    # Produce exactly one message (change event)
    p.produce(topic=topic, key=key, value=val, headers=headers, on_delivery=delivery)
    p.flush(10)  # seconds
    print("Done.")

if __name__ == "__main__":
    main()
