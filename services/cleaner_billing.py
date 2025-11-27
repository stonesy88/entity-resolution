# Cleaner needs fixing to match Shadow Traffic changes TBD!


#!/usr/bin/env python
import hashlib
import json
import os
import re
import sys
import csv
from datetime import datetime
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer
from metaphone import doublemetaphone
from nameparser import HumanName
import phonenumbers
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Nickname mapping (from names.csv)
# ---------------------------------------------------------------------------

NICKNAME_CSV = os.getenv("NICKNAME_CSV", "./services/names.csv")


def load_nickname_map(csv_path: str) -> dict[str, str]:
    nick_to_canon: dict[str, str] = {}

    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Expect headers: name1, relationship, name2
            required = {"name1", "relationship", "name2"}
            if not reader.fieldnames or not required.issubset(
                set(h.lower() for h in reader.fieldnames)
            ):
                print(
                    f"Unexpected headers in {csv_path}: {reader.fieldnames}",
                    file=sys.stderr,
                )

            for row in reader:
                name1 = (row.get("name1") or "").strip().lower()
                rel = (row.get("relationship") or "").strip().lower()
                name2 = (row.get("name2") or "").strip().lower()

                if rel != "has_nickname":
                    continue

                if not name1 or not name2:
                    continue

                canonical = name1
                nickname = name2

                # Prefer the longest canonical if collisions
                if (
                    nickname not in nick_to_canon
                    or len(canonical) > len(nick_to_canon[nickname])
                ):
                    nick_to_canon[nickname] = canonical

    except FileNotFoundError:
        print(
            f"Nickname CSV not found: {csv_path} — starting with empty map",
            file=sys.stderr,
        )

    print(f"Loaded {len(nick_to_canon)} nickname mappings from {csv_path}")
    return nick_to_canon


NICK_CANON = load_nickname_map(NICKNAME_CSV)

# ---------------------------------------------------------------------------
# Kafka config
# ---------------------------------------------------------------------------

RAW_TOPIC = os.getenv("RAW_TOPIC", "raw.customer")
ENR_TOPIC = os.getenv("ENR_TOPIC", "enriched.customer")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("CLEANER_GROUP_ID", "cleaner")

# ---------------------------------------------------------------------------
# Email, hashing, name, phone, DOB
# ---------------------------------------------------------------------------

email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def sha256(s: str) -> str:
    """Hash for PII IDs."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def normalise_name(raw: str) -> Dict[str, Any]:
    if not raw:
        return {
            "given_name": None,
            "family_name": None,
            "given_name_canonical": None,
            "surname_phonetic": None,
        }

    n = HumanName(raw)
    given = n.first.strip().title() if n.first else None
    family = n.last.strip().title() if n.last else None

    # Canonical name mapping from CSV to determine if a nickname
    canonical = given
    if given:
        lower = given.lower()
        if lower in NICK_CANON:
            canonical = NICK_CANON[lower].title()

    # Phonetic surname (Double Metaphone)
    surname_phonetic = doublemetaphone(family)[0] if family else None

    return {
        "given_name": given,
        "family_name": family,
        "given_name_canonical": canonical,
        "surname_phonetic": surname_phonetic,
    }


def normalise_phone(raw: str, default_region: str = "GB") -> Optional[str]:
    if not raw:
        return None
    try:
        num = phonenumbers.parse(raw, default_region)
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(
                num, phonenumbers.PhoneNumberFormat.E164
            )
    except Exception:
        return None
    return None


def normalise_email(raw: str) -> Optional[str]:
    if not raw:
        return None
    e = raw.strip().lower()
    return e if email_re.match(e) else None


def normalise_dob(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    # Assume already ISO, else try simple patterns
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Cleaning function
# ---------------------------------------------------------------------------


def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # Provenance
    out["source"] = rec.get("source") or "billing"
    out["source_system"] = rec.get("source_system") or "billing_portal"
    out["source_id"] = str(rec.get("source_id") or "").strip() or None

    # Event time
    et = rec.get("event_time")
    if isinstance(et, (int, float)):
        out["event_time"] = int(et)
    else:
        out["event_time"] = int(datetime.utcnow().timestamp() * 1000)

    # Name
    name_parts = normalise_name(
        f"{rec.get('given_name','')} {rec.get('family_name','')}".strip()
    )
    out.update(name_parts)

    # DOB
    out["dob"] = normalise_dob(rec.get("dob") or "")

    # Email
    out["email"] = normalise_email(rec.get("email") or "")

    # Phone
    out["phone_e164"] = normalise_phone(rec.get("phone") or "")

    # Address (PAF-ish) + postcode + sector
    addr_raw = rec.get("address") or rec.get("address_line_1") or ""
    postcode_raw = rec.get("postcode") or rec.get("postal_code") or ""
    country_raw = rec.get("country")

    addr = parse_uk_address_with_ollama(addr_raw, postcode_raw, country_raw)

    out["company_name"] = addr.get("company_name")
    out["department"] = addr.get("department")
    out["address_line_1"] = addr.get("address_line_1")
    out["address_line_2"] = addr.get("address_line_2")
    out["post_town"] = addr.get("post_town")
    out["postcode"] = addr.get("postal_code")
    out["postcode_sector"] = addr.get("postcode_sector")
    out["country"] = addr.get("country")

    # Policy & family
    policy = str(rec.get("policy_id") or "").upper().replace("-", "").strip()
    out["policy_id"] = policy or None
    out["family_id"] = rec.get("family_id") or None

    # National ID hash
    nid = str(rec.get("national_id") or "").strip()
    out["national_id_hash"] = sha256(nid) if nid else None

    # Derived keys
    dob = out.get("dob")
    surname_phonetic = out.get("surname_phonetic")
    email = out.get("email")
    phone_e164 = out.get("phone_e164")
    policy_id = out.get("policy_id")

    out["bk_nid_dob"] = (
        f"{out['national_id_hash']}|{dob}"
        if out.get("national_id_hash") and dob
        else None
    )
    out["bk_email_dob"] = f"{email}|{dob}" if email and dob else None
    out["bk_phone_sndx"] = (
        f"{phone_e164}|{surname_phonetic}" if phone_e164 and surname_phonetic else None
    )
    out["bk_policy_dob"] = f"{policy_id}|{dob}" if policy_id and dob else None
    out["bk_nameYdob"] = (
        f"{surname_phonetic[:1]}|{dob[:4]}"
        if surname_phonetic and dob
        else None
    )

    return out


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main():
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "lz4",
    }

    c = Consumer(consumer_conf)
    p = Producer(producer_conf)

    print(f"[cleaner] Consuming from {RAW_TOPIC}, producing to {ENR_TOPIC}")
    c.subscribe([RAW_TOPIC])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error(), file=sys.stderr)
                continue

            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                print("Bad JSON:", e, file=sys.stderr)
                continue

            cleaned = clean_record(rec)

            p.produce(
                ENR_TOPIC,
                key=f"billing:{cleaned.get('source_id','')}".encode("utf-8"),
                value=json.dumps(cleaned).encode("utf-8"),
            )
            p.poll(0)

    except KeyboardInterrupt:
        print("Shutting down cleaner...")
    finally:
        c.close()
        p.flush(10)


if __name__ == "__main__":
    main()