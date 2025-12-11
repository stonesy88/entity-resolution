#!/usr/bin/env python
"""
Cleaner for billing events enriched with CocoIndex embeddings.

The general flow is as follows:
- Normalising / standardising raw customer fields
- Generating blocking keys
- Embedding name/email/address/phone/signature using CocoIndex
- Publishing enriched records to Kafka for Quine

TBD - Common Nickname library is pretty defunct with ST name generator.
"""

import hashlib
import json
import os
import re
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, Producer
from metaphone import doublemetaphone
from nameparser import HumanName
import phonenumbers

# -------------------------------
# CocoIndex
# -------------------------------
try:
    from cocoindex import Coco
    coco = Coco(name="universal-embedder-v1")
    COCO_ENABLED = True
except Exception as e:
    print("[cleaner] WARNING: CocoIndex unavailable:", e, file=sys.stderr)
    COCO_ENABLED = False
    coco = None

RAW_TOPIC = os.getenv("RAW_TOPIC", "customers.raw")
ENR_TOPIC = os.getenv("ENR_TOPIC", "customers.enriched")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("CLEANER_GROUP_ID", "cleaner_billing")
DEFAULT_REGION = os.getenv("PHONE_REGION", "GB")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None

def lower_str(value: Optional[str]) -> Optional[str]:
    cleaned = clean_str(value)
    return cleaned.lower() if cleaned else None

def initial(value: Optional[str]) -> Optional[str]:
    cleaned = clean_str(value)
    return cleaned[0].lower() if cleaned else None

def metaphones(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    cleaned = clean_str(value)
    if not cleaned:
        return None, None
    p, s = doublemetaphone(cleaned)
    return p or None, s or None

def tokenise(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [t for t in re.split(r"[^a-z0-9]+", value.lower()) if t]


# ---------------------------------------------------------------------------
# Email / phone / DOB / address normalisation
# ---------------------------------------------------------------------------

def normalise_email(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not raw:
        return None, None, None

    lowered = raw.strip().lower()
    if not EMAIL_RE.match(lowered):
        return None, None, None

    user_part, _, domain = lowered.partition("@")
    user_norm = user_part
    if user_norm:
        if "+" in user_norm:
            user_norm = user_norm.split("+", 1)[0]
        user_norm = user_norm.replace(".", "")

    return lowered, domain, user_norm or None

def normalise_phone(raw: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {"phoneE164": None, "phoneCountryCode": None, "phoneDigitsOnly": None,
                "phoneAreaCode": None, "phoneLast4": None}

    try:
        parsed = phonenumbers.parse(raw, DEFAULT_REGION)
        if not (phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed)):
            raise Exception()
    except Exception:
        return {"phoneE164": None, "phoneCountryCode": None, "phoneDigitsOnly": None,
                "phoneAreaCode": None, "phoneLast4": None}

    e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    digits = re.sub(r"\D", "", phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL))

    return {
        "phoneE164": e164,
        "phoneCountryCode": str(parsed.country_code),
        "phoneDigitsOnly": digits,
        "phoneAreaCode": digits[:3] if len(digits) >= 3 else None,
        "phoneLast4": digits[-4:] if len(digits) >= 4 else None
    }


def parse_postcode(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {"postcode": None, "postcodeLower": None, "postcodeSector": None, "postcodeArea": None}

    normalized = re.sub(r"\s+", " ", cleaned.upper())
    parts = normalized.split(" ")
    outward = parts[0] if parts else None
    inward = parts[1] if len(parts) > 1 else ""
    sector = f"{outward} {inward[:1]}" if outward and inward else outward
    m = re.match(r"([A-Z]+)", outward or "")
    area = m.group(1) if m else None

    return {
        "postcode": normalized,
        "postcodeLower": normalized.lower(),
        "postcodeSector": sector,
        "postcodeArea": area,
    }


def parse_address(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {"houseNumber": None, "streetLower": None, "addressTokens": []}

    house, street = None, cleaned
    m = re.match(r"^(\d+)[\s,]+(.+)$", cleaned)
    if m:
        house = m.group(1)
        street = m.group(2)

    return {
        "houseNumber": house,
        "streetLower": street.lower(),
        "addressTokens": tokenize(cleaned),
    }


def parse_dob(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {"dobDateOnly": None, "dobYearOnly": None, "dobMonthDayOnly": None, "ageBucket": None}

    try:
        dt = datetime.fromisoformat(cleaned)
    except Exception:
        dt = None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(cleaned, fmt)
                break
            except ValueError:
                continue

    if not dt:
        return {"dobDateOnly": None, "dobYearOnly": None, "dobMonthDayOnly": None, "ageBucket": None}

    dob = dt.date()
    return {
        "dobDateOnly": dob.isoformat(),
        "dobYearOnly": str(dob.year),
        "dobMonthDayOnly": dob.strftime("%m-%d"),
        "ageBucket": compute_age_bucket(dob),
    }


def compute_age_bucket(d: date) -> str:
    today = date.today()
    age = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
    if age < 18: return "0-17"
    if age < 25: return "18-24"
    if age < 35: return "25-34"
    if age < 45: return "35-44"
    if age < 55: return "45-54"
    if age < 65: return "55-64"
    if age < 75: return "65-74"
    return "75+"


# ---------------------------------------------------------------------------
# Embedding helper CocoIndex
# ---------------------------------------------------------------------------

def embed_safe(text: Optional[str]) -> Optional[List[float]]:
    """Embed text using CocoIndex, but fail gracefully."""
    if not COCO_ENABLED or not text:
        return None
    try:
        return coco.embed_text(text)
    except Exception as e:
        print("[cleaner] CocoIndex embedding error:", e, file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# MAIN cleaning
# ---------------------------------------------------------------------------

def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}

    # ---- NAME ----
    first_raw, last_raw = rec.get("firstName"), rec.get("lastName")
    first_lower, last_lower = lower_str(first_raw), lower_str(last_raw)
    first_dm1, _ = metaphones(first_raw)
    last_dm1, _ = metaphones(last_raw)

    cleaned.update({
        "firstName": clean_str(first_raw),
        "lastName": clean_str(last_raw),
        "firstNameLower": first_lower,
        "lastNameLower": last_lower,
        "firstNameInitial": initial(first_raw),
        "lastNameInitial": initial(last_raw),
        "firstNameMetaphone1": first_dm1,
        "lastNameMetaphone1": last_dm1,
    })

    # Prefix / suffix parsing
    name_obj = HumanName(" ".join([p for p in [rec.get("title"), first_raw, last_raw] if p]))
    cleaned["namePrefix"] = lower_str(name_obj.title)
    cleaned["nameSuffix"] = lower_str(name_obj.suffix)

    cleaned["nameTokens"] = tokenize(f"{first_raw} {last_raw}")

    # ---- DOB ----
    cleaned.update(parse_dob(rec.get("dob")))

    # ---- EMAIL ----
    email_lower, email_domain, email_user_norm = normalize_email(rec.get("email"))
    cleaned.update({
        "emailLower": email_lower,
        "emailDomain": email_domain,
        "emailUserPartNormalized": email_user_norm,
    })

    # ---- PHONE ----
    cleaned.update(normalize_phone(rec.get("phone")))

    # ---- ADDRESS ----
    addr = parse_address(rec.get("address"))
    cleaned.update(addr)

    # ---- POSTCODE ----
    cleaned.update(parse_postcode(rec.get("postcode")))

    cleaned["cityLower"] = lower_str(rec.get("city"))
    cleaned["countyLower"] = lower_str(rec.get("county"))

    # ---- HASHES ----
    cleaned["personNameHash"] = (
        sha256(f"{first_lower}|{last_lower}") if first_lower or last_lower else None
    )
    cleaned["personAddressHash"] = (
        sha256("|".join(filter(None, [
            addr.get("houseNumber"), addr.get("streetLower"), cleaned.get("postcodeLower")
        ])))
        if addr.get("houseNumber") or addr.get("streetLower") or cleaned.get("postcodeLower")
        else None
    )

    # ---- BLOCKING KEYS ----
    cleaned.update(compound_keys(cleaned))

# -------------------------------------------------------------------
# COCO Embeddings
# Reminder to self:
# You stripped address from signature because it was noisy
# Don't embed DOB, send to ML as a feature as is
# -------------------------------------------------------------------

    # 1) Name embdding
    full_name = " ".join([cleaned.get("firstNameLower") or "",
                          cleaned.get("lastNameLower") or ""]).strip()
    cleaned["nameEmbedding"] = embed_safe(full_name)

    # 2) Email embedding
    if cleaned.get("emailLower"):
        cleaned["emailEmbedding"] = embed_safe(
            f"{email_user_norm or ''} {email_domain or ''}"
        )
    else:
        cleaned["emailEmbedding"] = None

    # 3) Address embedding
    address_text = " ".join(filter(None, [
        addr.get("houseNumber"),
        addr.get("streetLower"),
        cleaned.get("postcodeLower"),
    ]))
    cleaned["addressEmbedding"] = embed_safe(address_text)

    # 4) Phone embedding
    phone_text = " ".join(filter(None, [
        cleaned.get("phoneCountryCode"),
        cleaned.get("phoneDigitsOnly"),
    ]))
    cleaned["phoneEmbedding"] = embed_safe(phone_text)

    # 5) Signature embedding
    signature = " ".join(filter(None, [
        cleaned.get("firstNameLower"),
        cleaned.get("lastNameLower"),
        cleaned.get("emailUserPartNormalized"),
        cleaned.get("phoneDigitsOnly"),
        cleaned.get("postcodeLower")
    ]))
    cleaned["signatureEmbedding"] = embed_safe(signature)

    return cleaned

# ---------------------------------------------------------------------------
# Compound keys
# ---------------------------------------------------------------------------

def compound_keys(data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    def join(*parts: Optional[str]) -> Optional[str]:
        if any(part is None for part in parts):
            return None
        return "|".join(str(part) for part in parts)

    keys: Dict[str, Optional[str]] = {}

    keys["bk_deterministic_res"] = join(
        data.get("firstNameLower"),
        data.get("lastNameLower"),
        data.get("dobDateOnly"),
        data.get("phoneE164"),
        data.get("streetLower"),
        data.get("postcodeLower"),
        data.get("emailLower")
    )
    keys["bk_lname_dob"] = join(data.get("lastNameLower"), data.get("dobDateOnly"))
    keys["bk_lname_init_dobY"] = join(data.get("lastNameInitial"), data.get("dobYearOnly"))
    keys["bk_lname_dm_dobY"] = join(data.get("lastNameMetaphone1"), data.get("dobYearOnly"))
    keys["bk_fname_init_lname"] = join(data.get("firstNameInitial"), data.get("lastNameLower"))
    keys["bk_fname_lower_lname_lower"] = join(
        data.get("firstNameLower"), data.get("lastNameLower")
    )
    keys["bk_fname_metaphone_lname"] = join(
        data.get("firstNameMetaphone1"), data.get("lastNameLower")
    )
    keys["bk_fname_metaphone1_lname_metaphone1"] = join(
        data.get("firstNameMetaphone1"), data.get("lastNameMetaphone1")
    )
    keys["bk_fname_initial_lname_initial"] = join(
        data.get("firstNameInitial"), data.get("lastNameInitial")
    )
    keys["bk_fname_initial_lname_lower_dobY"] = join(
        data.get("firstNameInitial"), data.get("lastNameLower"), data.get("dobYearOnly")
    )
    keys["bk_person_namehash_dob"] = join(
        data.get("personNameHash"), data.get("dobDateOnly")
    )
    keys["bk_person_namehash_phone_last4"] = join(
        data.get("personNameHash"), data.get("phoneLast4")
    )
    keys["bk_person_namehash_email_domain"] = join(
        data.get("personNameHash"), data.get("emailDomain")
    )

    keys["bk_dob_phone"] = join(data.get("dobDateOnly"), data.get("phoneE164"))
    keys["bk_dob_email"] = join(data.get("dobDateOnly"), data.get("emailLower"))
    keys["bk_dob_house_postcode"] = join(
        data.get("dobDateOnly"), data.get("houseNumber"), data.get("postcodeLower")
    )
    keys["bk_dobY_lname_metaphone1"] = join(
        data.get("dobYearOnly"), data.get("lastNameMetaphone1")
    )
    keys["bk_dobY_lname_lower"] = join(data.get("dobYearOnly"), data.get("lastNameLower"))
    keys["bk_dobY_phone_last4"] = join(data.get("dobYearOnly"), data.get("phoneLast4"))
    keys["bk_ageBucket_lname_initial"] = join(
        data.get("ageBucket"), data.get("lastNameInitial")
    )
    keys["bk_ageBucket_lname_metaphone1"] = join(
        data.get("ageBucket"), data.get("lastNameMetaphone1")
    )
    keys["bk_ageBucket_phone_country"] = join(
        data.get("ageBucket"), data.get("phoneCountryCode")
    )

    keys["bk_email_dob"] = keys.get("bk_dob_email")
    keys["bk_email_domain_surname"] = join(
        data.get("emailDomain"), data.get("lastNameLower")
    )
    keys["bk_email_domain_dobY"] = join(
        data.get("emailDomain"), data.get("dobYearOnly")
    )
    email_user_dm1, _ = metaphones(data.get("emailUserPartNormalized"))
    keys["bk_email_user_dmY"] = join(email_user_dm1, data.get("dobYearOnly"))
    keys["bk_email_user_surname_lower"] = join(
        data.get("emailUserPartNormalized"), data.get("lastNameLower")
    )
    keys["bk_email_domain_phone_last4"] = join(
        data.get("emailDomain"), data.get("phoneLast4")
    )
    keys["bk_email_domain_fname_initial"] = join(
        data.get("emailDomain"), data.get("firstNameInitial")
    )
    keys["bk_email_domain_lname_initial_dobY"] = join(
        data.get("emailDomain"), data.get("lastNameInitial"), data.get("dobYearOnly")
    )
    keys["bk_email_domain_lname_metaphone1_dobY"] = join(
        data.get("emailDomain"), data.get("lastNameMetaphone1"), data.get("dobYearOnly")
    )
    keys["bk_email_lower_surname_lower"] = join(
        data.get("emailLower"), data.get("lastNameLower")
    )

    keys["bk_phone_dob"] = join(data.get("phoneE164"), data.get("dobDateOnly"))
    keys["bk_phone_last4_dobY"] = join(data.get("phoneLast4"), data.get("dobYearOnly"))
    keys["bk_phone_digits_surname_initial"] = join(
        data.get("phoneDigitsOnly"), data.get("lastNameInitial")
    )
    keys["bk_phone_DM_surnameY"] = join(
        data.get("phoneDigitsOnly"), data.get("lastNameMetaphone1"), data.get("dobYearOnly")
    )
    keys["bk_phone_country_surname_initial"] = join(
        data.get("phoneCountryCode"), data.get("lastNameInitial")
    )
    keys["bk_phone_country_surname_metaphone1"] = join(
        data.get("phoneCountryCode"), data.get("lastNameMetaphone1")
    )
    keys["bk_phoneE164_lname_initial"] = join(
        data.get("phoneE164"), data.get("lastNameInitial")
    )
    keys["bk_phoneE164_lname_metaphone1"] = join(
        data.get("phoneE164"), data.get("lastNameMetaphone1")
    )
    keys["bk_phone_last4_email_domain"] = join(
        data.get("phoneLast4"), data.get("emailDomain")
    )

    keys["bk_house_postcode"] = join(
        data.get("houseNumber"), data.get("postcodeLower")
    )
    keys["bk_house_surname_initial"] = join(
        data.get("houseNumber"), data.get("lastNameInitial")
    )
    keys["bk_house_surnameDM_dobY"] = join(
        data.get("houseNumber"), data.get("lastNameMetaphone1"), data.get("dobYearOnly")
    )
    keys["bk_house_lname_initial_fname_initial"] = join(
        data.get("houseNumber"), data.get("lastNameInitial"), data.get("firstNameInitial")
    )
    keys["bk_house_fname_initial_street_lower"] = join(
        data.get("houseNumber"), data.get("firstNameInitial"), data.get("streetLower")
    )
    first_street_token = None
    street_lower = data.get("streetLower") or ""
    street_tokens = street_lower.split()
    if street_tokens:
        first_street_token = street_tokens[0]
    keys["bk_houseNumber_street_firstToken_postcodeSector"] = join(
        data.get("houseNumber"), first_street_token, data.get("postcodeSector")
    )
    keys["bk_postcodeArea_surname_initial_dobY"] = join(
        data.get("postcodeArea"), data.get("lastNameInitial"), data.get("dobYearOnly")
    )
    keys["bk_postcodeSector_surname_initial"] = join(
        data.get("postcodeSector"), data.get("lastNameInitial")
    )
    keys["bk_postcodeSector_dobY_surname_metaphone1"] = join(
        data.get("postcodeSector"), data.get("dobYearOnly"), data.get("lastNameMetaphone1")
    )
    keys["bk_house_postcode_dob"] = join(
        data.get("houseNumber"), data.get("postcodeLower"), data.get("dobDateOnly")
    )

    return keys

# ---------------------------------------------------------------------------
# Main Kafka loop
# ---------------------------------------------------------------------------

def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "lz4",
    })

    print(f"[cleaner] Consuming {RAW_TOPIC}, producing {ENR_TOPIC}")
    consumer.subscribe([RAW_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg:
                continue
            if msg.error():
                print("Consumer error:", msg.error(), file=sys.stderr)
                continue

            try:
                rec = json.loads(msg.value())
            except Exception:
                print("[cleaner] Bad JSON", file=sys.stderr)
                continue

            payload = unwrap_payload(rec)
            if not isinstance(payload, dict):
                print("[cleaner] No usable payload", file=sys.stderr)
                continue

            enriched = clean_record(payload)

            key = msg.key()
            if isinstance(key, (bytes, bytearray)):
                key_val = key.decode("utf-8", errors="ignore")
            else:
                key_val = enriched.get("id") or ""

            if "id" not in enriched:
                enriched["id"] = key_val

            producer.produce(
                ENR_TOPIC,
                key=f"customers:{key_val}".encode("utf-8"),
                value=json.dumps(enriched).encode("utf-8"),
            )
            producer.poll(0)

    except KeyboardInterrupt:
        print("Shutting down cleaner...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
