#!/usr/bin/env python
"""
Cleaner for billing events enriched with SentenceTransformer embeddings.

Flow:
- Normalising / standardising raw customer fields
- Generating blocking keys
- Embedding name/email/address/phone/signature using SentenceTransformers
- Publishing enriched records to Kafka for Quine/GraphSAGE
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

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
    primary, secondary = doublemetaphone(cleaned)
    return primary or None, secondary or None


def tokenize(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]


# ---------------------------------------------------------------------------
# Unwrap Debezium / raw envelopes
# ---------------------------------------------------------------------------

def unwrap_payload(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extracts payload from raw events."""
    if not isinstance(rec, dict):
        return None

    # Some producers wrap the envelope inside {"value": {...}}
    if isinstance(rec.get("value"), dict):
        rec = rec["value"]

    # Support envelopes where payload is nested or top-level
    env = rec.get("payload") if isinstance(rec.get("payload"), dict) else rec

    op = env.get("op")
    after = env.get("after")
    before = env.get("before")

    if op:
        op = str(op).lower()
        if op in ("c", "u") and isinstance(after, dict):
            return after
        if op == "d" and isinstance(before, dict):
            return before

    # Fallbacks when op is missing
    if isinstance(after, dict):
        return after
    if isinstance(before, dict):
        return before

    # Already looks like the business payload
    if any(key in env for key in ("firstName", "lastName", "email", "id")):
        return env

    return None


# ---------------------------------------------------------------------------
# Email / phone / DOB / address normalisation
# ---------------------------------------------------------------------------

def normalize_email(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not raw:
        return None, None, None
    lowered = raw.strip().lower()
    if not EMAIL_RE.match(lowered):
        return None, None, None

    user_part, _, domain = lowered.partition("@")
    domain = domain or None
    user_normalized = user_part
    if user_normalized:
        if "+" in user_normalized:
            user_normalized = user_normalized.split("+", 1)[0]
        user_normalized = user_normalized.replace(".", "")

    return lowered, domain, user_normalized or None


def normalize_phone(raw: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {
            "phoneE164": None,
            "phoneCountryCode": None,
            "phoneDigitsOnly": None,
            "phoneAreaCode": None,
            "phoneLast4": None,
        }

    try:
        parsed = phonenumbers.parse(raw, DEFAULT_REGION)
        if not (phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed)):
            raise phonenumbers.NumberParseException(0, "Invalid number")
    except Exception:
        return {
            "phoneE164": None,
            "phoneCountryCode": None,
            "phoneDigitsOnly": None,
            "phoneAreaCode": None,
            "phoneLast4": None
        }

    e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    national_number = phonenumbers.format_number(
        parsed, phonenumbers.PhoneNumberFormat.NATIONAL
    )
    digits_only = re.sub(r"\D", "", national_number)

    return {
        "phoneE164": e164,
        "phoneCountryCode": str(parsed.country_code),
        "phoneDigitsOnly": digits_only,
        "phoneAreaCode": digits_only[:3] if len(digits_only) >= 3 else None,
        "phoneLast4": digits_only[-4:] if len(digits_only) >= 4 else None
    }


def parse_postcode(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {"postcode": None, "postcodeLower": None, "postcodeSector": None, "postcodeArea": None}

    normalized = re.sub(r"\s+", " ", cleaned.upper())
    parts = normalized.split(" ")
    outward = parts[0] if parts else None
    inward = parts[1] if len(parts) > 1 else ""
    sector = None
    if outward and inward:
        sector = f"{outward} {inward[:1]}"
    elif outward:
        sector = outward

    area_match = re.match(r"([A-Z]+)", outward or "")
    area = area_match.group(1) if area_match else None

    return {
        "postcode": normalized,
        "postcodeLower": normalized.lower(),
        "postcodeSector": sector,
        "postcodeArea": area,
    }


def parse_address(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {
            "houseNumber": None,
            "streetLower": None,
            "addressTokens": [],
        }

    house_number = None
    street = cleaned
    match = re.match(r"^(\d+)[\s,]+(.+)$", cleaned)
    if match:
        house_number = match.group(1)
        street = match.group(2)

    street_lower = street.lower()

    return {
        "houseNumber": house_number,
        "streetLower": street_lower,
        "addressTokens": tokenize(cleaned),
    }


def parse_dob(raw: Optional[str]) -> Dict[str, Optional[str]]:
    cleaned = clean_str(raw)
    if not cleaned:
        return {
            "dobDateOnly": None,
            "dobYearOnly": None,
            "dobMonthDayOnly": None,
            "ageBucket": None,
        }

    # Handle long fractional seconds if present
    normalized_cleaned = cleaned
    if "." in cleaned:
        main_part, frac_part = cleaned.split(".", 1)
        if frac_part and len(frac_part) > 6:
            normalized_cleaned = f"{main_part}.{frac_part[:6]}"

    dt: Optional[datetime] = None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(normalized_cleaned, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        try:
            dt = datetime.fromisoformat(normalized_cleaned)
        except ValueError:
            return {
                "dobDateOnly": None,
                "dobYearOnly": None,
                "dobMonthDayOnly": None,
                "ageBucket": None,
            }

    date_only = dt.date()
    dob_year = str(date_only.year)
    dob_month_day = date_only.strftime("%m-%d")
    age_bucket = compute_age_bucket(date_only)

    return {
        "dobDateOnly": date_only.isoformat(),
        "dobYearOnly": dob_year,
        "dobMonthDayOnly": dob_month_day,
        "ageBucket": age_bucket,
    }


def compute_age_bucket(dob: date) -> str:
    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    if age < 18:
        return "0-17"
    if age < 25:
        return "18-24"
    if age < 35:
        return "25-34"
    if age < 45:
        return "35-44"
    if age < 55:
        return "45-54"
    if age < 65:
        return "55-64"
    if age < 75:
        return "65-74"
    return "75+"


def name_tokens(first: Optional[str], last: Optional[str]) -> List[str]:
    tokens: List[str] = []
    tokens.extend(tokenize(first or ""))
    tokens.extend(tokenize(last or ""))
    return tokens


# ---------------------------------------------------------------------------
# Compound keys (blocking)
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
# MAIN CLEANING LOGIC
# ---------------------------------------------------------------------------

def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}

    # Name
    first_raw = rec.get("firstName")
    last_raw = rec.get("lastName")
    title_raw = rec.get("title")

    first_lower = lower_str(first_raw)
    last_lower = lower_str(last_raw)

    first_dm1, first_dm2 = metaphones(first_raw)
    last_dm1, last_dm2 = metaphones(last_raw)

    cleaned.update(
        {
            "firstName": clean_str(first_raw),
            "lastName": clean_str(last_raw),
            "firstNameLower": first_lower,
            "firstNameInitial": initial(first_raw),
            "firstNameMetaphone1": first_dm1,
            "firstNameMetaphone2": first_dm2,
            "lastNameLower": last_lower,
            "lastNameInitial": initial(last_raw),
            "lastNameMetaphone1": last_dm1,
            "lastNameMetaphone2": last_dm2,
        }
    )

    name_obj = HumanName(
        " ".join(part for part in [title_raw, first_raw, last_raw] if part)
    )
    cleaned["namePrefix"] = lower_str(name_obj.title)
    cleaned["nameSuffix"] = lower_str(name_obj.suffix)
    cleaned["titleNorm"] = lower_str(title_raw)
    cleaned["nameTokens"] = name_tokens(first_raw, last_raw)

    # DOB
    dob_parts = parse_dob(rec.get("dob"))
    cleaned.update(dob_parts)

    # Email
    email_lower, email_domain, email_user_norm = normalize_email(rec.get("email"))
    cleaned.update(
        {
            "emailLower": email_lower,
            "emailDomain": email_domain,
            "emailUserPartNormalized": email_user_norm,
        }
    )

    # Phone
    phone_parts = normalize_phone(rec.get("phone"))
    cleaned.update(phone_parts)

    # Address & postcode
    address_parts = parse_address(rec.get("address"))
    cleaned.update(address_parts)

    postcode_parts = parse_postcode(rec.get("postcode"))
    cleaned.update(postcode_parts)

    cleaned["cityLower"] = lower_str(rec.get("city"))
    cleaned["countyLower"] = lower_str(rec.get("county"))

    # Derived hashes
    cleaned["personNameHash"] = (
        sha256(f"{first_lower}|{last_lower}") if first_lower or last_lower else None
    )
    cleaned["personAddressHash"] = (
        sha256(
            "|".join(
                filter(
                    None,
                    [
                        address_parts.get("houseNumber"),
                        address_parts.get("streetLower"),
                        postcode_parts.get("postcodeLower"),
                    ],
                )
            )
        )
        if address_parts.get("houseNumber") or address_parts.get("streetLower") or postcode_parts.get("postcodeLower")
        else None
    )

    # Include original identifiers
    # custsourceid == id - This is the unique fork ID from Shadow Traffic, which enables tracing across events. It is not used in the graph or in the graph neural network.
    cleaned["custsourceid"] = clean_str(rec.get("id"))
    cleaned["eventid"] = rec.get("eventid")

    cleaned.update(compound_keys(cleaned))

    return cleaned


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

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)

    print(f"[cleaner] Consuming from {RAW_TOPIC}, producing to {ENR_TOPIC}")
    consumer.subscribe([RAW_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error(), file=sys.stderr)
                continue

            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as exc:
                print("Bad JSON:", exc, file=sys.stderr)
                continue

            payload = unwrap_payload(rec)
            if not isinstance(payload, dict):
                print("Skipping record without usable payload", file=sys.stderr)
                continue

            cleaned = clean_record(payload)

            # Derive outbound key from the incoming Kafka key (fallback to cleaned id)
            raw_key = msg.key()
            if isinstance(raw_key, (bytes, bytearray)):
                key_value = raw_key.decode("utf-8", errors="ignore")
            elif raw_key is None:
                key_value = cleaned.get("eventid") or ""
            else:
                key_value = str(raw_key)

            if not cleaned.get("eventid") and key_value:
                cleaned["eventid"] = key_value
            producer.produce(
                ENR_TOPIC,
                key=f"customers:{key_value}".encode("utf-8"),
                value=json.dumps(cleaned).encode("utf-8"),
            )
            producer.poll(0)

    except KeyboardInterrupt:
        print("Shutting down cleaner...")
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
