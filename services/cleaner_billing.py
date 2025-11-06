from confluent_kafka import Consumer, Producer
import hashlib
import json
import os
import re
from typing import Optional
from metaphone import doublemetaphone
import phonenumbers
from ollama import chat
from pydantic import BaseModel

RAW_TOPIC = "raw.customer.billing"
ENR_TOPIC = "enriched.customer.billing"
BOOTSTRAP = "localhost:9092"

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "cleaner-billing",
    "auto.offset.reset": "earliest",
})
p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "linger.ms": 10,
    "compression.type": "lz4",
})

email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

country_aliases = {
    "UK": "UNITED KINGDOM",
    "UNITED KINGDOM": "UNITED KINGDOM",
    "GREAT BRITAIN": "UNITED KINGDOM",
    "ENGLAND": "UNITED KINGDOM",
    "SCOTLAND": "UNITED KINGDOM",
    "WALES": "UNITED KINGDOM",
    "NORTHERN IRELAND": "UNITED KINGDOM",
}

DEFAULT_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

class StandardUkAddress(BaseModel):
    company_name: Optional[str] = None
    department: Optional[str] = None
    address_line_1: Optional[str] = None
    address_line_2: Optional[str] = None
    post_town: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = "UNITED KINGDOM"

def _normalise_postcode(pc: Optional[str]) -> Optional[str]:
    if not pc:
        return None
    clean = re.sub(r"\s+", "", pc.upper())
    if len(clean) <= 3:
        return clean
    return f"{clean[:-3]} {clean[-3:]}"

def _normalised_country(country: Optional[str]) -> str:
    if not country:
        return "UNITED KINGDOM"
    upper = country.upper()
    return country_aliases.get(upper, upper)

def _default_uk_address(postcode: Optional[str], country: Optional[str]) -> dict:
    return {
        "company_name": None,
        "department": None,
        "address_line_1": None,
        "address_line_2": None,
        "post_town": None,
        "postal_code": _normalise_postcode(postcode),
        "country": _normalised_country(country),
    }

def parse_uk_address_with_ollama(address: Optional[str], postcode: Optional[str], country: Optional[str]) -> dict:
    if chat is None:
        return _default_uk_address(postcode, country)

    if not address and not postcode:
        return _default_uk_address(postcode, country)

    prompt_lines = [
        "Convert the provided free-form address into structured UK address fields.",
        "If a value is missing, set it to null.",
        "Postal codes and country must be uppercase and UK postal codes require a space before the final three characters.",
        f"Raw address: {address or ''}",
    ]
    if postcode:
        prompt_lines.append(f"Known postcode: {postcode}")
    if country:
        prompt_lines.append(f"Known country: {country}")
    prompt_lines.append("Return only the JSON that matches the provided schema.")

    fallback = _default_uk_address(postcode, country)
    try:
        response = chat(
            model=DEFAULT_OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": "You are an assistant that standardises UK postal addresses for billing records."},
                {"role": "user", "content": "\n".join(prompt_lines)},
            ],
            format=StandardUkAddress.model_json_schema(),
        )
        parsed = StandardUkAddress.model_validate_json(response.message.content).model_dump()
        for key, value in list(parsed.items()):
            if isinstance(value, str):
                stripped = value.strip()
                parsed[key] = stripped or None
    except Exception:
        return fallback

    if parsed.get("postal_code"):
        parsed["postal_code"] = _normalise_postcode(parsed["postal_code"])
    elif fallback["postal_code"]:
        parsed["postal_code"] = fallback["postal_code"]

    parsed["country"] = _normalised_country(parsed.get("country")) if parsed.get("country") else fallback["country"]
    return parsed

def norm_phone(s, region="GB"):
    try:
        num = phonenumbers.parse(s, region)
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None

def clean(rec):
    out = {}
    # required system origin
    out["source"] = "billing"
    out["source_id"] = str(rec.get("source_id") or "").strip()

    # names
    given = str(rec.get("given_name") or "").strip().title()
    family = str(rec.get("family_name") or "").strip().title()
    out["given_name"] = given or None
    out["family_name"] = family or None
    out["given_name_phonetic"] = doublemetaphone(given)[0] if given else None
    out["surname_phonetic"] = doublemetaphone(family)[0] if family else None

    # dob (keep as-is if already ISO)
    dob = str(rec.get("dob") or "").strip()
    out["dob"] = dob or None

    # email
    email = str(rec.get("email") or "").strip().lower()
    out["email"] = email if email_re.match(email) else None

    # phone
    out["phone_e164"] = norm_phone(rec.get("phone") or "")

    # postcode (UK simple normalisation)
    pc = str(rec.get("postcode") or "").upper().replace(" ", "")
    if len(pc) >= 5:
        out["postcode"] = pc[:-3] + " " + pc[-3:]
        out["postcode_sector"] = out["postcode"].split(" ")[0]
    else:
        out["postcode"] = None
        out["postcode_sector"] = None

    out["address_standard"] = parse_uk_address_with_ollama(
        rec.get("address"),
        out.get("postcode"),
        rec.get("country"),
    )

    # policy & national id
    policy = str(rec.get("policy_id") or "").upper().replace("-", "")
    out["policy_id"] = policy or None
    nid = str(rec.get("national_id") or "").strip()
    out["national_id_hash"] = hashlib.sha256(nid.encode()).hexdigest() if nid else None

    # blocking keys
    out["bk_nid_dob"]    = f"{out['national_id_hash']}|{out['dob']}" if out.get("national_id_hash") and out.get("dob") else None
    out["bk_email_dob"]  = f"{out['email']}|{out['dob']}" if out.get("email") and out.get("dob") else None
    out["bk_phone_sndx"] = f"{out['phone_e164']}|{out['surname_phonetic']}" if out.get("phone_e164") and out.get("surname_phonetic") else None
    out["bk_policy_dob"] = f"{out['policy_id']}|{out['dob']}" if out.get("policy_id") and out.get("dob") else None

    return out

def run():
    c.subscribe([RAW_TOPIC])
    print(f"Cleaning from {RAW_TOPIC} → {ENR_TOPIC}")
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            print("!", msg.error())
            continue
        try:
            rec = json.loads(msg.value().decode("utf-8"))
            cleaned = clean(rec)
            p.produce(ENR_TOPIC, json.dumps(cleaned).encode("utf-8"))
            p.poll(0)
        except Exception as e:
            print("! cleaning error:", e)

run()
