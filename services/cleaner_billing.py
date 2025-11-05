from confluent_kafka import Consumer, Producer
import json, re, hashlib
from metaphone import doublemetaphone
import phonenumbers

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
