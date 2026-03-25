#!/usr/bin/env python3
"""Generate synthetic healthcare data for Databricks interop demo."""

import csv
import os
import random
import uuid
from datetime import date, timedelta

random.seed(42)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "synthetic")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

FIRST_NAMES_MALE = [
    "James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph",
    "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
    "Donald", "Steven", "Andrew", "Paul", "Joshua", "Kenneth", "Kevin", "Brian",
    "George", "Timothy", "Ronald", "Edward", "Jason", "Jeffrey", "Ryan",
    "Jacob", "Gary", "Nicholas", "Eric", "Jonathan", "Stephen", "Larry",
    "Justin", "Scott", "Brandon", "Benjamin", "Samuel", "Raymond", "Gregory",
    "Frank", "Alexander", "Patrick", "Jack", "Dennis", "Jerry",
    "Carlos", "Juan", "Jose", "Miguel", "Luis", "Jorge", "Pedro", "Rafael",
    "Alejandro", "Diego", "Wei", "Jin", "Hiroshi", "Raj", "Amir",
    "Darnell", "DeShawn", "Terrence", "Jamal", "Andre", "Marcus", "Tyrone",
    "Kwame", "Dante", "Malik",
]

FIRST_NAMES_FEMALE = [
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
    "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
    "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
    "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
    "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna", "Brenda",
    "Pamela", "Emma", "Nicole", "Helen", "Samantha", "Katherine", "Christine",
    "Debra", "Rachel", "Carolyn", "Janet", "Catherine", "Maria", "Heather",
    "Maria-Elena", "Mary-Jo", "Ana", "Rosa", "Guadalupe", "Carmen", "Lucia",
    "Mei", "Yuki", "Priya", "Fatima", "Aisha",
    "Latasha", "Keisha", "Tamika", "Shaniqua", "Ebony", "Monique", "Jasmine",
    "Aaliyah", "Destiny", "Imani",
    "Jean-Marie", "Mary-Kate", "Anne-Marie", "Jo-Ann", "Betty-Lou",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris",
    "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan",
    "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos",
    "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez",
    "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long",
    "Ross", "Foster", "Jimenez", "Powell",
    "O'Brien", "O'Connor", "O'Neill", "O'Malley", "O'Sullivan",
    "McDonald", "MacDonald", "McConnell", "McKenzie", "MacArthur",
    "Van Der Berg", "De La Cruz", "Del Rosario",
]

MIDDLE_INITIALS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

CITIES_STATES_ZIPS = [
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"),
    ("Chicago", "IL", "60601"), ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75201"), ("San Jose", "CA", "95101"),
    ("Austin", "TX", "73301"), ("Jacksonville", "FL", "32099"),
    ("Columbus", "OH", "43004"), ("Charlotte", "NC", "28201"),
    ("Indianapolis", "IN", "46201"), ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80201"), ("Nashville", "TN", "37201"),
    ("Portland", "OR", "97201"), ("Las Vegas", "NV", "89101"),
    ("Detroit", "MI", "48201"), ("Memphis", "TN", "38101"),
    ("Baltimore", "MD", "21201"), ("Boston", "MA", "02101"),
    ("Atlanta", "GA", "30301"), ("Miami", "FL", "33101"),
    ("Minneapolis", "MN", "55401"), ("Cleveland", "OH", "44101"),
    ("Tampa", "FL", "33601"), ("St. Louis", "MO", "63101"),
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Elm St",
    "Washington Blvd", "Park Ave", "Lake Rd", "Hill St", "Church St",
    "Broad St", "Market St", "Spring St", "High St", "Union Ave",
    "Center St", "River Rd", "Forest Dr", "Meadow Ln", "Sunset Blvd",
    "Valley Rd", "Highland Ave", "Franklin St", "Jefferson Ave",
]

LANGUAGES = (
    ["English"] * 85 + ["Spanish"] * 10
    + ["Chinese", "Vietnamese", "Tagalog", "Korean", "French"]
)

RACE_ETHNICITY = (
    ["White"] * 58 + ["Hispanic/Latino"] * 19 + ["Black/African American"] * 13
    + ["Asian"] * 6 + ["Two or More Races"] * 3
    + ["Native American/Alaska Native"]
)

SOURCE_SYSTEMS = ["EPIC", "CERNER", "ALLSCRIPTS", "MANUAL_ENTRY"]
SOURCE_SYSTEM_WEIGHTS = [45, 30, 15, 10]

CPT_CODES = [
    "99213", "99214", "99215", "99203", "99204", "99205",
    "27447", "29881", "29880", "27130",
    "43239", "45380", "45385",
    "70553", "71046", "71250", "72148", "73721", "74177",
    "90837", "90834", "90847",
    "97110", "97140", "97530",
    "36415", "80053", "85025",
    "11042", "17000",
]

PROCEDURE_MODIFIERS = [None, None, None, None, "26", "TC", "59", "LT", "RT", "50"]

ICD10_CODES = [
    "M5416", "M5417", "M5412", "M5432",
    "E119", "E1165", "E1122", "E780", "E785",
    "I10", "I110", "I2510", "I4891",
    "J449", "J441", "J189", "J069",
    "K219", "K5900",
    "G4700", "G8929",
    "F329", "F419", "F1010", "F1020",
    "Z0000", "Z2389", "Z8673",
    "S72001A", "S82001A",
    "C5011", "C50912", "C61",
    "N179", "N186", "N390",
    "R1010", "R0600", "R0789", "R109",
    "L03116",
]

STATUS_POOL = (
    ["Approved"] * 45 + ["Pending"] * 25 + ["Denied"] * 15
    + ["Partial"] * 8 + ["Pended"] * 4 + ["Cancelled"] * 2 + ["Expired"] * 1
)

DENIAL_CODES = ["96", "97", "197", "204", "18", "50", "29", "16", "4", "252"]

DOC_MATCH_METHODS = ["CoverSheet"] * 40 + ["OCR"] * 30 + ["FaxMeta"] * 15 + ["Manual"] * 15

PROVIDER_FIRST = [
    "James", "Sarah", "Michael", "Jennifer", "Robert", "Linda", "David",
    "Elizabeth", "William", "Maria", "Richard", "Susan", "Joseph", "Karen",
    "Thomas", "Nancy", "Charles", "Lisa", "Christopher", "Patricia",
    "Raj", "Priya", "Wei", "Mei", "Hiroshi", "Ahmed", "Fatima",
    "Carlos", "Ana", "Sergei",
]

PROVIDER_LAST = [
    "Patel", "Kim", "Chen", "Singh", "Gupta", "Shah", "Williams",
    "Johnson", "Smith", "Anderson", "Thompson", "Martinez", "Rodriguez",
    "Lee", "Park", "Nakamura", "Tanaka", "Ivanov", "Kowalski", "Muller",
    "Jones", "Brown", "Davis", "Wilson", "Moore", "Taylor", "White",
    "Harris", "Clark", "Lewis",
]

SYSTEM_USERS = [
    "sys_autoauth", "sys_intake", "jsmith_rn", "mwilliams_rn",
    "kpatel_md", "lchen_md", "admin_batch", "edi_processor",
    "fax_intake", "portal_submit",
]

FORM_TYPES_POOL = (
    ["prior_auth_form"] * 40 + ["clinical_note"] * 30
    + ["lab_result"] * 15 + ["imaging_report"] * 10 + ["discharge_summary"] * 5
)

SOURCE_CHANNEL_POOL = ["fax"] * 45 + ["electronic"] * 35 + ["upload"] * 15 + ["mail"] * 5

NAME_VARIANTS = {
    "McDonald": "MacDonald", "MacDonald": "McDonald",
    "Smith": "Smyth", "Johnson": "Jonson", "Williams": "Willams",
    "Anderson": "Andersen", "Thompson": "Thomson", "Peterson": "Petersen",
    "Nelson": "Nelsen", "Hansen": "Hanson", "Larson": "Larsen",
    "Mitchell": "Mitchel", "Campbell": "Cambell", "Stewart": "Stuart",
    "Phillips": "Philips", "Bennett": "Bennet", "Hughes": "Hughs",
    "Kelly": "Kelley", "Myers": "Miers", "Morgan": "Morgen",
    "O'Brien": "Obrien", "O'Connor": "Oconnor", "O'Neill": "Oneil",
    "Garcia": "Garsia", "Martinez": "Martines", "Hernandez": "Hernandes",
    "Lopez": "Lopes", "Gonzalez": "Gonzales", "Sanchez": "Sanches",
    "Ramirez": "Ramires", "Nguyen": "Ngyuen", "Patel": "Pattel",
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def uuid_str():
    return str(uuid.uuid4())


def random_dob():
    today = date(2026, 3, 24)
    age = int(random.gauss(50, 15))
    age = max(1, min(100, age))
    days_offset = random.randint(0, 364)
    return today - timedelta(days=age * 365 + days_offset)


def random_phone():
    area = random.randint(200, 999)
    mid = random.randint(200, 999)
    last = random.randint(1000, 9999)
    return f"{area}{mid}{last}"


def random_email(first, last):
    clean_first = first.lower().replace("'", "").replace("-", "").replace(" ", "")
    clean_last = last.lower().replace("'", "").replace("-", "").replace(" ", "")
    domain = random.choice([
        "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com",
        "icloud.com", "mail.com", "protonmail.com",
    ])
    fmt = random.choice([
        f"{clean_first}.{clean_last}@{domain}",
        f"{clean_first[0]}{clean_last}@{domain}",
        f"{clean_first}{clean_last[0]}@{domain}",
        f"{clean_first}{random.randint(1, 99)}@{domain}",
    ])
    return fmt


def random_npi():
    return f"{random.randint(1000000000, 9999999999)}"


def random_provider_name():
    first = random.choice(PROVIDER_FIRST)
    last = random.choice(PROVIDER_LAST)
    suffix = random.choice(["MD", "MD", "MD", "DO"])
    return f"Dr. {first} {last}, {suffix}"


def random_address():
    num = random.randint(1, 9999)
    street = random.choice(STREET_NAMES)
    apt = ""
    if random.random() < 0.2:
        apt = f" Apt {random.randint(1, 999)}"
    return f"{num} {street}{apt}"


# ---------------------------------------------------------------------------
# Generate Members
# ---------------------------------------------------------------------------

def generate_members(n=1000):
    members = []
    for _ in range(n):
        mid = uuid_str()
        gender_roll = random.random()
        if gender_roll < 0.48:
            gender = "Male"
            first = random.choice(FIRST_NAMES_MALE)
        elif gender_roll < 0.97:
            gender = "Female"
            first = random.choice(FIRST_NAMES_FEMALE)
        elif gender_roll < 0.99:
            gender = "Other"
            first = random.choice(FIRST_NAMES_MALE + FIRST_NAMES_FEMALE)
        else:
            gender = "Unknown"
            first = random.choice(FIRST_NAMES_MALE + FIRST_NAMES_FEMALE)

        last = random.choice(LAST_NAMES)
        middle = random.choice(MIDDLE_INITIALS) if random.random() > 0.30 else ""
        dob = random_dob().strftime("%Y-%m-%d")
        ssn4 = f"{random.randint(0, 9999):04d}" if random.random() > 0.05 else ""
        city, state, zip_code = random.choice(CITIES_STATES_ZIPS)
        address = random_address()
        phone = random_phone() if random.random() > 0.10 else ""
        email = random_email(first, last) if random.random() > 0.20 else ""
        lang = random.choice(LANGUAGES)
        race = random.choice(RACE_ETHNICITY)
        source = random.choices(SOURCE_SYSTEMS, weights=SOURCE_SYSTEM_WEIGHTS, k=1)[0]

        members.append({
            "member_id": mid,
            "first_name": first,
            "last_name": last,
            "middle_initial": middle,
            "dob": dob,
            "gender": gender,
            "ssn4": ssn4,
            "address_line1": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "phone": phone,
            "email": email,
            "preferred_language": lang,
            "race_ethnicity": race,
            "source_system": source,
        })
    return members


# ---------------------------------------------------------------------------
# Generate Authorizations
# ---------------------------------------------------------------------------

def generate_authorizations(members, n=3000):
    member_ids = [m["member_id"] for m in members]
    auths = []
    today = date(2026, 3, 24)

    for i in range(n):
        aid = uuid_str()
        auth_number = f"AUTH-2026-{i + 1:06d}"
        mid = random.choice(member_ids)

        days_back = random.randint(0, 730)
        sfd = today - timedelta(days=days_back)
        service_from = sfd.strftime("%Y-%m-%d")

        if random.random() > 0.05:
            duration = random.randint(30, 365)
            service_to = (sfd + timedelta(days=duration)).strftime("%Y-%m-%d")
        else:
            service_to = ""

        proc_code = random.choice(CPT_CODES)
        proc_mod = random.choice(PROCEDURE_MODIFIERS) or ""
        diag_code = random.choice(ICD10_CODES)
        npi = random_npi()
        prov_name = random_provider_name()
        status = random.choice(STATUS_POOL)

        approved_units = ""
        if status in ("Approved", "Partial"):
            approved_units = str(random.randint(1, 30))

        denial_reason = ""
        if status == "Denied":
            denial_reason = random.choice(DENIAL_CODES)

        req_offset = random.randint(0, 14)
        auth_requested = (sfd - timedelta(days=req_offset)).strftime("%Y-%m-%d")

        if status not in ("Pending", "Pended"):
            dec_offset = random.randint(1, 10)
            auth_decision = (sfd - timedelta(days=req_offset) + timedelta(days=dec_offset)).strftime("%Y-%m-%d")
        else:
            auth_decision = ""

        doc_match = ""
        if random.random() < 0.70:
            doc_match = random.choice(DOC_MATCH_METHODS)

        pend_reason = ""
        if status in ("Pending", "Pended"):
            pend_reason = random.choice([
                "Missing clinical notes", "Awaiting peer review",
                "Incomplete documentation", "Provider callback needed",
                "Additional info requested", "Medical director review",
            ])

        created_by = random.choice(SYSTEM_USERS)

        auths.append({
            "auth_id": aid,
            "auth_number": auth_number,
            "member_id": mid,
            "service_from_date": service_from,
            "service_to_date": service_to,
            "procedure_code": proc_code,
            "procedure_modifier": proc_mod,
            "diagnosis_code": diag_code,
            "rendering_provider_npi": npi,
            "rendering_provider_name": prov_name,
            "status": status,
            "approved_units": approved_units,
            "denial_reason_code": denial_reason,
            "auth_requested_date": auth_requested,
            "auth_decision_date": auth_decision,
            "doc_match_method": doc_match,
            "pend_reason": pend_reason,
            "created_by": created_by,
        })
    return auths


# ---------------------------------------------------------------------------
# Generate Intake Forms (with error injection)
# ---------------------------------------------------------------------------

def transpose_dob(dob_str):
    parts = dob_str.split("-")
    if len(parts) != 3:
        return dob_str
    y, m, d = parts
    return f"{y}-{d}-{m}"


def variant_last_name(name):
    if name in NAME_VARIANTS:
        return NAME_VARIANTS[name]
    if len(name) > 4:
        idx = random.randint(1, len(name) - 2)
        if random.random() < 0.5:
            return name[:idx] + name[idx] + name[idx:]
        else:
            return name[:idx] + name[idx + 1:]
    return name


def generate_intake_forms(members, auths, n=6000):
    member_lookup = {m["member_id"]: m for m in members}
    auth_list = list(auths)
    member_list = list(members)

    docs = []
    for _ in range(n):
        doc_id = uuid_str()

        auth_rec = random.choice(auth_list)
        true_member_id = auth_rec["member_id"]
        member_rec = member_lookup[true_member_id]

        if random.random() > 0.20:
            true_auth_id = auth_rec["auth_id"]
        else:
            true_auth_id = ""

        form_type = random.choice(FORM_TYPES_POOL)
        source_channel = random.choice(SOURCE_CHANNEL_POOL)

        ext_first = member_rec["first_name"]
        ext_last = member_rec["last_name"]
        ext_dob = member_rec["dob"]
        ext_ssn4 = member_rec["ssn4"]
        ext_member_id = member_rec["member_id"] if random.random() < 0.6 else ""
        ext_auth_number = auth_rec["auth_number"] if true_auth_id and random.random() < 0.7 else ""

        provider_name = auth_rec["rendering_provider_name"]
        provider_npi = auth_rec["rendering_provider_npi"]

        sender_fax = ""
        if source_channel == "fax":
            sender_fax = random_phone()

        quality_score = min(1.0, max(0.0, random.gauss(0.85, 0.08)))
        is_readable = True
        error_type = "none"

        roll = random.random()

        if roll < 0.03:
            noise_member = random.choice(member_list)
            ext_first = noise_member["first_name"]
            ext_last = noise_member["last_name"]
            ext_dob = noise_member["dob"]
            ext_ssn4 = noise_member["ssn4"]
            error_type = "noise"
        elif roll < 0.13:
            ext_dob = transpose_dob(ext_dob)
            error_type = "transposed_dob"
        elif roll < 0.28:
            ext_last = variant_last_name(ext_last)
            error_type = "name_variant"
        elif roll < 0.33:
            ext_first, ext_last = ext_last, ext_first
            error_type = "swapped_names"
        elif roll < 0.41:
            ext_ssn4 = ""
            error_type = "missing_ssn4"

        if random.random() < 0.07:
            quality_score = round(random.uniform(0.05, 0.30), 2)
            is_readable = False
            if error_type == "none":
                error_type = "low_quality"

        quality_score = round(quality_score, 2)

        docs.append({
            "doc_id": doc_id,
            "true_member_id": true_member_id,
            "true_auth_id": true_auth_id,
            "form_type": form_type,
            "extracted_first_name": ext_first,
            "extracted_last_name": ext_last,
            "extracted_dob": ext_dob,
            "extracted_ssn4": ext_ssn4,
            "extracted_member_id_on_form": ext_member_id,
            "extracted_auth_number": ext_auth_number,
            "provider_name": provider_name,
            "provider_npi": provider_npi,
            "source_channel": source_channel,
            "sender_fax_number": sender_fax,
            "quality_score": quality_score,
            "is_readable": is_readable,
            "error_type": error_type,
        })
    return docs


# ---------------------------------------------------------------------------
# Write CSV helper
# ---------------------------------------------------------------------------

def write_csv(filepath, rows):
    if not rows:
        return
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows -> {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating synthetic healthcare data...")
    print()

    members = generate_members(1000)
    members_path = os.path.join(OUTPUT_DIR, "members_seed.csv")
    write_csv(members_path, members)

    auths = generate_authorizations(members, 3000)
    auths_path = os.path.join(OUTPUT_DIR, "authorizations_seed.csv")
    write_csv(auths_path, auths)

    docs = generate_intake_forms(members, auths, 6000)
    docs_path = os.path.join(OUTPUT_DIR, "intake_forms_structured.csv")
    write_csv(docs_path, docs)

    print()
    print("Done. Summary:")
    print(f"  Members:        {len(members):>6,}")
    print(f"  Authorizations: {len(auths):>6,}")
    print(f"  Intake Forms:   {len(docs):>6,}")


if __name__ == "__main__":
    main()
