#!/usr/bin/env python3
"""
Generate clinical PDF and TIFF documents using ONLY the Python standard library.

Produces minimal but valid PDF and TIFF files with embedded clinical text from
the intake_forms_structured.csv seed data. No external dependencies required.

Usage:
    python3 generate_docs_standalone.py --pdf 60 --tiff 10 --output data/generated_docs/
"""

import argparse
import csv
import hashlib
import os
import random
import struct
import sys
import zlib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# Helper data for filling templates
# ---------------------------------------------------------------------------

HEALTH_PLAN_NAMES = [
    "Blue Cross Blue Shield of Illinois", "Aetna Health Plans",
    "UnitedHealthcare Choice Plus", "Cigna HealthSpring",
    "Humana Gold Plus", "Anthem Blue Cross", "Kaiser Permanente",
    "Molina Healthcare", "Centene Health Plan", "WellCare Health Plans",
]

FACILITY_NAMES = [
    "Regional Medical Center", "University Hospital",
    "St. Mary's Health System", "Memorial Hermann Hospital",
    "Northwestern Memorial", "Cedars-Sinai Medical Center",
    "Johns Hopkins Medicine", "Cleveland Clinic Florida",
    "Mercy General Hospital", "Providence Health Center",
    "Rush University Medical Center", "Duke University Hospital",
]

SPECIALTIES = [
    "Internal Medicine", "Orthopedic Surgery", "Cardiology",
    "Pulmonology", "Gastroenterology", "Neurology",
    "Oncology", "Rheumatology", "Endocrinology",
    "Pain Management", "Physical Medicine", "General Surgery",
]

DIAGNOSIS_DESCRIPTIONS = {
    "M5416": "Radiculopathy, lumbar region",
    "E119": "Type 2 diabetes mellitus without complications",
    "I10": "Essential hypertension",
    "J449": "Chronic obstructive pulmonary disease, unspecified",
    "K219": "Gastro-esophageal reflux disease without esophagitis",
    "M179": "Osteoarthritis of knee, unspecified",
    "G4700": "Insomnia, unspecified",
    "F329": "Major depressive disorder, single episode, unspecified",
    "N186": "End stage renal disease",
    "C509": "Malignant neoplasm of breast, unspecified",
    "Z87891": "Personal history of nicotine dependence",
    "R079": "Chest pain, unspecified",
}

PROCEDURE_DESCRIPTIONS = {
    "99213": "Office visit, established patient, low complexity",
    "99214": "Office visit, established patient, moderate complexity",
    "99215": "Office visit, established patient, high complexity",
    "27447": "Total knee arthroplasty",
    "29881": "Arthroscopy, knee, surgical",
    "43239": "Upper GI endoscopy with biopsy",
    "70553": "MRI brain without and with contrast",
    "71046": "Chest X-ray, 2 views",
    "93000": "Electrocardiogram, routine ECG",
    "90837": "Psychotherapy, 60 minutes",
    "97110": "Therapeutic exercises",
    "20610": "Arthrocentesis, major joint",
}

CLINICAL_JUSTIFICATIONS = [
    "Patient has failed conservative management over the past 6 weeks including "
    "physical therapy and oral anti-inflammatory medications. Imaging confirms "
    "progressive deterioration requiring the requested intervention.",
    "Medical necessity is established by persistent symptoms despite 3 months of "
    "first-line therapy. Lab results demonstrate elevated inflammatory markers "
    "consistent with active disease.",
    "The patient presents with severe, debilitating symptoms that significantly "
    "limit activities of daily living. Prior treatments including OTC medications "
    "and prescription therapy have been insufficient.",
    "Clinical evaluation reveals worsening condition despite compliance with "
    "prescribed treatment plan. Current presentation meets criteria for the "
    "requested procedure per clinical guidelines.",
    "Patient has documented history of treatment failure with two prior agents "
    "in the same class. Step therapy requirements have been met. Current "
    "severity score indicates need for escalation of care.",
]

CHIEF_COMPLAINTS = [
    "Persistent lower back pain radiating to left leg for 8 weeks",
    "Worsening shortness of breath with exertion over past month",
    "Right knee pain and swelling limiting mobility",
    "Recurrent chest pain with activity, resolved with rest",
    "Chronic fatigue and unintentional weight loss of 15 lbs",
    "Progressive difficulty swallowing solid foods",
    "Bilateral hand joint stiffness and swelling, worse in mornings",
    "Recurring headaches with visual aura, increasing frequency",
]


def build_placeholders(row: dict, rng: random.Random) -> dict:
    """Build placeholder dict from a CSV row."""
    def get(keys, default=""):
        if isinstance(keys, str):
            keys = [keys]
        for k in keys:
            v = row.get(k, "").strip()
            if v:
                return v
        return default

    diag = get(["diagnosis_code"], "M5416")
    proc = get(["procedure_code"], "99213")

    return {
        "FIRST_NAME": get(["extracted_first_name", "first_name"], "Jane"),
        "LAST_NAME": get(["extracted_last_name", "last_name"], "Doe"),
        "MIDDLE_INITIAL": rng.choice("ABCDEFGHJKLMNPRST"),
        "DOB": get(["extracted_dob", "dob"], "1965-01-15"),
        "MEMBER_ID": get(["true_member_id", "member_id"], f"MBR{rng.randint(100000,999999)}"),
        "SSN4": get(["extracted_ssn4", "ssn4"], f"{rng.randint(1000,9999):04d}"),
        "GENDER": rng.choice(["Male", "Female"]),
        "PHONE": f"({rng.randint(200,999)}) {rng.randint(200,999)}-{rng.randint(1000,9999)}",
        "AUTH_NUMBER": get(["extracted_auth_number", "auth_number"], f"AUTH-2026-{rng.randint(100000,999999)}"),
        "PROVIDER_NAME": f"Dr. {rng.choice(['James','Sarah','Michael','Lisa','Robert','Jennifer'])} {rng.choice(['Anderson','Williams','Chen','Patel','Johnson','Thompson'])}, MD",
        "PROVIDER_NPI": get(["provider_npi"], f"{rng.randint(1000000000,9999999999)}"),
        "FACILITY_NAME": rng.choice(FACILITY_NAMES),
        "SPECIALTY": rng.choice(SPECIALTIES),
        "HEALTH_PLAN_NAME": rng.choice(HEALTH_PLAN_NAMES),
        "DIAGNOSIS_CODE": diag,
        "DIAGNOSIS_DESC": DIAGNOSIS_DESCRIPTIONS.get(diag, "Unspecified diagnosis"),
        "PROCEDURE_CODE": proc,
        "PROCEDURE_DESC": PROCEDURE_DESCRIPTIONS.get(proc, "Unspecified procedure"),
        "SERVICE_FROM_DATE": get(["service_from_date"], "2026-01-15"),
        "SERVICE_TO_DATE": get(["service_to_date"], "2026-04-15"),
        "REQUESTED_UNITS": str(rng.randint(1, 24)),
        "SUBMISSION_DATE": "2026-03-20",
        "CLINICAL_JUSTIFICATION": rng.choice(CLINICAL_JUSTIFICATIONS),
        "CHIEF_COMPLAINT": rng.choice(CHIEF_COMPLAINTS),
        "ADDRESS": f"{rng.randint(100,9999)} {rng.choice(['Main','Oak','Elm','Cedar','Pine'])} {rng.choice(['St','Ave','Blvd','Dr','Ln'])}",
        "CITY": rng.choice(["Chicago", "Houston", "Phoenix", "Denver", "Austin", "Miami"]),
        "STATE": rng.choice(["IL", "TX", "AZ", "CO", "TX", "FL"]),
        "ZIP": f"{rng.randint(10000,99999)}",
    }


# ---------------------------------------------------------------------------
# Document text builders
# ---------------------------------------------------------------------------

def build_prior_auth_form(p: dict, rng: random.Random) -> str:
    variant = rng.randint(1, 3)
    if variant == 1:
        return f"""{'='*72}
                 PRIOR AUTHORIZATION REQUEST FORM
{'='*72}
                     {p['HEALTH_PLAN_NAME']}
                Member Services: 1-800-555-{rng.randint(1000,9999)}
{'='*72}

SECTION 1: MEMBER INFORMATION
------------------------------
Member Name:        {p['FIRST_NAME']} {p['MIDDLE_INITIAL']} {p['LAST_NAME']}
Date of Birth:      {p['DOB']}
Member ID:          {p['MEMBER_ID']}
SSN (Last 4):       {p['SSN4']}
Gender:             {p['GENDER']}
Phone:              {p['PHONE']}
Address:            {p['ADDRESS']}
                    {p['CITY']}, {p['STATE']} {p['ZIP']}

SECTION 2: AUTHORIZATION DETAILS
---------------------------------
Authorization #:    {p['AUTH_NUMBER']}
Service From Date:  {p['SERVICE_FROM_DATE']}
Service To Date:    {p['SERVICE_TO_DATE']}

SECTION 3: PROVIDER INFORMATION
--------------------------------
Rendering Provider: {p['PROVIDER_NAME']}
NPI:                {p['PROVIDER_NPI']}
Facility:           {p['FACILITY_NAME']}
Specialty:          {p['SPECIALTY']}

SECTION 4: CLINICAL INFORMATION
---------------------------------
Primary Diagnosis:  {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}
Procedure Code:     {p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}
Requested Units:    {p['REQUESTED_UNITS']}

SECTION 5: CLINICAL JUSTIFICATION
-----------------------------------
{p['CLINICAL_JUSTIFICATION']}

Provider Signature: _________________________  Date: {p['SUBMISSION_DATE']}

{'='*72}
                  FAX THIS FORM TO: 1-800-555-{rng.randint(1000,9999)}
{'='*72}
"""
    elif variant == 2:
        return f"""+{'='*66}+
|                    URGENT PRIOR AUTH REQUEST                       |
|                    {p['HEALTH_PLAN_NAME']:<46s}|
+{'='*66}+

REQUEST DATE: {p['SUBMISSION_DATE']}         AUTH REF: {p['AUTH_NUMBER']}

PATIENT:
  Name: {p['LAST_NAME']}, {p['FIRST_NAME']} {p['MIDDLE_INITIAL']}
  DOB:  {p['DOB']}          Gender: {p['GENDER']}
  ID#:  {p['MEMBER_ID']}    SSN4:   {p['SSN4']}
  Tel:  {p['PHONE']}

REQUESTING PROVIDER:
  {p['PROVIDER_NAME']} (NPI: {p['PROVIDER_NPI']})
  {p['FACILITY_NAME']}

SERVICE REQUESTED:
  CPT:  {p['PROCEDURE_CODE']}
  ICD:  {p['DIAGNOSIS_CODE']}
  From: {p['SERVICE_FROM_DATE']}  To: {p['SERVICE_TO_DATE']}
  Units: {p['REQUESTED_UNITS']}

CLINICAL RATIONALE:
{p['CLINICAL_JUSTIFICATION']}

Urgency: [ ] Standard (14 day)  [X] Urgent (72 hour)
+{'='*66}+
"""
    else:
        return f"""PA REQUEST

Pt: {p['FIRST_NAME']} {p['LAST_NAME']}
BD: {p['DOB']}
Mbr#: {p['MEMBER_ID']}

Auth: {p['AUTH_NUMBER']}
Dx: {p['DIAGNOSIS_CODE']}
Px: {p['PROCEDURE_CODE']}
Dr: {p['PROVIDER_NAME']}
NPI: {p['PROVIDER_NPI']}

Dates: {p['SERVICE_FROM_DATE']} - {p['SERVICE_TO_DATE']}

{p['CLINICAL_JUSTIFICATION']}
"""


def build_clinical_note(p: dict, rng: random.Random) -> str:
    variant = rng.randint(1, 2)
    if variant == 1:
        return f"""{'='*72}
                       CLINICAL PROGRESS NOTE
{'='*72}
Date of Service:    {p['SERVICE_FROM_DATE']}
Provider:           {p['PROVIDER_NAME']}, {p['SPECIALTY']}
NPI:                {p['PROVIDER_NPI']}
Facility:           {p['FACILITY_NAME']}

PATIENT INFORMATION:
  Name:             {p['FIRST_NAME']} {p['MIDDLE_INITIAL']} {p['LAST_NAME']}
  DOB:              {p['DOB']}
  MRN:              {p['MEMBER_ID']}

CHIEF COMPLAINT:
{p['CHIEF_COMPLAINT']}

HISTORY OF PRESENT ILLNESS:
Patient presents today for follow-up evaluation. {p['CLINICAL_JUSTIFICATION']}

ASSESSMENT:
  Primary Diagnosis: {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}

PLAN:
  - {p['PROCEDURE_CODE']}: {p['PROCEDURE_DESC']}
  - Requested authorization for {p['REQUESTED_UNITS']} units
  - Follow-up in {rng.randint(7,30)} days

Prior Authorization Reference: {p['AUTH_NUMBER']}

Electronically signed by {p['PROVIDER_NAME']}
Date: {p['SUBMISSION_DATE']}
{'='*72}
"""
    else:
        return f"""PROGRESS NOTE - {p['FACILITY_NAME']}

Patient: {p['LAST_NAME']}, {p['FIRST_NAME']}     DOB: {p['DOB']}
MRN: {p['MEMBER_ID']}                        Visit Date: {p['SERVICE_FROM_DATE']}

Attending: {p['PROVIDER_NAME']} (NPI {p['PROVIDER_NPI']})

S: {p['CHIEF_COMPLAINT']}

O: Vitals stable. Patient appears in no acute distress.

A: {p['DIAGNOSIS_CODE']} {p['DIAGNOSIS_DESC']}

P: Recommend {p['PROCEDURE_DESC']} ({p['PROCEDURE_CODE']})
   Units requested: {p['REQUESTED_UNITS']}
   Auth#: {p['AUTH_NUMBER']}
   Return visit: {rng.randint(7,30)} days

__{p['PROVIDER_NAME']}__
{p['SUBMISSION_DATE']}
"""


def build_lab_report(p: dict, rng: random.Random) -> str:
    tests = [
        ("WBC", f"{rng.uniform(4.0,11.0):.1f}", "4.5-11.0 K/uL", ""),
        ("RBC", f"{rng.uniform(4.0,5.5):.2f}", "4.20-5.40 M/uL", ""),
        ("Hemoglobin", f"{rng.uniform(12.0,17.0):.1f}", "12.0-17.5 g/dL", ""),
        ("Hematocrit", f"{rng.uniform(36.0,50.0):.1f}", "36.0-50.0 %", ""),
        ("Glucose", f"{rng.randint(70,200)}", "70-100 mg/dL", "H" if rng.random() > 0.5 else ""),
        ("BUN", f"{rng.randint(7,25)}", "7-20 mg/dL", ""),
        ("Creatinine", f"{rng.uniform(0.6,1.5):.2f}", "0.60-1.20 mg/dL", ""),
        ("eGFR", f"{rng.randint(60,120)}", ">60 mL/min", ""),
    ]

    lines = [
        f"                        LABORATORY REPORT",
        f"                        {p['FACILITY_NAME']}",
        f"",
        f"Patient: {p['FIRST_NAME']} {p['LAST_NAME']}        DOB: {p['DOB']}",
        f"ID: {p['MEMBER_ID']}",
        f"Collection: {p['SERVICE_FROM_DATE']}",
        f"",
        f"Ordering Provider: {p['PROVIDER_NAME']}  NPI: {p['PROVIDER_NPI']}",
        f"",
        f"{'Test':<20s} {'Result':<12s} {'Reference Range':<20s} {'Flag':<6s}",
        f"{'-'*60}",
    ]
    for test, val, ref, flag in tests:
        lines.append(f"{test:<20s} {val:<12s} {ref:<20s} {flag:<6s}")

    lines += [
        f"",
        f"Clinical Correlation: {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}",
        f"Authorization Reference: {p['AUTH_NUMBER']}",
        f"",
        f"Report generated: {p['SUBMISSION_DATE']}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Minimal PDF writer (no dependencies)
# ---------------------------------------------------------------------------

class MinimalPDF:
    """Generate a valid PDF file using raw PDF spec. No external libraries."""

    def __init__(self):
        self.objects = []
        self.pages = []

    def _add_object(self, content: str) -> int:
        self.objects.append(content)
        return len(self.objects)  # 1-based object number

    def add_text_page(self, text: str, font_size: int = 9):
        """Add a page with monospace text."""
        lines = text.split("\n")
        # Build text stream with line-by-line positioning
        stream_lines = []
        stream_lines.append(f"BT")
        stream_lines.append(f"/F1 {font_size} Tf")

        y = 780  # Start near top of page
        line_height = font_size + 2
        for line in lines:
            if y < 40:  # Near bottom, start new page
                stream_lines.append("ET")
                self._finish_page("\n".join(stream_lines))
                stream_lines = ["BT", f"/F1 {font_size} Tf"]
                y = 780

            # Escape special PDF characters
            safe_line = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            stream_lines.append(f"1 0 0 1 36 {y} Tm")
            stream_lines.append(f"({safe_line}) Tj")
            y -= line_height

        stream_lines.append("ET")
        self._finish_page("\n".join(stream_lines))

    def _finish_page(self, stream_content: str):
        self.pages.append(stream_content)

    def save(self, filepath: str):
        """Write the PDF to disk."""
        objects = []
        offsets = []

        # Object 1: Catalog
        objects.append("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
        # Object 2: Pages (placeholder — we'll fix the Kids list)
        objects.append("")  # placeholder
        # Object 3: Font
        objects.append("3 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>\nendobj\n")

        # Build page objects
        page_obj_nums = []
        next_obj = 4
        for page_stream in self.pages:
            content_obj = next_obj
            page_obj = next_obj + 1

            # Content stream
            stream_bytes = page_stream.encode("latin-1", errors="replace")
            objects.append(
                f"{content_obj} 0 obj\n"
                f"<< /Length {len(stream_bytes)} >>\n"
                f"stream\n"
                f"{page_stream}\n"
                f"endstream\n"
                f"endobj\n"
            )

            # Page object
            objects.append(
                f"{page_obj} 0 obj\n"
                f"<< /Type /Page /Parent 2 0 R "
                f"/MediaBox [0 0 612 792] "
                f"/Contents {content_obj} 0 R "
                f"/Resources << /Font << /F1 3 0 R >> >> >>\n"
                f"endobj\n"
            )
            page_obj_nums.append(page_obj)
            next_obj += 2

        # Fix Pages object (object 2)
        kids = " ".join(f"{n} 0 R" for n in page_obj_nums)
        objects[1] = (
            f"2 0 obj\n"
            f"<< /Type /Pages /Kids [{kids}] /Count {len(page_obj_nums)} >>\n"
            f"endobj\n"
        )

        # Write file
        with open(filepath, "wb") as f:
            f.write(b"%PDF-1.4\n")
            for i, obj in enumerate(objects):
                offsets.append(f.tell())
                f.write(obj.encode("latin-1", errors="replace"))

            # Cross-reference table
            xref_offset = f.tell()
            f.write(b"xref\n")
            f.write(f"0 {len(objects) + 1}\n".encode())
            f.write(b"0000000000 65535 f \n")
            for off in offsets:
                f.write(f"{off:010d} 00000 n \n".encode())

            # Trailer
            f.write(b"trailer\n")
            f.write(f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode())
            f.write(b"startxref\n")
            f.write(f"{xref_offset}\n".encode())
            f.write(b"%%EOF\n")


# ---------------------------------------------------------------------------
# Minimal TIFF writer (no dependencies)
# ---------------------------------------------------------------------------

def text_to_bitmap(text: str, width: int = 2550, line_height: int = 30, char_width: int = 15) -> bytes:
    """Convert text to a raw bitmap (1-bit per pixel, packbits-like).
    Returns raw uncompressed grayscale image bytes (8-bit)."""
    lines = text.split("\n")
    height = max(len(lines) * line_height + 100, 400)

    # Create a white image (255 = white for 8-bit grayscale)
    img = bytearray(b'\xff' * (width * height))

    # Simple bitmap font rendering — draw black pixels for each character
    # We use a very basic approach: each char is a small block of dark pixels
    y_offset = 50
    for line in lines:
        x_offset = 100
        for ch in line:
            if ch != ' ' and ch.isprintable():
                # Draw a small block for each character (crude but visible)
                for dy in range(2, line_height - 8):
                    for dx in range(1, char_width - 3):
                        py = y_offset + dy
                        px = x_offset + dx
                        if 0 <= py < height and 0 <= px < width:
                            # Create character-like patterns
                            if (dy + dx + ord(ch)) % 3 != 0:
                                img[py * width + px] = 0  # black
            x_offset += char_width
            if x_offset > width - 100:
                break
        y_offset += line_height
        if y_offset > height - 50:
            break

    return bytes(img), width, height


def write_tiff(filepath: str, image_data: bytes, width: int, height: int):
    """Write a minimal valid TIFF file (uncompressed 8-bit grayscale)."""
    # TIFF header
    header = struct.pack("<2sHI", b"II", 42, 8)  # Little-endian, TIFF magic, IFD offset

    # IFD entries (tag, type, count, value_or_offset)
    entries = [
        (256, 3, 1, width),          # ImageWidth
        (257, 3, 1, height),         # ImageLength
        (258, 3, 1, 8),             # BitsPerSample
        (259, 3, 1, 1),             # Compression = None
        (262, 3, 1, 1),             # PhotometricInterpretation = BlackIsZero
        (273, 4, 1, 0),             # StripOffsets (placeholder)
        (278, 3, 1, height),        # RowsPerStrip
        (279, 4, 1, len(image_data)),  # StripByteCounts
        (282, 5, 1, 0),             # XResolution (placeholder)
        (283, 5, 1, 0),             # YResolution (placeholder)
        (296, 3, 1, 2),             # ResolutionUnit = inch
    ]

    num_entries = len(entries)
    ifd_size = 2 + num_entries * 12 + 4  # count + entries + next_ifd
    data_start = 8 + ifd_size

    # Resolution rational values (300/1)
    res_offset = data_start
    res_data = struct.pack("<II", 300, 1)  # 300 DPI
    image_offset = data_start + len(res_data) * 2

    # Fix offsets
    fixed_entries = []
    for tag, typ, count, value in entries:
        if tag == 273:  # StripOffsets
            value = image_offset
        elif tag == 282:  # XResolution
            value = res_offset
        elif tag == 283:  # YResolution
            value = res_offset + 8
        fixed_entries.append((tag, typ, count, value))

    with open(filepath, "wb") as f:
        f.write(header)

        # IFD
        f.write(struct.pack("<H", num_entries))
        for tag, typ, count, value in fixed_entries:
            f.write(struct.pack("<HHII", tag, typ, count, value))
        f.write(struct.pack("<I", 0))  # Next IFD = 0 (none)

        # Resolution data
        f.write(res_data)  # XResolution
        f.write(res_data)  # YResolution

        # Image data
        f.write(image_data)


# ---------------------------------------------------------------------------
# Main generation logic
# ---------------------------------------------------------------------------

def generate_document(row: dict, row_index: int, output_dir: Path,
                      output_format: str = "pdf") -> str:
    """Generate a single document from a CSV row."""
    seed = int(hashlib.md5(f"{row_index}_{row.get('doc_id','')}".encode()).hexdigest(), 16) % (2**31)
    rng = random.Random(seed)

    form_type = row.get("form_type", "prior_auth_form").strip().lower()
    p = build_placeholders(row, rng)

    if "prior_auth" in form_type or "pa_" in form_type:
        text = build_prior_auth_form(p, rng)
    elif "clinical" in form_type or "note" in form_type:
        text = build_clinical_note(p, rng)
    elif "lab" in form_type:
        text = build_lab_report(p, rng)
    elif "imaging" in form_type:
        text = build_clinical_note(p, rng)  # reuse clinical note layout
    elif "discharge" in form_type:
        text = build_clinical_note(p, rng)
    else:
        text = build_prior_auth_form(p, rng)

    doc_id = row.get("doc_id", f"doc_{row_index:06d}")

    if output_format == "pdf":
        filename = f"{doc_id}.pdf"
        filepath = output_dir / filename
        pdf = MinimalPDF()
        pdf.add_text_page(text)
        pdf.save(str(filepath))
    elif output_format == "tiff":
        filename = f"{doc_id}.tiff"
        filepath = output_dir / filename
        img_data, w, h = text_to_bitmap(text, width=2550, line_height=30)
        write_tiff(str(filepath), img_data, w, h)
    else:
        raise ValueError(f"Unknown format: {output_format}")

    return filename


def main():
    parser = argparse.ArgumentParser(description="Generate clinical PDF and TIFF documents")
    parser.add_argument("--input", "-i",
                        default=str(PROJECT_ROOT / "data" / "synthetic" / "intake_forms_structured.csv"))
    parser.add_argument("--output", "-o",
                        default=str(PROJECT_ROOT / "data" / "generated_docs"))
    parser.add_argument("--pdf", type=int, default=60, help="Number of PDFs to generate")
    parser.add_argument("--tiff", type=int, default=10, help="Number of TIFFs to generate")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read CSV
    with open(args.input, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total_docs = args.pdf + args.tiff
    if total_docs > len(rows):
        print(f"Warning: requested {total_docs} docs but CSV only has {len(rows)} rows. Using first {len(rows)}.")
        total_docs = min(total_docs, len(rows))

    random.seed(42)
    # Shuffle to get diverse form types
    selected = rows[:total_docs]

    pdf_count = min(args.pdf, len(selected))
    tiff_count = min(args.tiff, len(selected) - pdf_count)

    print(f"Generating {pdf_count} PDFs and {tiff_count} TIFFs...")
    print(f"Output directory: {output_dir}")

    generated = []

    for i in range(pdf_count):
        fname = generate_document(selected[i], i, output_dir, "pdf")
        generated.append(fname)
        if (i + 1) % 10 == 0:
            print(f"  PDFs: {i + 1}/{pdf_count}")

    print(f"  PDFs: {pdf_count}/{pdf_count} done")

    for i in range(tiff_count):
        idx = pdf_count + i
        fname = generate_document(selected[idx], idx, output_dir, "tiff")
        generated.append(fname)

    print(f"  TIFFs: {tiff_count}/{tiff_count} done")

    print(f"\nGenerated {len(generated)} documents:")
    print(f"  PDFs:  {pdf_count}")
    print(f"  TIFFs: {tiff_count}")
    print(f"  Dir:   {output_dir}")

    # Print file sizes
    total_size = 0
    for fname in generated:
        fpath = output_dir / fname
        sz = fpath.stat().st_size
        total_size += sz
    print(f"  Total size: {total_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
