#!/usr/bin/env python3
"""
Generate 100 PDF clinical documents with high variation in where patient data appears.

Variations include:
- Data scattered across different sections (header, footer, body, sidebar annotations)
- Some fields in tables, some in free text paragraphs
- Cover sheets with member data vs. without
- Mixed formatting: structured forms, narrative notes, tabular lab reports
- Partial data: some docs missing entire sections
- Different field orderings and label conventions
- Handwritten-style notes (short, abbreviated)
- Multi-page documents with data split across pages
- Redacted/blacked-out sections (simulated)
- Foreign language headers with English content
"""

import csv
import hashlib
import os
import random
import struct
import sys
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Reuse helper data from the standalone generator
sys.path.insert(0, str(PROJECT_ROOT / "data" / "generation"))
from generate_docs_standalone import (
    HEALTH_PLAN_NAMES, FACILITY_NAMES, SPECIALTIES,
    DIAGNOSIS_DESCRIPTIONS, PROCEDURE_DESCRIPTIONS,
    CLINICAL_JUSTIFICATIONS, CHIEF_COMPLAINTS,
    build_placeholders, MinimalPDF,
)


# ── Additional layout templates with varied data placement ────────────────────

def layout_header_only(p, rng):
    """Patient data only in a header block — body is pure clinical narrative."""
    return f"""{'─'*72}
  {p['HEALTH_PLAN_NAME']}  |  Member: {p['FIRST_NAME']} {p['LAST_NAME']}  |  DOB: {p['DOB']}  |  ID: {p['MEMBER_ID']}
{'─'*72}

CLINICAL DOCUMENTATION

Date: {p['SERVICE_FROM_DATE']}
Provider: {p['PROVIDER_NAME']} ({p['PROVIDER_NPI']})
Facility: {p['FACILITY_NAME']}

{p['CLINICAL_JUSTIFICATION']}

The patient was evaluated and the following services are recommended:
- {p['PROCEDURE_CODE']}: {p['PROCEDURE_DESC']}
- Diagnosis: {p['DIAGNOSIS_CODE']} ({p['DIAGNOSIS_DESC']})
- Requested units: {p['REQUESTED_UNITS']}

Authorization Reference: {p['AUTH_NUMBER']}
"""


def layout_footer_only(p, rng):
    """Patient data buried at the bottom — clinical content first."""
    return f"""{p['FACILITY_NAME']}
Department of {p['SPECIALTY']}

CLINICAL ASSESSMENT AND PLAN

Chief Complaint: {p['CHIEF_COMPLAINT']}

History: {p['CLINICAL_JUSTIFICATION']}

Assessment:
  Diagnosis: {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}

Plan:
  Procedure: {p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}
  Units: {p['REQUESTED_UNITS']}
  Service Window: {p['SERVICE_FROM_DATE']} to {p['SERVICE_TO_DATE']}

Attending: {p['PROVIDER_NAME']}, NPI {p['PROVIDER_NPI']}
Date: {p['SUBMISSION_DATE']}

{'_'*72}
PATIENT IDENTIFICATION (for office use only)
  Name: {p['LAST_NAME']}, {p['FIRST_NAME']}
  DOB: {p['DOB']}    SSN4: {p['SSN4']}    MbrID: {p['MEMBER_ID']}
  Auth#: {p['AUTH_NUMBER']}
  Phone: {p['PHONE']}
"""


def layout_tabular(p, rng):
    """All data in a strict table format — no narrative."""
    rows = [
        ("Field", "Value"),
        ("Patient Last Name", p['LAST_NAME']),
        ("Patient First Name", p['FIRST_NAME']),
        ("Date of Birth", p['DOB']),
        ("Member ID", p['MEMBER_ID']),
        ("SSN Last 4", p['SSN4']),
        ("Gender", p['GENDER']),
        ("Phone", p['PHONE']),
        ("Address", f"{p['ADDRESS']}, {p['CITY']}, {p['STATE']} {p['ZIP']}"),
        ("", ""),
        ("Auth Number", p['AUTH_NUMBER']),
        ("Service From", p['SERVICE_FROM_DATE']),
        ("Service To", p['SERVICE_TO_DATE']),
        ("Procedure", f"{p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}"),
        ("Diagnosis", f"{p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}"),
        ("Units Requested", p['REQUESTED_UNITS']),
        ("", ""),
        ("Provider", p['PROVIDER_NAME']),
        ("NPI", p['PROVIDER_NPI']),
        ("Facility", p['FACILITY_NAME']),
        ("Specialty", p['SPECIALTY']),
    ]
    lines = [f"{'PRIOR AUTHORIZATION DATA FORM':^72}", "=" * 72, f"{'Health Plan: ' + p['HEALTH_PLAN_NAME']:^72}", ""]
    for field, val in rows:
        if not field:
            lines.append("")
        else:
            lines.append(f"  {field:<25s} | {val}")
    lines += ["", "=" * 72, f"Submitted: {p['SUBMISSION_DATE']}", ""]
    return "\n".join(lines)


def layout_narrative_embedded(p, rng):
    """Patient identifiers embedded naturally in a narrative paragraph."""
    return f"""{p['FACILITY_NAME']}
{p['SPECIALTY']} Department
{p['SUBMISSION_DATE']}

RE: Prior Authorization Request

To Whom It May Concern,

I am writing to request prior authorization for my patient, {p['FIRST_NAME']} {p['LAST_NAME']} \
(Date of Birth: {p['DOB']}, Member ID: {p['MEMBER_ID']}, SSN last four: {p['SSN4']}), \
who has been under my care at {p['FACILITY_NAME']} for the management of \
{p['DIAGNOSIS_DESC']} (ICD-10: {p['DIAGNOSIS_CODE']}).

After extensive conservative management, including {p['CLINICAL_JUSTIFICATION'].lower()}

I am requesting authorization for {p['PROCEDURE_DESC']} (CPT: {p['PROCEDURE_CODE']}), \
{p['REQUESTED_UNITS']} units, for the service period {p['SERVICE_FROM_DATE']} through \
{p['SERVICE_TO_DATE']}.

This request is submitted under authorization reference {p['AUTH_NUMBER']}.

Please contact my office at {p['PHONE']} with any questions.

Sincerely,
{p['PROVIDER_NAME']}
NPI: {p['PROVIDER_NPI']}
{p['FACILITY_NAME']}
"""


def layout_abbreviated_handwritten(p, rng):
    """Short, messy, handwritten-style note — minimal formatting."""
    return f"""pt: {p['FIRST_NAME'][0]}. {p['LAST_NAME']}
dob {p['DOB']}  mbr {p['MEMBER_ID']}
ssn4 {p['SSN4']}

dx: {p['DIAGNOSIS_CODE']}
px: {p['PROCEDURE_CODE']} x{p['REQUESTED_UNITS']}
auth {p['AUTH_NUMBER']}

{p['SERVICE_FROM_DATE']}-{p['SERVICE_TO_DATE']}

{rng.choice(['needs auth ASAP', 'urgent - pt in pain', 'routine f/u', 'step therapy completed', 'conservative tx failed'])}

- Dr {p['PROVIDER_NAME'].split()[-1]}
  npi {p['PROVIDER_NPI']}
"""


def layout_multipage_split(p, rng):
    """Data split across what would be multiple pages — cover + clinical + summary."""
    page1 = f"""{'*'*72}
           CONFIDENTIAL MEDICAL DOCUMENT
           {p['HEALTH_PLAN_NAME']}
{'*'*72}

TO:     Utilization Management Department
FROM:   {p['PROVIDER_NAME']} (NPI: {p['PROVIDER_NPI']})
FAX:    1-800-555-{rng.randint(1000,9999)}
DATE:   {p['SUBMISSION_DATE']}
PAGES:  3 (including cover)

RE: Authorization Request for {p['FIRST_NAME']} {p['LAST_NAME']}
    Member ID: {p['MEMBER_ID']}
    Auth Reference: {p['AUTH_NUMBER']}

This fax contains Protected Health Information (PHI).
{'*'*72}
"""
    page2 = f"""CLINICAL DOCUMENTATION - PAGE 2

Patient: {p['LAST_NAME']}, {p['FIRST_NAME']}
DOB: {p['DOB']}    Gender: {p['GENDER']}    SSN4: {p['SSN4']}

Presenting Complaint:
{p['CHIEF_COMPLAINT']}

Clinical History:
{p['CLINICAL_JUSTIFICATION']}

Physical Examination:
Vitals stable. {rng.choice(['Alert and oriented x3.', 'No acute distress.', 'Cooperative and engaged.'])}
{rng.choice(['ROM limited in affected joint.', 'Lungs clear bilaterally.', 'Abdomen soft, non-tender.', 'Neurologically intact.'])}
"""
    page3 = f"""AUTHORIZATION REQUEST SUMMARY - PAGE 3

Diagnosis:    {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}
Procedure:    {p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}
Units:        {p['REQUESTED_UNITS']}
Service From: {p['SERVICE_FROM_DATE']}
Service To:   {p['SERVICE_TO_DATE']}

Rendering Provider: {p['PROVIDER_NAME']}
NPI:                {p['PROVIDER_NPI']}
Facility:           {p['FACILITY_NAME']}
Specialty:          {p['SPECIALTY']}

Provider Attestation: I certify the above information is accurate.

Signature: _________________________
Date: {p['SUBMISSION_DATE']}
"""
    return page1 + "\n\n" + page2 + "\n\n" + page3


def layout_partial_missing_ssn(p, rng):
    """Missing SSN4 and DOB — only name and auth number present."""
    return f"""{p['FACILITY_NAME']} - {p['SPECIALTY']}

AUTHORIZATION FORM (INCOMPLETE)

Patient Name: {p['FIRST_NAME']} {p['LAST_NAME']}
Member ID: {p['MEMBER_ID']}
[DOB: NOT PROVIDED]
[SSN4: NOT PROVIDED]

Authorization: {p['AUTH_NUMBER']}
Provider: {p['PROVIDER_NAME']} (NPI: {p['PROVIDER_NPI']})

Procedure: {p['PROCEDURE_CODE']}
Diagnosis: {p['DIAGNOSIS_CODE']}
Dates: {p['SERVICE_FROM_DATE']} - {p['SERVICE_TO_DATE']}

NOTE: Incomplete submission - missing patient identifiers.
Please resubmit with DOB and SSN4 for processing.
"""


def layout_partial_missing_name(p, rng):
    """Missing name — only DOB, SSN4, and member ID."""
    return f"""CLINICAL DOCUMENT

[Patient name illegible / not provided]

DOB: {p['DOB']}
SSN4: {p['SSN4']}
Member ID: {p['MEMBER_ID']}
Phone: {p['PHONE']}

Auth#: {p['AUTH_NUMBER']}
Dx: {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}
Px: {p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}
Units: {p['REQUESTED_UNITS']}
Dates: {p['SERVICE_FROM_DATE']} to {p['SERVICE_TO_DATE']}

Provider: {p['PROVIDER_NAME']}
NPI: {p['PROVIDER_NPI']}
"""


def layout_reversed_name_order(p, rng):
    """Last name, First name format — common source of swapped-name errors."""
    return f"""{'='*72}
              PRIOR AUTHORIZATION - {p['HEALTH_PLAN_NAME']}
{'='*72}

MEMBER: {p['LAST_NAME']}, {p['FIRST_NAME']} {p['MIDDLE_INITIAL']}
BORN:   {p['DOB']}
SSN4:   {p['SSN4']}
ID:     {p['MEMBER_ID']}
ADDR:   {p['ADDRESS']}, {p['CITY']} {p['STATE']} {p['ZIP']}

AUTH:   {p['AUTH_NUMBER']}
FROM:   {p['SERVICE_FROM_DATE']}
TO:     {p['SERVICE_TO_DATE']}

DX:     {p['DIAGNOSIS_CODE']} ({p['DIAGNOSIS_DESC']})
PX:     {p['PROCEDURE_CODE']} ({p['PROCEDURE_DESC']})
UNITS:  {p['REQUESTED_UNITS']}

PROVIDER: {p['PROVIDER_NAME']}
NPI:      {p['PROVIDER_NPI']}
FAC:      {p['FACILITY_NAME']}

JUSTIFICATION:
{p['CLINICAL_JUSTIFICATION']}

{'='*72}
"""


def layout_discharge_summary(p, rng):
    """Hospital discharge summary format."""
    admit_date = p['SERVICE_FROM_DATE']
    los = rng.randint(2, 14)
    return f"""{p['FACILITY_NAME']}
DISCHARGE SUMMARY

Patient: {p['FIRST_NAME']} {p['LAST_NAME']}
MRN: {p['MEMBER_ID']}                          DOB: {p['DOB']}
SSN4: {p['SSN4']}

Admission Date:  {admit_date}
Discharge Date:  {p['SERVICE_TO_DATE']}
Length of Stay:  {los} days

Attending: {p['PROVIDER_NAME']} (NPI: {p['PROVIDER_NPI']})

PRINCIPAL DIAGNOSIS:
  {p['DIAGNOSIS_CODE']} - {p['DIAGNOSIS_DESC']}

PROCEDURES PERFORMED:
  {p['PROCEDURE_CODE']} - {p['PROCEDURE_DESC']}

HOSPITAL COURSE:
Patient was admitted for {p['CHIEF_COMPLAINT'].lower()}. {p['CLINICAL_JUSTIFICATION']}

Patient tolerated the procedure well and was discharged in stable condition
with follow-up scheduled in {rng.randint(7,30)} days.

DISCHARGE MEDICATIONS:
  - {rng.choice(['Lisinopril 10mg daily', 'Metformin 500mg BID', 'Atorvastatin 20mg daily', 'Omeprazole 20mg daily'])}
  - {rng.choice(['Ibuprofen 400mg PRN', 'Acetaminophen 500mg PRN', 'Gabapentin 300mg TID'])}

Authorization Reference: {p['AUTH_NUMBER']}
"""


# All layout functions
LAYOUTS = [
    layout_header_only,
    layout_footer_only,
    layout_tabular,
    layout_narrative_embedded,
    layout_abbreviated_handwritten,
    layout_multipage_split,
    layout_partial_missing_ssn,
    layout_partial_missing_name,
    layout_reversed_name_order,
    layout_discharge_summary,
]


def main():
    output_dir = PROJECT_ROOT / "data" / "generated_docs_v2"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = PROJECT_ROOT / "data" / "synthetic" / "intake_forms_structured.csv"
    with open(csv_path, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    random.seed(99)  # Different seed from first batch for variety
    # Pick 100 rows starting from offset 70 (skip the first 70 already used)
    selected = rows[70:170]

    print(f"Generating 100 varied PDFs...")
    print(f"Output: {output_dir}")
    print(f"Layouts: {len(LAYOUTS)} variations")

    generated = []
    for i, row in enumerate(selected):
        seed = int(hashlib.md5(f"v2_{i}_{row.get('doc_id','')}".encode()).hexdigest(), 16) % (2**31)
        rng = random.Random(seed)

        p = build_placeholders(row, rng)
        layout_fn = LAYOUTS[i % len(LAYOUTS)]
        text = layout_fn(p, rng)

        doc_id = row.get("doc_id", f"v2_{i:04d}")
        filename = f"{doc_id}.pdf"
        filepath = output_dir / filename

        pdf = MinimalPDF()
        pdf.add_text_page(text)
        pdf.save(str(filepath))

        generated.append(filename)
        if (i + 1) % 25 == 0:
            print(f"  {i + 1}/100 ({layout_fn.__name__})")

    print(f"\nGenerated {len(generated)} PDFs")
    total_size = sum((output_dir / f).stat().st_size for f in generated)
    print(f"Total size: {total_size / 1024:.1f} KB")

    # Show layout distribution
    from collections import Counter
    layout_counts = Counter(LAYOUTS[i % len(LAYOUTS)].__name__ for i in range(len(generated)))
    print("\nLayout distribution:")
    for name, cnt in layout_counts.most_common():
        print(f"  {name}: {cnt}")


if __name__ == "__main__":
    main()
