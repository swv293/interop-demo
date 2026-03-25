#!/usr/bin/env python3
"""
generate_pdfs.py - Generate PDF clinical documents from CSV intake data and text templates.

Uses fpdf2 to render prior authorization forms, clinical notes, and lab reports
as PDFs with optional visual noise to simulate fax/scan quality variation.

Usage:
    python generate_pdfs.py --input data/intake_forms_structured.csv --output data/generated_docs/
    python generate_pdfs.py --input data/intake_forms_structured.csv --databricks --limit 100
"""

import argparse
import csv
import hashlib
import os
import random
import sys
import textwrap
from pathlib import Path

try:
    from fpdf import FPDF
except ImportError:
    print("=" * 60)
    print("ERROR: fpdf2 is not installed.")
    print()
    print("Install it with:")
    print("    pip install fpdf2")
    print()
    print("Or if using conda:")
    print("    conda install -c conda-forge fpdf2")
    print("=" * 60)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Helper data
# ---------------------------------------------------------------------------

CLINICAL_JUSTIFICATIONS = [
    "Patient has failed conservative management over the past 6 weeks including physical therapy "
    "and oral anti-inflammatory medications. Imaging confirms progressive deterioration requiring "
    "the requested intervention to prevent further functional decline.",

    "Medical necessity is established by persistent symptoms despite 3 months of first-line therapy. "
    "Lab results demonstrate elevated inflammatory markers consistent with active disease. Step therapy "
    "requirements have been met per plan guidelines.",

    "The patient presents with severe, debilitating symptoms that significantly limit activities of "
    "daily living. Prior treatments including OTC medications and prescription therapy have been "
    "inadequate. Specialist evaluation recommends the requested procedure.",

    "This authorization is requested based on clinical guidelines from the American College of "
    "Physicians. The patient meets all clinical criteria for the requested service including "
    "documented failure of two prior therapeutic attempts.",

    "Patient has a confirmed diagnosis supported by imaging and laboratory findings. The requested "
    "procedure is the standard of care for this condition at the current stage. Delay in treatment "
    "may result in irreversible complications.",

    "Chronic condition with acute exacerbation requiring escalation of care. Current medication "
    "regimen is insufficient as evidenced by worsening clinical markers. The requested treatment "
    "has demonstrated superior outcomes in peer-reviewed literature.",

    "The patient requires this service due to a new diagnosis confirmed by biopsy results. "
    "Multidisciplinary tumor board review recommends the proposed treatment plan. Timely "
    "initiation is critical for optimal therapeutic response.",

    "Authorization is requested for continuation of a previously approved treatment course. "
    "Clinical response has been documented with measurable improvement in functional scores. "
    "Interruption of therapy would risk disease relapse.",

    "Patient has documented allergy/intolerance to preferred formulary alternatives. The requested "
    "medication is the only viable option based on the patient's medical history and comorbid "
    "conditions. Supporting documentation from allergist is attached.",

    "Emergent clinical presentation necessitates expedited review. Patient was seen in the "
    "emergency department with acute symptoms requiring immediate specialist intervention. "
    "Delay beyond 72 hours poses significant clinical risk.",

    "Peer-reviewed evidence supports the use of this treatment for the patient's specific "
    "condition subtype. The patient's genetic testing results indicate likely response to "
    "the requested targeted therapy.",

    "The patient has exhausted all covered alternatives under the current formulary. Detailed "
    "treatment history documenting trials and failures of each agent is provided. The requested "
    "exception is supported by the treating specialist.",

    "Functional assessment demonstrates significant decline over the past 90 days despite "
    "ongoing therapy. Revised treatment plan addresses identified gaps and is consistent "
    "with evidence-based clinical pathways.",

    "Post-surgical follow-up indicates need for additional rehabilitation services beyond "
    "the standard allocation. Objective measures show the patient has not yet reached "
    "functional benchmarks required for safe independent activity.",

    "Patient is a pediatric case with developmental considerations requiring specialized "
    "treatment approach. Standard adult protocols are not appropriate. The requested service "
    "follows pediatric subspecialty guidelines endorsed by the AAP.",
]

CHIEF_COMPLAINTS = [
    "Persistent lower back pain radiating to the left leg for 8 weeks",
    "Worsening shortness of breath on exertion over the past month",
    "Recurrent episodes of chest pain with activity, new onset 2 weeks ago",
    "Progressive bilateral knee pain limiting mobility for 3 months",
    "Chronic migraine headaches unresponsive to current medication regimen",
    "Elevated blood glucose levels despite oral medication compliance",
    "New onset of joint swelling and morning stiffness in bilateral hands",
    "Follow-up for abnormal mammogram findings from screening exam",
    "Fatigue and unintentional weight loss of 15 pounds over 2 months",
    "Worsening vision and eye pain in the right eye for 3 weeks",
]

HPI_TEXTS = [
    "Patient is a 58-year-old presenting with a 6-week history of progressive symptoms. "
    "Initial onset was gradual and has worsened despite conservative management. Patient reports "
    "that symptoms interfere with sleep and daily activities. No recent trauma or injury.",

    "The patient reports symptom onset approximately 3 months ago. Initial presentation was mild "
    "but has progressively worsened. Previous treatment with NSAIDs and physical therapy provided "
    "only temporary relief. Family history is significant for similar conditions.",

    "Patient has been followed in this clinic for the past 2 years for chronic disease management. "
    "Recent exacerbation began 4 weeks ago with increased frequency and severity of symptoms. "
    "Current medication regimen has been optimized without adequate response.",

    "Established patient returns for evaluation of worsening condition. Symptoms have been present "
    "for approximately 10 weeks. Patient has completed a 6-week course of prescribed therapy "
    "without significant improvement. No new medications or exposures.",

    "New patient referral from primary care for specialist evaluation. Symptoms have been present "
    "for several months with gradual progression. Relevant imaging and laboratory workup have been "
    "completed and are reviewed in this note.",

    "Patient presents with acute-on-chronic symptoms that have become unmanageable over the past "
    "2 weeks. Emergency department visit last week resulted in temporary management. Patient was "
    "advised to follow up for definitive treatment planning.",

    "The patient describes intermittent symptoms over the past year that have become constant "
    "in the last 6 weeks. Impact on quality of life is significant, with inability to perform "
    "occupational duties. Previous specialist consultations are documented in the chart.",

    "Post-operative follow-up at 6 weeks. Patient reports some improvement but has not reached "
    "expected functional milestones. Physical examination confirms residual limitations. "
    "Additional intervention is being considered.",

    "Patient with known comorbidities including hypertension and diabetes presents with new "
    "symptoms that may be related to underlying conditions. Comprehensive workup is indicated "
    "to determine etiology and guide treatment decisions.",

    "Pediatric patient brought in by parent for evaluation of symptoms first noted 4 weeks ago. "
    "School performance has been affected. No prior history of similar complaints. "
    "Developmental milestones have been appropriate to date.",
]

OBJECTIVE_FINDINGS = [
    "Physical exam reveals limited range of motion. No acute distress noted.",
    "Lungs clear to auscultation bilaterally. Heart regular rate and rhythm.",
    "Tenderness to palpation over affected area. No erythema or swelling.",
    "Neurological exam intact. Strength 4/5 in affected extremity.",
    "BMI 31.2. Blood pressure 142/88. Mild peripheral edema noted.",
    "Skin exam unremarkable. Lymph nodes non-palpable. Abdomen soft, non-tender.",
    "Joint exam shows crepitus bilaterally. Effusion present on the right.",
    "Visual acuity 20/40 OD, 20/25 OS. Fundoscopic exam reveals mild changes.",
]

FACILITY_NAMES = [
    "Memorial Regional Medical Center",
    "St. Joseph's University Hospital",
    "Valley Health System - Main Campus",
    "Lakeside Community Health Center",
    "Metropolitan Specialty Clinic",
    "Sunrise Medical Associates",
    "Pacific Northwest Medical Group",
    "Heritage Health Partners",
    "Bayview Ambulatory Care Center",
    "Mountain Vista Medical Plaza",
    "Northside Family Health Clinic",
    "Riverside Integrated Care Network",
]

PROVIDER_SPECIALTIES = [
    "Internal Medicine",
    "Orthopedic Surgery",
    "Cardiology",
    "Oncology",
    "Neurology",
    "Pulmonology",
    "Endocrinology",
    "Rheumatology",
    "Ophthalmology",
    "General Surgery",
    "Pain Management",
    "Physical Medicine & Rehabilitation",
]

# Lab data for clinical_note_3 (lab report) template
LAB_PANELS = [
    # CBC panel
    {
        "tests": [
            ("WBC", "7.2", "4.5-11.0 K/uL", ""),
            ("Hemoglobin", "13.8", "12.0-17.5 g/dL", ""),
            ("Platelets", "245", "150-400 K/uL", ""),
        ]
    },
    {
        "tests": [
            ("WBC", "12.4", "4.5-11.0 K/uL", "H"),
            ("Hemoglobin", "10.2", "12.0-17.5 g/dL", "L"),
            ("Platelets", "189", "150-400 K/uL", ""),
        ]
    },
    # BMP panel
    {
        "tests": [
            ("Glucose", "142", "70-100 mg/dL", "H"),
            ("BUN", "18", "7-20 mg/dL", ""),
            ("Creatinine", "1.1", "0.7-1.3 mg/dL", ""),
        ]
    },
    {
        "tests": [
            ("Glucose", "98", "70-100 mg/dL", ""),
            ("Sodium", "141", "136-145 mEq/L", ""),
            ("Potassium", "4.2", "3.5-5.0 mEq/L", ""),
        ]
    },
    # Lipid panel
    {
        "tests": [
            ("Total Cholesterol", "242", "< 200 mg/dL", "H"),
            ("LDL", "162", "< 100 mg/dL", "H"),
            ("HDL", "38", "> 40 mg/dL", "L"),
        ]
    },
    # Inflammatory markers
    {
        "tests": [
            ("ESR", "48", "0-20 mm/hr", "H"),
            ("CRP", "3.8", "< 1.0 mg/dL", "H"),
            ("RF", "< 10", "< 14 IU/mL", ""),
        ]
    },
    # Thyroid
    {
        "tests": [
            ("TSH", "6.8", "0.4-4.0 mIU/L", "H"),
            ("Free T4", "0.7", "0.8-1.8 ng/dL", "L"),
            ("Free T3", "2.1", "2.3-4.2 pg/mL", "L"),
        ]
    },
    # HbA1c + metabolic
    {
        "tests": [
            ("HbA1c", "8.2", "< 5.7 %", "H"),
            ("Fasting Glucose", "168", "70-100 mg/dL", "H"),
            ("Insulin", "22.4", "2.6-24.9 uIU/mL", ""),
        ]
    },
]

DIAGNOSIS_DESCRIPTIONS = {
    "M54.5": "Low back pain",
    "M17.11": "Primary osteoarthritis, right knee",
    "M17.12": "Primary osteoarthritis, left knee",
    "E11.65": "Type 2 diabetes mellitus with hyperglycemia",
    "E11.9": "Type 2 diabetes mellitus without complications",
    "I10": "Essential (primary) hypertension",
    "J44.1": "Chronic obstructive pulmonary disease with acute exacerbation",
    "G43.909": "Migraine, unspecified, not intractable",
    "C50.911": "Malignant neoplasm of unspecified site of right female breast",
    "M06.9": "Rheumatoid arthritis, unspecified",
    "I25.10": "Atherosclerotic heart disease of native coronary artery",
    "J45.40": "Moderate persistent asthma, uncomplicated",
    "K21.0": "Gastro-esophageal reflux disease with esophagitis",
    "F32.1": "Major depressive disorder, single episode, moderate",
    "N18.3": "Chronic kidney disease, stage 3",
    "R06.00": "Dyspnea, unspecified",
    "Z96.641": "Presence of right artificial hip joint",
}

PROCEDURE_DESCRIPTIONS = {
    "99213": "Office visit, established patient, low complexity",
    "99214": "Office visit, established patient, moderate complexity",
    "99215": "Office visit, established patient, high complexity",
    "27447": "Total knee replacement arthroplasty",
    "27130": "Total hip replacement arthroplasty",
    "72148": "MRI lumbar spine without contrast",
    "72141": "MRI cervical spine without contrast",
    "43239": "Upper GI endoscopy with biopsy",
    "93306": "Transthoracic echocardiography, complete",
    "71046": "Chest X-ray, 2 views",
    "70553": "MRI brain with and without contrast",
    "29881": "Arthroscopy, knee, surgical with meniscectomy",
    "64483": "Epidural steroid injection, lumbar/sacral",
    "20610": "Arthrocentesis, aspiration and/or injection, major joint",
    "96413": "Chemotherapy administration, IV infusion, first hour",
    "90837": "Psychotherapy, 60 minutes",
    "97110": "Therapeutic exercises",
    "36415": "Venipuncture for blood draw",
}

HEALTH_PLAN_NAMES = [
    "Blue Cross Blue Shield of Illinois",
    "Aetna Better Health",
    "UnitedHealthcare Choice Plus",
    "Cigna HealthSpring",
    "Humana Gold Plus",
    "Anthem Blue Cross",
    "Molina Healthcare",
    "Centene Superior Health",
    "Kaiser Permanente",
    "WellCare Health Plans",
]

# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TEMPLATE_DIR = PROJECT_ROOT / "data" / "docs_templates"

TEMPLATE_MAP = {
    "prior_auth": [
        "prior_auth_form_1.txt",
        "prior_auth_form_2.txt",
        "prior_auth_form_3.txt",
    ],
    "clinical_note": [
        "clinical_note_1.txt",
        "clinical_note_2.txt",
    ],
    "lab_report": [
        "clinical_note_3.txt",
    ],
}


def load_template(filename: str) -> str:
    path = TEMPLATE_DIR / filename
    if not path.exists():
        print(f"  WARNING: template not found: {path}")
        return ""
    with open(path, "r") as f:
        return f.read()


def pick_template(form_type: str, seed: int) -> tuple[str, str]:
    """Return (template_text, template_filename) for a given form_type."""
    rng = random.Random(seed)
    form_type_lower = form_type.strip().lower().replace(" ", "_")
    # Normalise common variants
    if "prior" in form_type_lower or "auth" in form_type_lower or "pa_" in form_type_lower:
        key = "prior_auth"
    elif "lab" in form_type_lower:
        key = "lab_report"
    elif "clinical" in form_type_lower or "note" in form_type_lower or "progress" in form_type_lower:
        key = "clinical_note"
    else:
        key = rng.choice(list(TEMPLATE_MAP.keys()))

    choices = TEMPLATE_MAP[key]
    fname = rng.choice(choices)
    return load_template(fname), fname


# ---------------------------------------------------------------------------
# Placeholder filling
# ---------------------------------------------------------------------------

def deterministic_hash(val: str) -> int:
    return int(hashlib.md5(val.encode()).hexdigest(), 16)


def build_placeholders(row: dict, rng: random.Random) -> dict:
    """Build a full placeholder dict from a CSV row, filling gaps with helper data."""
    p = {}

    # Direct mappings (CSV column -> placeholder) - try common column name variants
    def get(col_names, default=""):
        if isinstance(col_names, str):
            col_names = [col_names]
        for c in col_names:
            val = row.get(c, "").strip()
            if val:
                return val
        return default

    p["FIRST_NAME"] = get(["first_name", "member_first_name", "patient_first_name"], "Jane")
    p["LAST_NAME"] = get(["last_name", "member_last_name", "patient_last_name"], "Doe")
    p["MIDDLE_INITIAL"] = get(["middle_initial", "mi"], rng.choice("ABCDEFGHJKLMNPRST"))
    p["DOB"] = get(["dob", "date_of_birth", "birth_date"], "01/15/1965")
    p["MEMBER_ID"] = get(["member_id", "subscriber_id", "patient_id"], f"MBR{rng.randint(100000,999999)}")
    p["SSN4"] = get(["ssn4", "ssn_last4", "last_4_ssn"], f"{rng.randint(1000,9999)}")
    p["GENDER"] = get(["gender", "sex"], rng.choice(["Male", "Female"]))
    p["PHONE"] = get(["phone", "member_phone", "patient_phone"], f"({rng.randint(200,999)}) {rng.randint(200,999)}-{rng.randint(1000,9999)}")
    p["ADDRESS_LINE1"] = get(["address", "address_line1", "street"], f"{rng.randint(100,9999)} {rng.choice(['Oak','Maple','Elm','Pine','Cedar'])} {rng.choice(['St','Ave','Blvd','Dr'])}")
    p["CITY"] = get(["city"], rng.choice(["Springfield", "Riverside", "Madison", "Georgetown", "Fairview"]))
    p["STATE"] = get(["state"], rng.choice(["IL", "CA", "TX", "FL", "NY", "OH", "PA"]))
    p["ZIP"] = get(["zip", "zip_code", "postal_code"], f"{rng.randint(10000,99999)}")

    p["AUTH_NUMBER"] = get(["auth_number", "authorization_number", "auth_id", "prior_auth_number"],
                           f"PA-{rng.randint(100000,999999)}")
    p["SERVICE_FROM_DATE"] = get(["service_from_date", "service_date", "dos", "date_of_service"], "03/01/2026")
    p["SERVICE_TO_DATE"] = get(["service_to_date", "service_end_date"], "03/31/2026")
    p["SUBMISSION_DATE"] = get(["submission_date", "submitted_date", "form_date"], "02/24/2026")

    p["PROVIDER_NAME"] = get(["provider_name", "rendering_provider", "physician_name", "doctor_name"],
                              f"Dr. {rng.choice(['Smith','Patel','Johnson','Lee','Garcia','Chen','Williams'])}")
    p["PROVIDER_NPI"] = get(["provider_npi", "npi"], f"{rng.randint(1000000000,1999999999)}")
    p["FACILITY_NAME"] = get(["facility_name", "facility", "clinic_name"], rng.choice(FACILITY_NAMES))
    p["PROVIDER_PHONE"] = get(["provider_phone"], f"({rng.randint(200,999)}) {rng.randint(200,999)}-{rng.randint(1000,9999)}")
    p["PROVIDER_FAX"] = get(["provider_fax", "fax"], f"({rng.randint(200,999)}) {rng.randint(200,999)}-{rng.randint(1000,9999)}")
    p["PROVIDER_SPECIALTY"] = get(["provider_specialty", "specialty"], rng.choice(PROVIDER_SPECIALTIES))

    diag_code = get(["diagnosis_code", "icd10", "icd_code", "primary_diagnosis"], rng.choice(list(DIAGNOSIS_DESCRIPTIONS.keys())))
    p["DIAGNOSIS_CODE"] = diag_code
    p["DIAGNOSIS_DESC"] = get(["diagnosis_desc", "diagnosis_description"],
                               DIAGNOSIS_DESCRIPTIONS.get(diag_code, "Unspecified diagnosis"))

    proc_code = get(["procedure_code", "cpt_code", "cpt"], rng.choice(list(PROCEDURE_DESCRIPTIONS.keys())))
    p["PROCEDURE_CODE"] = proc_code
    p["PROCEDURE_DESC"] = get(["procedure_desc", "procedure_description"],
                               PROCEDURE_DESCRIPTIONS.get(proc_code, "Medical procedure"))
    p["PROCEDURE_MODIFIER"] = get(["procedure_modifier", "modifier"], rng.choice(["", "", "", "26", "TC", "59", "LT", "RT"]))
    p["REQUESTED_UNITS"] = get(["requested_units", "units", "qty"], str(rng.choice([1, 1, 2, 3, 4, 6, 8, 12])))

    p["HEALTH_PLAN_NAME"] = get(["health_plan_name", "plan_name", "insurance_name", "payer_name"],
                                 rng.choice(HEALTH_PLAN_NAMES))
    p["PHONE_SUFFIX"] = f"{rng.randint(1000,9999)}"
    p["FAX_SUFFIX"] = f"{rng.randint(1000,9999)}"

    p["CLINICAL_JUSTIFICATION"] = get(["clinical_justification", "clinical_rationale", "justification"],
                                       rng.choice(CLINICAL_JUSTIFICATIONS))
    p["CHIEF_COMPLAINT"] = get(["chief_complaint", "cc"], rng.choice(CHIEF_COMPLAINTS))
    p["HPI_TEXT"] = get(["hpi_text", "hpi", "history_present_illness"], rng.choice(HPI_TEXTS))
    p["OBJECTIVE_FINDINGS"] = rng.choice(OBJECTIVE_FINDINGS)
    p["FOLLOWUP_DAYS"] = str(rng.choice([7, 14, 21, 30, 60, 90]))

    # Lab data
    lab_panel = rng.choice(LAB_PANELS)
    for i, (test, val, ref, flag) in enumerate(lab_panel["tests"], start=1):
        p[f"LAB_TEST_{i}"] = test
        p[f"LAB_VAL_{i}"] = val
        p[f"LAB_REF_{i}"] = ref
        p[f"LAB_FLAG_{i}"] = flag

    return p


def fill_template(template: str, placeholders: dict) -> str:
    text = template
    for key, value in placeholders.items():
        text = text.replace("{" + key + "}", value)
    return text


# ---------------------------------------------------------------------------
# PDF rendering
# ---------------------------------------------------------------------------

class ClinicalPDF(FPDF):
    """Custom FPDF subclass for clinical document rendering."""

    def __init__(self, add_noise: bool = False, noise_level: float = 0.3):
        super().__init__()
        self.add_noise = add_noise
        self.noise_level = noise_level

    def render_text_page(self, text: str, font_size: int = 9):
        """Render a page of monospace text."""
        self.add_page()
        if self.add_noise:
            self._draw_noise()
        self.set_font("Courier", size=font_size)
        # Split into lines and render
        for line in text.split("\n"):
            # Slight random x-offset for noisy docs
            x_offset = 0
            if self.add_noise and random.random() < self.noise_level * 0.3:
                x_offset = random.uniform(-1.5, 1.5)
            self.set_x(10 + x_offset)
            # Truncate very long lines
            if len(line) > 95:
                line = line[:95]
            self.cell(0, 4, line, new_x="LMARGIN", new_y="NEXT")

    def render_cover_sheet(self, placeholders: dict):
        """Render a cover/fax sheet as the first page."""
        self.add_page()
        if self.add_noise:
            self._draw_noise()

        self.set_font("Courier", "B", 16)
        self.ln(30)
        self.cell(0, 10, "CONFIDENTIAL FAX TRANSMISSION", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(10)

        self.set_font("Courier", "", 11)
        lines = [
            f"TO:     {placeholders.get('HEALTH_PLAN_NAME', 'Health Plan')}",
            f"FAX:    1-800-555-{placeholders.get('FAX_SUFFIX', '0000')}",
            f"FROM:   {placeholders.get('PROVIDER_NAME', 'Provider')}",
            f"        {placeholders.get('FACILITY_NAME', 'Facility')}",
            f"DATE:   {placeholders.get('SUBMISSION_DATE', '')}",
            f"RE:     Prior Authorization - {placeholders.get('FIRST_NAME', '')} {placeholders.get('LAST_NAME', '')}",
            f"        Member ID: {placeholders.get('MEMBER_ID', '')}",
            f"PAGES:  2 (including cover)",
            "",
            "=" * 50,
            "",
            "This fax contains confidential health information",
            "protected under HIPAA. If you are not the intended",
            "recipient, please notify the sender immediately",
            "and destroy this document.",
            "",
            "=" * 50,
        ]
        for line in lines:
            self.cell(0, 6, line, new_x="LMARGIN", new_y="NEXT")

    def _draw_noise(self):
        """Add visual noise to simulate fax/scan artifacts."""
        rng = random
        # Random gray rectangles
        num_rects = int(rng.uniform(1, 4) * self.noise_level)
        for _ in range(num_rects):
            gray = rng.randint(200, 240)
            self.set_fill_color(gray, gray, gray)
            x = rng.uniform(0, 180)
            y = rng.uniform(0, 280)
            w = rng.uniform(5, 40)
            h = rng.uniform(2, 8)
            self.rect(x, y, w, h, style="F")

        # Random dots / specks
        num_dots = int(rng.uniform(3, 12) * self.noise_level)
        for _ in range(num_dots):
            gray = rng.randint(160, 210)
            self.set_fill_color(gray, gray, gray)
            x = rng.uniform(5, 200)
            y = rng.uniform(5, 285)
            r = rng.uniform(0.3, 1.5)
            self.ellipse(x, y, r, r, style="F")

        # Reset to black
        self.set_text_color(0, 0, 0)


def generate_pdf(row: dict, row_index: int, output_dir: Path) -> str:
    """Generate a single PDF from a CSV row. Returns the output filename."""
    seed = deterministic_hash(f"{row_index}_{row.get('member_id', '')}_{row.get('auth_number', '')}")
    rng = random.Random(seed)
    # Seed global random for noise drawing (fpdf callbacks)
    random.seed(seed)

    form_type = row.get("form_type", row.get("document_type", "prior_auth"))
    template_text, template_file = pick_template(form_type, seed)

    if not template_text:
        # Fallback: use prior_auth_form_1
        template_text = load_template("prior_auth_form_1.txt")
        template_file = "prior_auth_form_1.txt"

    placeholders = build_placeholders(row, rng)
    filled = fill_template(template_text, placeholders)

    # Determine noise level
    is_noisy = "form_3" in template_file or rng.random() < 0.25
    noise_level = rng.uniform(0.4, 1.0) if is_noisy else 0.0

    # Build PDF
    pdf = ClinicalPDF(add_noise=is_noisy, noise_level=noise_level)
    pdf.set_auto_page_break(auto=True, margin=15)

    # Cover sheet for ~40% of docs
    add_cover = rng.random() < 0.40
    if add_cover:
        pdf.render_cover_sheet(placeholders)

    # Determine font size (smaller for dense templates)
    if "form_1" in template_file or "note_1" in template_file:
        font_size = 8
    elif "form_3" in template_file:
        font_size = 10
    else:
        font_size = 9

    # Split filled text into pages (~55 lines per page)
    lines = filled.split("\n")
    lines_per_page = 55
    page_chunks = [lines[i:i + lines_per_page] for i in range(0, len(lines), lines_per_page)]

    for chunk in page_chunks:
        pdf.render_text_page("\n".join(chunk), font_size=font_size)

    # Build output filename
    member_id = placeholders.get("MEMBER_ID", f"UNK{row_index}")
    auth_num = placeholders.get("AUTH_NUMBER", f"NA{row_index}").replace("-", "")
    safe_type = form_type.strip().lower().replace(" ", "_")[:20]
    filename = f"{safe_type}_{member_id}_{auth_num}_{row_index:04d}.pdf"
    # Sanitize
    filename = "".join(c for c in filename if c.isalnum() or c in "_-.")

    output_path = output_dir / filename
    pdf.output(str(output_path))
    return filename


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate PDF clinical documents from structured CSV data and text templates."
    )
    parser.add_argument(
        "--input", "-i",
        default=str(PROJECT_ROOT / "data" / "intake_forms_structured.csv"),
        help="Path to the intake CSV file (default: data/intake_forms_structured.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=str(PROJECT_ROOT / "data" / "generated_docs"),
        help="Output directory for generated PDFs (default: data/generated_docs/)",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=0,
        help="Limit number of PDFs to generate (0 = all rows)",
    )
    parser.add_argument(
        "--databricks",
        action="store_true",
        help="Save output to Databricks Volume path /Volumes/healthcare_demo/raw/raw_docs/",
    )
    args = parser.parse_args()

    # Resolve output directory
    if args.databricks:
        output_dir = Path("/Volumes/healthcare_demo/raw/raw_docs")
    else:
        output_dir = Path(args.output)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Read CSV
    csv_path = Path(args.input)
    if not csv_path.exists():
        print(f"ERROR: Input CSV not found: {csv_path}")
        print("Please provide a valid path with --input")
        sys.exit(1)

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("ERROR: CSV file is empty or has no data rows.")
        sys.exit(1)

    total = len(rows) if args.limit <= 0 else min(args.limit, len(rows))
    print(f"Generating {total} PDFs from {csv_path.name}")
    print(f"Output directory: {output_dir}")
    print(f"Templates directory: {TEMPLATE_DIR}")
    print("-" * 60)

    generated = 0
    errors = 0
    for i, row in enumerate(rows[:total]):
        try:
            filename = generate_pdf(row, i, output_dir)
            generated += 1

            # Progress bar
            pct = int((generated / total) * 100)
            bar_len = 40
            filled_len = int(bar_len * generated / total)
            bar = "#" * filled_len + "-" * (bar_len - filled_len)
            print(f"\r  [{bar}] {pct:3d}% ({generated}/{total}) {filename[:40]:<40s}", end="", flush=True)
        except Exception as e:
            errors += 1
            print(f"\n  ERROR on row {i}: {e}")

    print()
    print("-" * 60)
    print(f"Done! Generated {generated} PDFs, {errors} errors.")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
