#!/usr/bin/env python3
"""
Generate clinical TIFF documents (CCITT Group 4, fax-realistic) using Pillow.

Renders the same text as generate_docs_standalone.py but as multi-page,
monochrome G4 TIFFs — the actual format faxed clinical documents arrive in.
Reuses the text-builder helpers from the standalone PDF generator.

Usage:
    python3 generate_tiffs.py --tiff 10 --output data/generated_docs/

Requires Pillow (`brew install pillow` on macOS — Python 3.13+ binding).
"""

import argparse
import csv
import hashlib
import random
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "data" / "generation"))

from generate_docs_standalone import (
    build_clinical_note,
    build_lab_report,
    build_placeholders,
    build_prior_auth_form,
)

# Fax-like rendering parameters
DPI = 200
PAGE_WIDTH_IN = 8.5
PAGE_HEIGHT_IN = 11.0
PAGE_W = int(PAGE_WIDTH_IN * DPI)   # 1700 px
PAGE_H = int(PAGE_HEIGHT_IN * DPI)  # 2200 px
MARGIN = 120
LINE_HEIGHT = 28
FONT_SIZE = 18

FONT_CANDIDATES = [
    "/System/Library/Fonts/Supplemental/Courier New.ttf",
    "/System/Library/Fonts/Monaco.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
]


def load_font(size: int = FONT_SIZE) -> ImageFont.FreeTypeFont:
    for path in FONT_CANDIDATES:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def text_to_pages(text: str, font: ImageFont.FreeTypeFont) -> list:
    """Render text to one or more 1-bit fax-realistic page images."""
    lines = text.split("\n")
    lines_per_page = (PAGE_H - 2 * MARGIN) // LINE_HEIGHT

    pages = []
    for chunk_start in range(0, len(lines), lines_per_page):
        chunk = lines[chunk_start:chunk_start + lines_per_page]

        img = Image.new("L", (PAGE_W, PAGE_H), color=255)
        draw = ImageDraw.Draw(img)

        y = MARGIN
        for line in chunk:
            draw.text((MARGIN, y), line, font=font, fill=0)
            y += LINE_HEIGHT

        # Convert to 1-bit for G4 compression (fax standard)
        pages.append(img.convert("1"))

    return pages


def write_tiff(filepath: Path, pages: list):
    """Write a multi-page CCITT Group 4 (fax) TIFF."""
    pages[0].save(
        str(filepath),
        format="TIFF",
        compression="group4",
        save_all=True,
        append_images=pages[1:],
        dpi=(DPI, DPI),
    )


def generate_tiff(row: dict, row_index: int, output_dir: Path,
                  font: ImageFont.FreeTypeFont) -> str:
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
    elif "imaging" in form_type or "discharge" in form_type:
        text = build_clinical_note(p, rng)
    else:
        text = build_prior_auth_form(p, rng)

    pages = text_to_pages(text, font)
    doc_id = row.get("doc_id", f"doc_{row_index:06d}")
    filename = f"{doc_id}.tiff"
    write_tiff(output_dir / filename, pages)
    return filename


def main():
    parser = argparse.ArgumentParser(description="Generate clinical TIFF documents (fax-realistic G4)")
    parser.add_argument("--input", "-i",
                        default=str(PROJECT_ROOT / "data" / "synthetic" / "intake_forms_structured.csv"))
    parser.add_argument("--output", "-o",
                        default=str(PROJECT_ROOT / "data" / "generated_docs"))
    parser.add_argument("--tiff", type=int, default=10, help="Number of TIFFs to generate")
    parser.add_argument("--offset", type=int, default=60,
                        help="Skip the first N CSV rows (default 60 to avoid colliding with PDFs)")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(args.input, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    if args.offset + args.tiff > len(rows):
        print(f"Warning: offset {args.offset} + count {args.tiff} exceeds CSV size {len(rows)}; clamping.")
        args.tiff = max(0, len(rows) - args.offset)

    selected = rows[args.offset:args.offset + args.tiff]
    font = load_font()

    print(f"Generating {len(selected)} multipage G4 TIFFs at {DPI} DPI...")
    print(f"Output directory: {output_dir}")

    for i, row in enumerate(selected):
        fname = generate_tiff(row, args.offset + i, output_dir, font)
        if (i + 1) % 5 == 0 or (i + 1) == len(selected):
            print(f"  TIFFs: {i + 1}/{len(selected)}  (latest: {fname})")

    print(f"\nDone. {len(selected)} TIFFs written to {output_dir}")


if __name__ == "__main__":
    main()
