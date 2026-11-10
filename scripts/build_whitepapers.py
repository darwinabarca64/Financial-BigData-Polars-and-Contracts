# -*- coding: utf-8 -*-
"""Whitepaper Compiler: generates executive DOCX and structured TXT publications.

Generates:
- dist/Financial-BigData-Polars-and-Contracts-ES.docx
- dist/Financial-BigData-Polars-and-Contracts-EN.docx
- dist/Financial-BigData-Polars-and-Contracts-ES.txt
- dist/Financial-BigData-Polars-and-Contracts-EN.txt
"""

import os
import re
import sys
import docx
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import parse_xml
from docx.oxml.ns import nsdecls

DIST_DIR = "dist"
AUTHOR_NAME = "Darwin Miguel Abarca Vargas"
AUTHOR_TITLE = "Senior Data & Analytics Engineer | Quantitative Economist"
YEAR_LABEL = "2026"

def set_cell_shading(cell, color_hex):
    """Apply background shading to a table cell."""
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_pr.append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>'))

def set_cell_padding(cell, top=100, bottom=100, left=150, right=150):
    """Apply internal margins (padding) to a table cell in dxa (1 pt = 20 dxa)."""
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_mar = parse_xml(
        f'<w:tcMar {nsdecls("w")}>'
        f'<w:top w:w="{top}" w:type="dxa"/>'
        f'<w:bottom w:w="{bottom}" w:type="dxa"/>'
        f'<w:left w:w="{left}" w:type="dxa"/>'
        f'<w:right w:w="{right}" w:type="dxa"/>'
        f'</w:tcMar>'
    )
    tc_pr.append(tc_mar)

def set_table_borders(table, color="CBD5E1", sz="4"):
    """Apply subtle borders to a table."""
    tbl_pr = table._tbl.tblPr
    borders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>'
        f'<w:top w:val="single" w:sz="{sz}" w:space="0" w:color="{color}"/>'
        f'<w:bottom w:val="single" w:sz="{sz}" w:space="0" w:color="{color}"/>'
        f'<w:left w:val="none"/>'
        f'<w:right w:val="none"/>'
        f'<w:insideH w:val="single" w:sz="{sz}" w:space="0" w:color="{color}"/>'
        f'<w:insideV w:val="none"/>'
        f'</w:tblBorders>'
    )
    tbl_pr.append(borders)

def make_callout_box(doc, text, lang=""):
    """Create a formatted single-cell callout box for code, mermaid, or math."""
    tbl = doc.add_table(rows=1, cols=1)
    tbl.autofit = False
    tbl.columns[0].width = Inches(6.5)
    cell = tbl.cell(0, 0)
    set_cell_shading(cell, "F8FAFC")
    set_cell_padding(cell, top=120, bottom=120, left=180, right=180)

    accent_color = "0D9488" if lang == "math" else ("2563EB" if lang == "mermaid" else "475569")
    tbl_pr = tbl._tbl.tblPr
    borders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>'
        f'<w:top w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'<w:bottom w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'<w:left w:val="single" w:sz="24" w:space="0" w:color="{accent_color}"/>'
        f'<w:right w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'</w:tblBorders>'
    )
    tbl_pr.append(borders)

    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    p.paragraph_format.line_spacing = 1.05

    run = p.add_run(text)
    run.font.name = "Consolas"
    run.font.size = Pt(8.5)
    run.font.color.rgb = RGBColor(0x0F, 0x17, 0x2A)

def add_formatted_runs(paragraph, text):
    """Parse basic inline markdown (bold, italic, code) and append runs."""
    pattern = re.compile(r'(\*\*.*?\*\*|\*.*?\*|`.*?`)')
    tokens = pattern.split(text)
    for token in tokens:
        if not token:
            continue
        if token.startswith('**') and token.endswith('**') and len(token) >= 4:
            run = paragraph.add_run(token[2:-2])
            run.bold = True
        elif token.startswith('*') and token.endswith('*') and len(token) >= 2:
            run = paragraph.add_run(token[1:-1])
            run.italic = True
        elif token.startswith('`') and token.endswith('`') and len(token) >= 2:
            run = paragraph.add_run(token[1:-1])
            run.font.name = "Consolas"
            run.font.size = Pt(9.0)
            run.font.color.rgb = RGBColor(0x0F, 0x17, 0x2A)
        else:
            paragraph.add_run(token)

def build_cover_page(doc, title, subtitle, edition_label):
    """Generate a formal institutional cover page."""
    for _ in range(3):
        doc.add_paragraph()

    # Title
    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_title = p_title.add_run(title)
    run_title.font.name = "Calibri"
    run_title.font.size = Pt(28)
    run_title.font.bold = True
    run_title.font.color.rgb = RGBColor(0x0F, 0x17, 0x2A)

    # Decorative Rule
    p_div = doc.add_paragraph()
    p_div.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_div = p_div.add_run("____________________________________________________________")
    run_div.font.size = Pt(10)
    run_div.font.color.rgb = RGBColor(0x02, 0x84, 0xC7)

    # Subtitle
    p_sub = doc.add_paragraph()
    p_sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_sub.paragraph_format.space_before = Pt(12)
    p_sub.paragraph_format.space_after = Pt(24)
    run_sub = p_sub.add_run(subtitle)
    run_sub.font.name = "Calibri"
    run_sub.font.size = Pt(12)
    run_sub.font.italic = True
    run_sub.font.color.rgb = RGBColor(0x33, 0x41, 0x55)

    for _ in range(4):
        doc.add_paragraph()

    # Edition badge
    p_ed = doc.add_paragraph()
    p_ed.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_ed = p_ed.add_run(f"[ {edition_label.upper()} ]")
    run_ed.font.name = "Consolas"
    run_ed.font.size = Pt(11)
    run_ed.font.bold = True
    run_ed.font.color.rgb = RGBColor(0x03, 0x69, 0xA1)

    # Author
    p_auth = doc.add_paragraph()
    p_auth.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_auth.paragraph_format.space_before = Pt(24)
    run_auth = p_auth.add_run(AUTHOR_NAME)
    run_auth.font.name = "Calibri"
    run_auth.font.size = Pt(14)
    run_auth.font.bold = True
    run_auth.font.color.rgb = RGBColor(0x0F, 0x17, 0x2A)

    p_role = doc.add_paragraph()
    p_role.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_role = p_role.add_run(AUTHOR_TITLE)
    run_role.font.name = "Calibri"
    run_role.font.size = Pt(10.5)
    run_role.font.italic = True
    run_role.font.color.rgb = RGBColor(0x47, 0x55, 0x69)

    p_year = doc.add_paragraph()
    p_year.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_year.paragraph_format.space_before = Pt(8)
    run_year = p_year.add_run(YEAR_LABEL)
    run_year.font.name = "Calibri"
    run_year.font.size = Pt(10)
    run_year.font.color.rgb = RGBColor(0x64, 0x74, 0x8B)

    doc.add_page_break()

def parse_markdown_to_docx(md_path, docx_path, title, subtitle, edition_label):
    """Parse Markdown content and render into a structured Word document."""
    with open(md_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    doc = docx.Document()

    # Page setup: 1 inch margins
    for sec in doc.sections:
        sec.top_margin = Inches(1.0)
        sec.bottom_margin = Inches(1.0)
        sec.left_margin = Inches(1.0)
        sec.right_margin = Inches(1.0)

    build_cover_page(doc, title, subtitle, edition_label)

    in_code_block = False
    code_lang = ""
    code_lines = []

    in_math_block = False
    math_lines = []

    in_table = False
    table_lines = []

    first_h2 = True

    i = 0
    num_lines = len(lines)

    while i < num_lines:
        line = lines[i]
        stripped = line.strip()

        # 1. Code blocks (```)
        if stripped.startswith("```"):
            if not in_code_block:
                in_code_block = True
                code_lang = stripped[3:].strip()
                code_lines = []
            else:
                in_code_block = False
                make_callout_box(doc, "\n".join(code_lines), lang=code_lang)
                code_lines = []
                code_lang = ""
            i += 1
            continue

        if in_code_block:
            code_lines.append(line.rstrip("\r\n"))
            i += 1
            continue

        # 2. LaTeX Display Math blocks ($$)
        if stripped.startswith("$$"):
            if not in_math_block:
                if stripped.endswith("$$") and len(stripped) > 2 and stripped != "$$":
                    # Single-line math block
                    math_content = stripped[2:-2].strip()
                    make_callout_box(doc, math_content, lang="math")
                else:
                    in_math_block = True
                    math_lines = [stripped[2:].strip()]
            else:
                in_math_block = False
                if stripped != "$$":
                    math_lines.append(stripped.rstrip("$").strip())
                make_callout_box(doc, "\n".join([m for m in math_lines if m]), lang="math")
                math_lines = []
            i += 1
            continue

        if in_math_block:
            if stripped.endswith("$$"):
                in_math_block = False
                math_lines.append(stripped[:-2].strip())
                make_callout_box(doc, "\n".join([m for m in math_lines if m]), lang="math")
                math_lines = []
            else:
                math_lines.append(line.rstrip("\r\n"))
            i += 1
            continue

        # 3. Tables (| ... |)
        if stripped.startswith("|") and stripped.endswith("|"):
            if not in_table:
                in_table = True
                table_lines = [stripped]
            else:
                table_lines.append(stripped)
            i += 1
            continue
        elif in_table:
            # End of table
            in_table = False
            _render_markdown_table(doc, table_lines)
            table_lines = []
            # continue processing current line below

        # 4. Skip horizontal rules (---)
        if stripped == "---":
            i += 1
            continue

        # 5. Headings
        if stripped.startswith("#"):
            if stripped.startswith("#### "):
                h = doc.add_heading(level=4)
                h.paragraph_format.space_before = Pt(8)
                h.paragraph_format.space_after = Pt(2)
                h.paragraph_format.keep_with_next = True
                run = h.add_run(stripped[5:].strip())
                run.font.name = "Calibri"
                run.font.size = Pt(11)
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x33, 0x41, 0x55)
            elif stripped.startswith("### "):
                h = doc.add_heading(level=3)
                h.paragraph_format.space_before = Pt(12)
                h.paragraph_format.space_after = Pt(4)
                h.paragraph_format.keep_with_next = True
                run = h.add_run(stripped[4:].strip())
                run.font.name = "Calibri"
                run.font.size = Pt(12.5)
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x1E, 0x29, 0x3B)
            elif stripped.startswith("## "):
                # Section or Module Header
                if not first_h2 and ("Módulo" in stripped or "Module" in stripped):
                    doc.add_page_break()
                first_h2 = False
                h = doc.add_heading(level=2)
                h.paragraph_format.space_before = Pt(18)
                h.paragraph_format.space_after = Pt(6)
                h.paragraph_format.keep_with_next = True
                run = h.add_run(stripped[3:].strip())
                run.font.name = "Calibri"
                run.font.size = Pt(16)
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x0F, 0x17, 0x2A)
            elif stripped.startswith("# "):
                # Document title (skip if already on cover or render as main title)
                pass
            i += 1
            continue

        # 6. Bullet lists
        if stripped.startswith("* ") or stripped.startswith("- "):
            p = doc.add_paragraph(style="List Bullet")
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after = Pt(2)
            p.paragraph_format.line_spacing = 1.15
            content = stripped[2:].strip()
            add_formatted_runs(p, content)
            i += 1
            continue

        if stripped.startswith("    * ") or stripped.startswith("    - "):
            p = doc.add_paragraph(style="List Bullet")
            p.paragraph_format.left_indent = Inches(0.5)
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after = Pt(2)
            p.paragraph_format.line_spacing = 1.15
            content = stripped[6:].strip()
            add_formatted_runs(p, content)
            i += 1
            continue

        # 7. Blockquotes
        if stripped.startswith(">"):
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Inches(0.3)
            p.paragraph_format.space_before = Pt(4)
            p.paragraph_format.space_after = Pt(4)
            content = stripped.lstrip("> ").strip()
            add_formatted_runs(p, content)
            i += 1
            continue

        # 8. Regular body paragraph
        if stripped:
            p = doc.add_paragraph()
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after = Pt(4)
            p.paragraph_format.line_spacing = 1.15
            add_formatted_runs(p, stripped)

        i += 1

    # Cleanup trailing table if file ends with one
    if in_table and table_lines:
        _render_markdown_table(doc, table_lines)

    doc.save(docx_path)
    size_kb = os.path.getsize(docx_path) / 1024
    print(f"[OK] Generated: {docx_path} ({size_kb:.2f} KB)")

def _render_markdown_table(doc, raw_lines):
    """Render a markdown table into a native Word table with headers and borders."""
    parsed_rows = []
    for line in raw_lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Check if separator row (e.g. :--- | :---)
        if all(re.match(r"^:?-+:?$", c) for c in cells if c):
            continue
        parsed_rows.append(cells)

    if not parsed_rows:
        return

    num_cols = max(len(r) for r in parsed_rows)
    # Pad shorter rows
    for r in parsed_rows:
        while len(r) < num_cols:
            r.append("")

    tbl = doc.add_table(rows=len(parsed_rows), cols=num_cols)
    tbl.autofit = True
    set_table_borders(tbl, color="CBD5E1", sz="4")

    # Header row formatting
    header_tr = tbl.rows[0]._tr.get_or_add_trPr()
    header_tr.append(parse_xml(f'<w:tblHeader {nsdecls("w")}/>'))

    for col_idx, cell_text in enumerate(parsed_rows[0]):
        cell = tbl.cell(0, col_idx)
        set_cell_shading(cell, "1E293B")
        set_cell_padding(cell, top=100, bottom=100, left=120, right=120)
        p = cell.paragraphs[0]
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after = Pt(1)
        p.paragraph_format.line_spacing = 1.05
        clean_text = re.sub(r"\*\*(.*?)\*\*", r"\1", cell_text)
        run = p.add_run(clean_text)
        run.bold = True
        run.font.name = "Calibri"
        run.font.size = Pt(9.0)
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

    # Data rows formatting
    for row_idx in range(1, len(parsed_rows)):
        bg_color = "F8FAFC" if row_idx % 2 == 1 else "FFFFFF"
        for col_idx, cell_text in enumerate(parsed_rows[row_idx]):
            cell = tbl.cell(row_idx, col_idx)
            set_cell_shading(cell, bg_color)
            set_cell_padding(cell, top=80, bottom=80, left=120, right=120)
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after = Pt(1)
            p.paragraph_format.line_spacing = 1.05
            clean_text = cell_text.replace("<br/>", " \n").replace("<br>", " \n")
            add_formatted_runs(p, clean_text)
            for r in p.runs:
                if not r.font.name:
                    r.font.name = "Calibri"
                r.font.size = Pt(8.5)

    doc.add_paragraph().paragraph_format.space_after = Pt(4)

def build_plain_text(src_md_path, txt_out_path):
    """Generate clean structured plain text publication with strict UTF-8 with BOM."""
    with open(src_md_path, "r", encoding="utf-8") as f:
        content = f.read()

    with open(txt_out_path, "w", encoding="utf-8-sig") as f:
        f.write(content)

    size_kb = os.path.getsize(txt_out_path) / 1024
    print(f"[OK] Generated: {txt_out_path} ({size_kb:.2f} KB)")

def main():
    os.makedirs(DIST_DIR, exist_ok=True)

    title_main = "Financial-BigData-Polars-and-Contracts"
    subtitle_es = (
        "Tratado Técnico sobre Ingeniería de Plataformas de Datos Financieros, "
        "Procesamiento Vectorizado en Rust (Polars / Apache Arrow), Contratos de Datos "
        "(Pydantic v2 / Great Expectations), Reconciliación Contable de Carteras "
        "(NAV & Position Breaks) y Gobernanza FinOps sobre AWS S3 y Snowflake / DuckDB."
    )
    subtitle_en = (
        "Technical Treatise on Financial Data Platform Engineering, "
        "Rust-Vectorized Processing (Polars / Apache Arrow), Data Contracts "
        "(Pydantic v2 / Great Expectations), Portfolio Accounting Reconciliation "
        "(NAV & Position Breaks) and FinOps Governance on AWS S3 and Snowflake / DuckDB."
    )

    # 1. Spanish DOCX & TXT
    print("[INFO] Building Spanish Whitepapers...")
    parse_markdown_to_docx(
        md_path="Data-architecture.md",
        docx_path=os.path.join(DIST_DIR, "Financial-BigData-Polars-and-Contracts-ES.docx"),
        title=title_main,
        subtitle=subtitle_es,
        edition_label="Edición Maestra Ejecutiva (Español)"
    )
    build_plain_text(
        src_md_path="Data-architecture.md",
        txt_out_path=os.path.join(DIST_DIR, "Financial-BigData-Polars-and-Contracts-ES.txt")
    )

    # 2. English DOCX & TXT
    print("[INFO] Building English Whitepapers...")
    parse_markdown_to_docx(
        md_path="README.md",
        docx_path=os.path.join(DIST_DIR, "Financial-BigData-Polars-and-Contracts-EN.docx"),
        title=title_main,
        subtitle=subtitle_en,
        edition_label="Institutional Executive Edition (English)"
    )
    build_plain_text(
        src_md_path="README.md",
        txt_out_path=os.path.join(DIST_DIR, "Financial-BigData-Polars-and-Contracts-EN.txt")
    )

    print("[OK] All 4 whitepaper artifacts successfully compiled in dist/.")

if __name__ == "__main__":
    main()
