# -*- coding: utf-8 -*-
"""Treatise Integrity, Parity, OPSEC, and Artifact Verification Suite."""

import os
import re
import unicodedata
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST_DIR = os.path.join(REPO_ROOT, "dist")

DATA_ARCH_MD = os.path.join(REPO_ROOT, "Data-architecture.md")
README_MD = os.path.join(REPO_ROOT, "README.md")

EXPECTED_DIST_FILES = [
    "Financial-BigData-Polars-and-Contracts-ES.docx",
    "Financial-BigData-Polars-and-Contracts-EN.docx",
    "Financial-BigData-Polars-and-Contracts-ES.txt",
    "Financial-BigData-Polars-and-Contracts-EN.txt",
]

# Construct prohibited OPSEC tokens dynamically to prevent self-matching
BANNED_OPSEC_TOKENS = [
    "C" + ":\\Users",
    "D" + ":\\programs",
    "C" + "eX",
    "c" + ":/users",
    "d" + ":/programs",
]

def test_treatise_files_exist_and_utf8():
    """Verify existence and clean UTF-8 decoding of master documents."""
    assert os.path.isfile(DATA_ARCH_MD), "[CRITICAL] Data-architecture.md not found"
    assert os.path.isfile(README_MD), "[CRITICAL] README.md not found"

    with open(DATA_ARCH_MD, "r", encoding="utf-8") as f:
        content_es = f.read()
    assert len(content_es) > 100_000, "[CRITICAL] Data-architecture.md is unexpectedly truncated"

    with open(README_MD, "r", encoding="utf-8") as f:
        content_en = f.read()
    assert len(content_en) > 100_000, "[CRITICAL] README.md is unexpectedly truncated"

def test_all_ten_modules_present():
    """Verify presence of all 10 modules in both Spanish and English editions."""
    with open(DATA_ARCH_MD, "r", encoding="utf-8") as f:
        content_es = f.read()
    with open(README_MD, "r", encoding="utf-8") as f:
        content_en = f.read()

    for m in range(1, 11):
        pattern_es = rf"^## Módulo {m}:"
        assert re.search(pattern_es, content_es, re.MULTILINE), (
            f"[CRITICAL] Missing Module {m} in Data-architecture.md"
        )

        pattern_en = rf"^## Module {m}:"
        assert re.search(pattern_en, content_en, re.MULTILINE), (
            f"[CRITICAL] Missing Module {m} in README.md"
        )

def test_structural_parity():
    """Verify structural parity: >= 12 Mermaid diagrams, >= 11 tables, and LaTeX math."""
    with open(DATA_ARCH_MD, "r", encoding="utf-8") as f:
        content_es = f.read()
    with open(README_MD, "r", encoding="utf-8") as f:
        content_en = f.read()

    mermaids_es = len(re.findall(r"```mermaid", content_es))
    mermaids_en = len(re.findall(r"```mermaid", content_en))
    assert mermaids_es >= 12, f"[CRITICAL] Data-architecture.md has only {mermaids_es} Mermaid blocks"
    assert mermaids_en >= 12, f"[CRITICAL] README.md has only {mermaids_en} Mermaid blocks"

    tables_es = len(re.findall(r"\|\s*:?---", content_es))
    tables_en = len(re.findall(r"\|\s*:?---", content_en))
    assert tables_es >= 11, f"[CRITICAL] Data-architecture.md has only {tables_es} tables"
    assert tables_en >= 11, f"[CRITICAL] README.md has only {tables_en} tables"

    math_es = len(re.findall(r"\$\$", content_es))
    math_en = len(re.findall(r"\$\$", content_en))
    assert math_es >= 20, f"[CRITICAL] Data-architecture.md has insufficient LaTeX blocks ({math_es})"
    assert math_en >= 20, f"[CRITICAL] README.md has insufficient LaTeX blocks ({math_en})"

def test_zero_emojis():
    """Verify absolute zero emojis across all .md, .py, and .txt files."""
    extensions = (".md", ".py", ".txt")
    emoji_violations = []

    for root, dirs, files in os.walk(REPO_ROOT):
        # Skip ignored directories
        dirs[:] = [d for d in dirs if d not in {".git", "__pycache__", ".pytest_cache", ".ruff_cache", "venv", ".venv"}]
        for fname in files:
            if fname.endswith(extensions):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        text = f.read()
                    for idx, ch in enumerate(text):
                        cp = ord(ch)
                        # Detect standard emoji code point ranges
                        if (
                            0x1F600 <= cp <= 0x1F64F or
                            0x1F300 <= cp <= 0x1F5FF or
                            0x1F680 <= cp <= 0x1F6FF or
                            0x1F700 <= cp <= 0x1F77F or
                            0x1F780 <= cp <= 0x1F7FF or
                            0x1F800 <= cp <= 0x1F8FF or
                            0x1F900 <= cp <= 0x1F9FF or
                            0x1FA00 <= cp <= 0x1FAFF or
                            0x2600 <= cp <= 0x26FF or
                            0x2700 <= cp <= 0x27BF
                        ):
                            rel_path = os.path.relpath(fpath, REPO_ROOT)
                            emoji_violations.append((rel_path, hex(cp), ch))
                except Exception as err:
                    pytest.fail(f"Failed to read file {fpath}: {err}")

    assert not emoji_violations, f"[CRITICAL] Emojis detected in repository files: {emoji_violations[:10]}"

def test_opsec_hygiene():
    """Verify absolute absence of local Windows paths and personal development usernames."""
    extensions = (".md", ".py", ".txt", ".json", ".yml", ".yaml", ".sql")
    opsec_violations = []

    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in {".git", "__pycache__", ".pytest_cache", ".ruff_cache", "venv", ".venv"}]
        for fname in files:
            if fname.endswith(extensions):
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, REPO_ROOT)
                # Skip checking test file itself for token definitions
                if fname == "test_treatise_integrity.py":
                    continue
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        content_lower = f.read().lower()
                    for token in BANNED_OPSEC_TOKENS:
                        if token.lower() in content_lower:
                            opsec_violations.append((rel_path, token))
                except Exception as err:
                    pytest.fail(f"Failed to scan {fpath}: {err}")

    assert not opsec_violations, f"[CRITICAL] OPSEC violations detected: {opsec_violations}"

def test_dist_whitepapers_exist_and_non_empty():
    """Verify existence and size (> 30 KB) of all 4 whitepapers in dist/."""
    assert os.path.isdir(DIST_DIR), "[CRITICAL] dist/ directory does not exist"

    for fname in EXPECTED_DIST_FILES:
        fpath = os.path.join(DIST_DIR, fname)
        assert os.path.isfile(fpath), f"[CRITICAL] Missing artifact: {fname}"

        size_bytes = os.path.getsize(fpath)
        min_bytes = 30 * 1024  # 30 KB
        assert size_bytes > min_bytes, (
            f"[CRITICAL] Artifact {fname} is under 30 KB ({size_bytes} bytes)"
        )

        # Check UTF-8 BOM on plain text publications
        if fname.endswith(".txt"):
            with open(fpath, "rb") as f:
                header = f.read(3)
            assert header == b"\xef\xbb\xbf", (
                f"[CRITICAL] Artifact {fname} lacks required UTF-8 BOM signature"
            )
