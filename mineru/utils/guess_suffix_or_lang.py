# Copyright (c) Opendatalab. All rights reserved.
import zipfile
from io import BytesIO
from pathlib import Path

from loguru import logger
from magika import Magika


DEFAULT_LANG = "txt"
PDF_SIG_BYTES = b'%PDF'
ZIP_SIG_BYTES = b'PK\x03\x04'
OOXML_SUFFIXES = (".xlsx", ".docx", ".pptx")
OOXML_CONTENT_TYPE_MARKERS = {
    "xlsx": b"spreadsheetml",
    "docx": b"wordprocessingml",
    "pptx": b"presentationml",
}
magika = Magika()


def _detect_ooxml_variant_from_zip(zip_source) -> str | None:
    """Inspect an OOXML (xlsx/docx/pptx) zip archive and return its variant.

    OOXML files are zip containers with a [Content_Types].xml describing the
    spreadsheet/document/presentation payload. Magika sometimes labels them
    as generic "zip" — this helper recovers the specific variant.
    """
    try:
        with zipfile.ZipFile(zip_source) as zf:
            if "[Content_Types].xml" not in zf.namelist():
                return None
            content_types = zf.read("[Content_Types].xml")
            for variant, marker in OOXML_CONTENT_TYPE_MARKERS.items():
                if marker in content_types:
                    return variant
    except (zipfile.BadZipFile, KeyError, OSError):
        return None
    return None

def _normalize_text_for_language_guess(code: str) -> str:
    if not code:
        return ""

    normalized = []
    index = 0
    while index < len(code):
        current_char = code[index]
        current_ord = ord(current_char)

        if 0xD800 <= current_ord <= 0xDBFF:
            if index + 1 < len(code):
                next_char = code[index + 1]
                next_ord = ord(next_char)
                if 0xDC00 <= next_ord <= 0xDFFF:
                    pair = current_char + next_char
                    normalized.append(pair.encode("utf-16", "surrogatepass").decode("utf-16"))
                    index += 2
                    continue
            index += 1
            continue

        if 0xDC00 <= current_ord <= 0xDFFF:
            index += 1
            continue

        normalized.append(current_char)
        index += 1

    return "".join(normalized)


def guess_language_by_text(code):
    normalized_code = _normalize_text_for_language_guess(code)
    if not normalized_code:
        return DEFAULT_LANG

    try:
        codebytes = normalized_code.encode("utf-8", errors="replace")
        lang = magika.identify_bytes(codebytes).prediction.output.label
    except Exception:
        return DEFAULT_LANG

    return lang if lang != "unknown" else DEFAULT_LANG


def _maybe_recover_ooxml(suffix: str, zip_source, hint_suffix: str) -> str:
    """Verify suspected OOXML files via [Content_Types].xml.

    Magika can mislabel legitimate xlsx/docx/pptx files as 'zip', 'xlsb', or
    other zip-derivative formats because the container is a zip archive.
    Trust the archive's [Content_Types].xml over Magika's guess when either
    (a) the filename extension claims OOXML and disagrees with Magika, or
    (b) no extension hint is available but Magika's label is zip-like.
    """
    if hint_suffix in OOXML_SUFFIXES:
        expected = hint_suffix.lstrip(".")
        if suffix == expected:
            return suffix
    elif hint_suffix:
        # Non-OOXML extension hint — don't second-guess Magika.
        return suffix
    else:
        # No hint (pure-bytes dispatch): only probe when Magika already thinks
        # it's some zip-family label that might actually be OOXML.
        if suffix in ("xlsx", "docx", "pptx"):
            return suffix
        if suffix not in ("zip", "xlsb", "xlsm", "docm", "pptm"):
            return suffix
    variant = _detect_ooxml_variant_from_zip(zip_source)
    if variant:
        return variant
    return suffix


def guess_suffix_by_bytes(file_bytes, file_path=None) -> str:
    suffix = magika.identify_bytes(file_bytes).prediction.output.label
    if file_path and suffix in ["ai", "html"] and Path(file_path).suffix.lower() in [".pdf"] and file_bytes[:4] == PDF_SIG_BYTES:
        suffix = "pdf"
    if file_bytes[:4] == ZIP_SIG_BYTES:
        hint = Path(file_path).suffix.lower() if file_path else ""
        suffix = _maybe_recover_ooxml(suffix, BytesIO(file_bytes), hint)
    return suffix


def guess_suffix_by_path(file_path) -> str:
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    suffix = magika.identify_path(file_path).prediction.output.label
    if suffix in ["ai", "html"] and file_path.suffix.lower() in [".pdf"]:
        try:
            with open(file_path, 'rb') as f:
                if f.read(4) == PDF_SIG_BYTES:
                    suffix = "pdf"
        except Exception as e:
            logger.warning(f"Failed to read file {file_path} for PDF signature check: {e}")
    suffix = _maybe_recover_ooxml(suffix, file_path, file_path.suffix.lower())
    return suffix
