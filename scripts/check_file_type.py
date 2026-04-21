# Copyright (c) Your Team.
"""File-type detection diagnostic.

Use this script to check how MinerU will classify a file before running
a parse — useful when Magika mis-labels Office/PDF files.

Usage:
    python scripts/check_file_type.py <path>
    python scripts/check_file_type.py           # uses DEFAULT_PATH below
"""
import sys
from pathlib import Path


DEFAULT_PATH = (
    "/Users/bububot/Desktop/project/MinerU/docs/excel/"
    "20260316_222916_SoftBank_Group_Corp_1__1.xlsx"
)

SUPPORTED = {
    "pdf": "PDF",
    "xlsx": "Excel",
    "docx": "Word",
    "pptx": "PowerPoint",
    "png": "image",
    "jpg": "image",
    "jpeg": "image",
    "bmp": "image",
    "gif": "image",
    "tiff": "image",
    "webp": "image",
    "jp2": "image",
}


def main() -> int:
    raw_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    path = Path(raw_path).expanduser()

    print("=" * 60)
    print(f"Path:   {path}")
    print("=" * 60)

    if not path.exists():
        print("❌ File does NOT exist")
        return 1
    if not path.is_file():
        print("❌ Path is not a regular file")
        return 1

    size = path.stat().st_size
    print(f"✅ File exists, size: {size:,} bytes ({size / 1024 / 1024:.2f} MB)")

    with open(path, "rb") as f:
        head = f.read(16)
    print(f"📎 Magic bytes (first 16): {head.hex()}  {head[:4]!r}")

    from magika import Magika

    raw_label = Magika().identify_path(path).prediction.output.label
    print(f"🔍 Magika raw label: {raw_label!r}")

    from mineru.utils.guess_suffix_or_lang import guess_suffix_by_path

    final = guess_suffix_by_path(path)
    print(f"🎯 MinerU final suffix: {final!r}")

    # When the file looks like an OOXML container (starts with PK), peek inside
    # so we can tell xlsx from xlsb/xlsm/docm/etc.
    if head[:4] == b"PK\x03\x04":
        print()
        print("-- ZIP internals --")
        try:
            import zipfile
            with zipfile.ZipFile(path) as zf:
                names = zf.namelist()
                print(f"  entries: {len(names)}")
                for n in names[:8]:
                    print(f"    {n}")
                if len(names) > 8:
                    print(f"    ... ({len(names) - 8} more)")
                if "[Content_Types].xml" in names:
                    content = zf.read("[Content_Types].xml").decode("utf-8", "replace")
                    markers = {
                        "spreadsheetml.sheet.binary": "xlsb (Excel Binary Workbook)",
                        "spreadsheetml.sheet.macroEnabled": "xlsm (Macro-enabled xlsx)",
                        "spreadsheetml.sheet": "xlsx (standard Excel)",
                        "wordprocessingml.document.macroEnabled": "docm (Macro-enabled docx)",
                        "wordprocessingml.document": "docx (standard Word)",
                        "presentationml.presentation.macroEnabled": "pptm (Macro-enabled pptx)",
                        "presentationml.presentation": "pptx (standard PowerPoint)",
                    }
                    matched = next(
                        (label for m, label in markers.items() if m in content),
                        "unknown OOXML variant",
                    )
                    print(f"  [Content_Types].xml payload → {matched}")
                else:
                    print("  [Content_Types].xml NOT present → not standard OOXML")
        except Exception as exc:
            print(f"  (failed to open as zip: {exc})")

    print()
    if final in SUPPORTED:
        print(f"✅ Supported by MinerU as: {SUPPORTED[final]}")
        return 0
    print(f"⚠️  NOT in MinerU's supported list — will be skipped")
    print(f"    Expected based on extension: {path.suffix.lower()}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
