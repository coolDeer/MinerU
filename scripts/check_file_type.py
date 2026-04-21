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

    if final in SUPPORTED:
        print(f"✅ Supported by MinerU as: {SUPPORTED[final]}")
        return 0
    print(f"⚠️  NOT in MinerU's supported list — will be skipped")
    print(f"    Expected based on extension: {path.suffix.lower()}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
