# bench/report/report.py
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Any, List


def write_csv(rows: List[Dict[str, Any]], out_csv: str) -> None:
    p = Path(out_csv)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        p.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(obj: Dict[str, Any], out_json: str) -> None:
    p = Path(out_json)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(summary_md: str, out_md: str) -> None:
    p = Path(out_md)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(summary_md, encoding="utf-8")
