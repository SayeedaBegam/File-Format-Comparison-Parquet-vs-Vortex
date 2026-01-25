from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "out"
UPLOAD_DIR = OUT_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, static_folder=str(REPO_ROOT), static_url_path="")


def _latest_report() -> Path | None:
  reports = sorted(OUT_DIR.glob("report_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
  return reports[0] if reports else None


def _plot_entries(dataset_name: str) -> list[dict[str, str]]:
  plots_dir = OUT_DIR / "plots" / dataset_name
  if not plots_dir.exists():
    return []
  plots = []
  for plot in sorted(plots_dir.glob("*.png")):
    plots.append(
      {
        "name": plot.stem.replace("_", " "),
        "url": f"/{plot.relative_to(REPO_ROOT).as_posix()}",
      }
    )
  return plots


@app.route("/api/run", methods=["POST"])
def run_benchmark():
  upload = request.files.get("dataset")
  if not upload:
    return jsonify({"error": "Missing dataset file"}), 400

  safe_name = secure_filename(upload.filename or "dataset")
  timestamp = int(time.time())
  filename = f"{timestamp}_{safe_name}"
  input_path = UPLOAD_DIR / filename
  upload.save(input_path)

  input_type = "parquet" if safe_name.lower().endswith(".parquet") else "csv"
  cmd = [
    sys.executable,
    "bench/run.py",
    "--input",
    str(input_path),
    "--input-type",
    input_type,
    "--auto-cols",
    "--csv-sample-size",
    "-1",
    "--csv-ignore-errors",
    "--no-like-tests",
    "--out",
    "out",
  ]

  result = subprocess.run(
    cmd,
    cwd=REPO_ROOT,
    capture_output=True,
    text=True,
  )

  if result.returncode != 0:
    print("Benchmark failed")
    print(result.stdout[-4000:])
    print(result.stderr[-4000:])
    return (
      jsonify(
        {
          "error": "Benchmark failed",
          "stdout": result.stdout[-4000:],
          "stderr": result.stderr[-4000:],
        }
      ),
      500,
    )

  report_path = _latest_report()
  if not report_path:
    return jsonify({"error": "Report JSON not found"}), 500

  report = json.loads(report_path.read_text(encoding="utf-8"))
  dataset_name = report_path.stem.replace("report_", "")

  return jsonify(
    {
      "report": report,
      "report_path": str(report_path.relative_to(REPO_ROOT).as_posix()),
      "plots": _plot_entries(dataset_name),
      "stdout": result.stdout[-4000:],
      "stderr": result.stderr[-4000:],
    }
  )


@app.route("/")
def root():
  return app.send_static_file("website/index.html")


if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)
