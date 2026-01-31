from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "out"
UPLOAD_DIR = OUT_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = REPO_ROOT / "website" / "data" / "datasets.json"

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


def _update_manifest(dataset_name: str, report_path: Path) -> None:
  if not MANIFEST_PATH.exists():
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    manifest = {"datasets": []}
  else:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
  datasets = manifest.get("datasets", [])
  rel_report = report_path.relative_to(REPO_ROOT).as_posix()
  if not any(d.get("name") == dataset_name for d in datasets):
    datasets.append({"name": dataset_name, "report": f"./{rel_report}"})
  manifest["datasets"] = datasets
  MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _load_manifest() -> dict:
  if not MANIFEST_PATH.exists():
    return {"datasets": []}
  return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _load_overall_summary() -> dict | None:
  summary_path = OUT_DIR / "overall_summary.json"
  if not summary_path.exists():
    return None
  return json.loads(summary_path.read_text(encoding="utf-8"))


def _escape_path(path: Path) -> str:
  return str(path).replace("'", "''")


def _load_preview(input_path: Path, input_type: str) -> dict[str, list]:
  con = duckdb.connect(database=":memory:")
  escaped = _escape_path(input_path)
  if input_type == "parquet":
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{escaped}');")
  else:
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_csv_auto('{escaped}');")
  rows = con.execute("SELECT * FROM data LIMIT 10;").fetchall()
  cols = [row[0] for row in con.execute("DESCRIBE data;").fetchall()]
  return {
    "columns": cols,
    "rows": rows,
  }


def _timed_query(con: duckdb.DuckDBPyConnection, sql: str, repeats: int, warmup: int) -> dict:
  for _ in range(max(0, warmup)):
    con.execute(sql).fetchall()
  times = []
  result_value = None
  for _ in range(max(1, repeats)):
    t0 = time.perf_counter()
    res = con.execute(sql).fetchone()
    t1 = time.perf_counter()
    times.append((t1 - t0) * 1000.0)
    result_value = res[0] if res else None
  times_sorted = sorted(times)
  median = times_sorted[len(times_sorted) // 2]
  p95 = times_sorted[int(0.95 * (len(times_sorted) - 1))]
  return {"median_ms": median, "p95_ms": p95, "runs": len(times), "result_value": result_value}


@app.route("/api/run", methods=["POST"])
def run_benchmark():
  upload = request.files.get("dataset")
  schema_upload = request.files.get("schema")
  sort_col = request.form.get("sort_col", "").strip()
  if not upload:
    return jsonify({"error": "Missing dataset file"}), 400

  safe_name = secure_filename(upload.filename or "dataset")
  timestamp = int(time.time())
  filename = f"{timestamp}_{safe_name}"
  input_path = UPLOAD_DIR / filename
  upload.save(input_path)
  schema_path = None
  if schema_upload and schema_upload.filename:
    schema_name = secure_filename(schema_upload.filename or "schema.sql")
    schema_path = UPLOAD_DIR / f"{timestamp}_{schema_name}"
    schema_upload.save(schema_path)

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
    "--include-cold",
    "--baseline-duckdb",
  ]
  if sort_col:
    cmd.extend(["--sorted-by", sort_col])
  if schema_path is not None:
    cmd.extend(["--schema", str(schema_path)])

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
  _update_manifest(dataset_name, report_path)
  manifest = _load_manifest()
  summary = _load_overall_summary()

  preview = _load_preview(input_path, input_type)

  return jsonify(
    {
      "report": report,
      "report_path": str(report_path.relative_to(REPO_ROOT).as_posix()),
      "plots": _plot_entries(dataset_name),
      "preview": preview,
      "upload": {"filename": filename, "input_type": input_type},
      "manifest": manifest,
      "summary": summary,
      "stdout": result.stdout[-4000:],
      "stderr": result.stderr[-4000:],
    }
  )


@app.route("/api/query", methods=["POST"])
def run_custom_query():
  payload = request.get_json(silent=True) or {}
  filename = payload.get("filename")
  input_type = payload.get("input_type")
  sql = (payload.get("sql") or "").strip()
  repeats = int(payload.get("repeats") or 5)
  warmup = int(payload.get("warmup") or 1)
  if not filename or not sql or input_type not in {"csv", "parquet"}:
    return jsonify({"error": "Missing query, filename, or input type."}), 400
  if not sql.lower().startswith("select"):
    return jsonify({"error": "Only SELECT queries are allowed."}), 400

  input_path = UPLOAD_DIR / filename
  if not input_path.exists():
    return jsonify({"error": "Uploaded file not found."}), 404

  con = duckdb.connect(database=":memory:")
  escaped = _escape_path(input_path)
  if input_type == "parquet":
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{escaped}');")
  else:
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_csv_auto('{escaped}');")

  result = _timed_query(con, sql, repeats=repeats, warmup=warmup)
  return jsonify(result)


@app.route("/")
def root():
  return app.send_static_file("website/index.html")


if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)
