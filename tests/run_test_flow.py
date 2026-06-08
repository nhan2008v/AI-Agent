# -*- coding: utf-8 -*-
import sys as _sys
if _sys.platform == "win32":
    import io as _io
    _sys.stdout = _io.TextIOWrapper(_sys.stdout.buffer, encoding='utf-8', errors='replace')
    _sys.stderr = _io.TextIOWrapper(_sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
run_test_flow.py
================
Script test flow tự động: chạy pipeline qua từng dirty dataset,
ghi lại kết quả profiler + input_validator vào JSON, tóm tắt kết quả.

Usage:
    # Bước 1: Sinh dataset trước
    python tests/generate_test_datasets.py

    # Bước 2: Chạy test flow
    python tests/run_test_flow.py

    # Chỉ test 1 dataset cụ thể
    python tests/run_test_flow.py --dataset DS01

    # Chỉ test profiler (không gọi LLM)
    python tests/run_test_flow.py --profiler-only

Output:
    tests/test_results/
        run_<timestamp>/
            DS01_result.json
            DS02_result.json
            ...
            SUMMARY.json
            SUMMARY.txt
"""

import argparse
import asyncio
import json
import selectors
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path

# ─── Ensure project root is on sys.path ─────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ingestion.normalizer import ingest_to_canonical
from app.tools.data.eda.profiler import StatisticalProfiler

DATASET_DIR = Path(__file__).parent / "dirty_datasets"
RESULTS_DIR = Path(__file__).parent / "test_results"

# 10 dirty datasets + metadata về expected behaviors
DATASET_REGISTRY = [
    {
        "id": "DS01",
        "file": "DS01_full_row_duplicate.csv",
        "domain": "Bệnh nhân / Hospital",
        "user_prompt": "Resolve all null, duplicate, and typecasting errors present in the dataset",
        "expected_issues": ["duplicate"],
        "expected_validator_status": "ready",
        "notes": "500 full-row duplicates. Expect profiler: duplicate_rows=500. Dedup agent should drop 500 rows.",
    },
    {
        "id": "DS02",
        "file": "DS02_pk_duplicate.csv",
        "domain": "Đơn hàng / E-commerce",
        "user_prompt": "Clean all duplicate orders, keep the first occurrence for each order_id",
        "expected_issues": ["duplicate"],
        "expected_validator_status": "ready",
        "notes": "300 PK duplicates on order_id. Validator should identify order_id as near_unique.",
    },
    {
        "id": "DS03",
        "file": "DS03_high_null.csv",
        "domain": "Khảo sát / Survey",
        "user_prompt": "clean the data",
        "expected_issues": ["null"],
        "expected_validator_status": "needs_clarification",
        "notes": "Vague prompt + nhiều cột null cao → validator nên hỏi clarification.",
    },
    {
        "id": "DS04",
        "file": "DS04_null_and_dmv.csv",
        "domain": "Nhân sự / HR",
        "user_prompt": "Remove all null values and disguised missing values (N/A, unknown, -) from the dataset",
        "expected_issues": ["null"],
        "expected_validator_status": "ready",
        "notes": "DMV + null. Profiler phải detect disguised nulls. Validator ready vì prompt cụ thể.",
    },
    {
        "id": "DS05",
        "file": "DS05_type_mismatch.csv",
        "domain": "Giao dịch / Finance",
        "user_prompt": "Fix all type errors: cast amount to float, convert txn_date to proper date format, cast is_flagged to boolean",
        "expected_issues": ["typecast"],
        "expected_validator_status": "ready",
        "notes": "Type mismatch nghiêm trọng. Semantic profiler phải detect type_mismatch trên amount, txn_date.",
    },
    {
        "id": "DS06",
        "file": "DS06_mixed_type.csv",
        "domain": "Điểm thi / Education",
        "user_prompt": "process this dataset",
        "expected_issues": ["null", "typecast"],
        "expected_validator_status": "needs_clarification",
        "notes": "Vague prompt + mixed type. Validator hỏi clarification về typecast strategy.",
    },
    {
        "id": "DS07",
        "file": "DS07_pk_dup_and_null.csv",
        "domain": "Sản phẩm / Inventory",
        "user_prompt": "Resolve all null, duplicate, and typecasting errors present in the dataset",
        "expected_issues": ["duplicate", "null"],
        "expected_validator_status": "ready",
        "notes": "PK dup 250 cases + NULL 20-35%. Validator ready vì prompt đủ cụ thể.",
    },
    {
        "id": "DS08",
        "file": "DS08_null_and_typecast.csv",
        "domain": "IoT Sensor",
        "user_prompt": "fix errors",
        "expected_issues": ["null", "typecast"],
        "expected_validator_status": "needs_clarification",
        "notes": "Vague prompt + null cao + type mismatch → hỏi clarification.",
    },
    {
        "id": "DS09",
        "file": "DS09_all_issues.csv",
        "domain": "Hồ sơ Bệnh viện (Mega Dirty)",
        "user_prompt": "Resolve all null, duplicate, and typecasting errors present in the dataset",
        "expected_issues": ["duplicate", "null", "typecast"],
        "expected_validator_status": "ready",
        "notes": "ALL issues. Largest test case. Validator should detect all 3 issue types.",
    },
    {
        "id": "DS10",
        "file": "DS10_edge_cases.csv",
        "domain": "Logistics",
        "user_prompt": "clean the data",
        "expected_issues": ["null", "typecast"],
        "expected_validator_status": "needs_clarification",
        "notes": "Edge: constant col, tên cột có space, near-constant. Vague prompt → clarification.",
    },
]


# ─────────────────────────────────────────────────────────────────
# Profiler-only test (không cần LLM)
# ─────────────────────────────────────────────────────────────────

def run_profiler_test(dataset_info: dict, run_dir: Path) -> dict:
    """Chạy statistical profiler và ghi kết quả ra file."""
    ds_id = dataset_info["id"]
    ds_file = DATASET_DIR / dataset_info["file"]

    result = {
        "dataset_id": ds_id,
        "file": str(ds_file),
        "domain": dataset_info["domain"],
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
        "profiler_result": None,
        "profiler_summary": {},
        "validator_result": None,
        "error": None,
        "assertions": [],
    }

    if not ds_file.exists():
        result["status"] = "SKIP"
        result["error"] = f"Dataset file not found: {ds_file}"
        print(f"  [{ds_id}] SKIP -- file not found")
        return result

    print(f"  [{ds_id}] Profiling {dataset_info['file']}...")

    try:
        # Ingest to canonical parquet
        canonical_path, input_format, _ = ingest_to_canonical(ds_file)
        print(f"    -> Canonical: {canonical_path}")

        # Run statistical profiler
        profiler = StatisticalProfiler()
        profile = profiler.profile_parquet(str(canonical_path))

        profile_dict = profile.to_dict()
        result["profiler_result"] = profile_dict
        result["profiler_summary"] = {
            "total_rows": profile.total_rows,
            "total_columns": profile.total_columns,
            "duplicate_rows": profile.duplicate_rows,
            "pk_candidates": profile.pk_candidates,
            "near_unique_columns": profile.near_unique_columns,
            "categorical_columns": profile.categorical_columns,
            "high_null_columns": profile.high_null_columns,
            "column_null_rates": {
                c.column_name: round(c.null_rate, 4)
                for c in profile.columns
            },
            "column_dtypes": {
                c.column_name: c.dtype
                for c in profile.columns
            },
            "column_unique_ratios": {
                c.column_name: round(c.unique_ratio, 4)
                for c in profile.columns
            },
        }

        # Assertions: kiểm tra xem profiler có detect đúng không
        assertions = []
        expected_issues = dataset_info["expected_issues"]

        if "duplicate" in expected_issues:
            has_dup = profile.duplicate_rows > 0 or len(profile.near_unique_columns) > 0
            assertions.append({
                "check": "duplicate_detected",
                "passed": has_dup,
                "detail": f"duplicate_rows={profile.duplicate_rows}, near_unique={profile.near_unique_columns}",
            })

        if "null" in expected_issues:
            has_null = len(profile.high_null_columns) > 0 or any(c.null_rate > 0 for c in profile.columns)
            assertions.append({
                "check": "null_detected",
                "passed": has_null,
                "detail": f"high_null_cols={profile.high_null_columns}",
            })

        result["assertions"] = assertions
        all_passed = all(a["passed"] for a in assertions)
        result["status"] = "PASS" if all_passed else "FAIL"

        print(f"    → Rows: {profile.total_rows} | Dup rows: {profile.duplicate_rows}")
        print(f"    → PK candidates: {profile.pk_candidates}")
        print(f"    → Near-unique: {profile.near_unique_columns}")
        print(f"    → High-null (>50%): {profile.high_null_columns}")
        print(f"    → Status: {result['status']}")

        # Cleanup canonical
        try:
            canonical_path.unlink()
        except Exception:
            pass

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
        print(f"    [ERR] ERROR: {e}")

    # Save result
    out_file = run_dir / f"{ds_id}_profiler_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)

    return result


# ─────────────────────────────────────────────────────────────────
# Full pipeline test (cần LLM)
# ─────────────────────────────────────────────────────────────────

async def run_full_pipeline_test(dataset_info: dict, run_dir: Path) -> dict:
    """Chạy toàn bộ pipeline và ghi kết quả."""
    from app.services.pipeline import run_pipeline, get_pipeline_state

    ds_id = dataset_info["id"]
    ds_file = DATASET_DIR / dataset_info["file"]

    result = {
        "dataset_id": ds_id,
        "file": str(ds_file),
        "domain": dataset_info["domain"],
        "user_prompt": dataset_info["user_prompt"],
        "expected_issues": dataset_info["expected_issues"],
        "expected_validator_status": dataset_info["expected_validator_status"],
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
        "profiler_summary": {},
        "validator_status_actual": None,
        "validator_reasoning": None,
        "validator_action_plan": None,
        "validator_clarifications": None,
        "status_match": None,
        "assertions": [],
        "error": None,
        "run_id": None,
    }

    if not ds_file.exists():
        result["status"] = "SKIP"
        result["error"] = f"Dataset file not found: {ds_file}"
        return result

    print(f"  [{ds_id}] Running FULL PIPELINE for {dataset_info['file']}...")
    print(f"    → User prompt: '{dataset_info['user_prompt']}'")

    try:
        # Ingest
        canonical_path, input_format, _ = ingest_to_canonical(ds_file)

        # Pipeline
        run_id = f"test-{ds_id.lower()}-{uuid.uuid4().hex[:8]}"
        result["run_id"] = run_id

        await run_pipeline(
            run_id=run_id,
            canonical_path=str(canonical_path),
            input_format=input_format.value,
            user_prompt=dataset_info["user_prompt"],
            original_filename=dataset_info["file"],
        )

        # Get state
        state = await get_pipeline_state(run_id)
        if not state:
            result["status"] = "ERROR"
            result["error"] = "Failed to retrieve pipeline state"
            return result

        # Extract profiler summary
        stat_prof = state.get("statistical_profile")
        if stat_prof:
            def gv(obj, key, default=None):
                if isinstance(obj, dict): return obj.get(key, default)
                return getattr(obj, key, default)

            result["profiler_summary"] = {
                "total_rows": gv(stat_prof, "total_rows"),
                "total_columns": gv(stat_prof, "total_columns"),
                "duplicate_rows": gv(stat_prof, "duplicate_rows"),
                "pk_candidates": gv(stat_prof, "pk_candidates"),
                "near_unique_columns": gv(stat_prof, "near_unique_columns"),
                "high_null_columns": gv(stat_prof, "high_null_columns"),
            }

        # Extract validator result
        val = state.get("input_validation_result")
        if val:
            def gv(obj, key, default=None):
                if isinstance(obj, dict): return obj.get(key, default)
                return getattr(obj, key, default)

            actual_status = gv(val, "status")
            result["validator_status_actual"] = actual_status
            result["validator_reasoning"] = gv(val, "reasoning")
            result["validator_action_plan"] = gv(val, "action_plan")
            result["validator_clarifications"] = gv(val, "clarifications")

            # Check if status matches expected
            expected_status = dataset_info["expected_validator_status"]
            status_match = (actual_status == expected_status)
            result["status_match"] = status_match

            ok_icon = "OK" if status_match else "FAIL"
            print(f"    -> Validator status: {actual_status} (expected: {expected_status}) [{ok_icon}]")

            # Assertions
            assertions = []
            assertions.append({
                "check": "validator_status_correct",
                "passed": status_match,
                "detail": f"expected={expected_status}, actual={actual_status}",
            })

            # Check clarifications generated when needed
            if expected_status == "needs_clarification":
                clars = gv(val, "clarifications")
                has_clars = clars is not None and isinstance(clars, dict) and any(
                    clars.get(k) for k in ["null", "duplicate", "typecast"]
                )
                assertions.append({
                    "check": "clarifications_generated",
                    "passed": has_clars,
                    "detail": f"clarifications={'present' if has_clars else 'absent'}",
                })

            result["assertions"] = assertions
            all_passed = all(a["passed"] for a in assertions)
            result["status"] = "PASS" if all_passed else "FAIL"

        else:
            result["status"] = "FAIL"
            result["error"] = "No input_validation_result found in state"

        # Cleanup
        try:
            canonical_path.unlink()
        except Exception:
            pass

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
        print(f"    [ERR] ERROR: {e}")

    # Save result
    out_file = run_dir / f"{ds_id}_full_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)

    return result


# ─────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────

def write_summary(results: list[dict], run_dir: Path, mode: str):
    """Ghi file SUMMARY.json và SUMMARY.txt."""
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    skipped = sum(1 for r in results if r["status"] == "SKIP")

    summary = {
        "mode": mode,
        "timestamp": datetime.now().isoformat(),
        "total": total,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "skipped": skipped,
        "pass_rate": f"{passed/total*100:.1f}%" if total > 0 else "N/A",
        "results": [
            {
                "id": r["dataset_id"],
                "domain": r.get("domain"),
                "status": r["status"],
                "assertions": r.get("assertions", []),
                "error": r.get("error"),
            }
            for r in results
        ],
    }

    # Save JSON
    json_path = run_dir / "SUMMARY.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Save human-readable TXT
    txt_path = run_dir / "SUMMARY.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write(f"  TEST RUN SUMMARY — Mode: {mode}\n")
        f.write(f"  Timestamp: {summary['timestamp']}\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"  TOTAL:   {total}\n")
        f.write(f"  PASS:    {passed}  ✓\n")
        f.write(f"  FAIL:    {failed}  ✗\n")
        f.write(f"  ERROR:   {errors}  !\n")
        f.write(f"  SKIP:    {skipped}  -\n")
        f.write(f"  PASS RATE: {summary['pass_rate']}\n\n")
        f.write("-" * 70 + "\n")
        f.write("  Per-Dataset Results:\n\n")

        for r in results:
            icon = {"PASS": "✓", "FAIL": "✗", "ERROR": "!", "SKIP": "-"}.get(r["status"], "?")
            f.write(f"  [{icon}] {r['dataset_id']} — {r.get('domain', '')}\n")
            f.write(f"      Status: {r['status']}\n")
            if r.get("assertions"):
                for a in r["assertions"]:
                    a_icon = "✓" if a["passed"] else "✗"
                    f.write(f"        [{a_icon}] {a['check']}: {a['detail']}\n")
            if r.get("error"):
                f.write(f"      Error: {r['error']}\n")
            f.write("\n")

    print(f"\n{'='*60}")
    print(f"  SUMMARY: {passed}/{total} PASS | {failed} FAIL | {errors} ERROR")
    print(f"  Results saved: {run_dir}")
    print(f"{'='*60}")

    return summary


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────

async def async_main(args):
    # Create run directory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "profiler" if args.profiler_only else "full"
    run_dir = RESULTS_DIR / f"run_{ts}_{mode}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(f"  Agentic Data Cleaner — Test Flow")
    print(f"  Mode: {'PROFILER ONLY' if args.profiler_only else 'FULL PIPELINE (LLM)'}")
    print(f"  Run dir: {run_dir}")
    print("=" * 70)

    # Filter datasets
    datasets = DATASET_REGISTRY
    if args.dataset:
        filter_ids = [d.strip().upper() for d in args.dataset.split(",")]
        datasets = [d for d in datasets if d["id"] in filter_ids]
        if not datasets:
            print(f"WARNING: No datasets found matching: {args.dataset}")
            return

    print(f"\n  Running {len(datasets)} dataset(s)...\n")

    results = []
    for ds_info in datasets:
        if args.profiler_only:
            r = run_profiler_test(ds_info, run_dir)
        else:
            r = await run_full_pipeline_test(ds_info, run_dir)
        results.append(r)
        print()

    # Write summary
    write_summary(results, run_dir, mode)


def main():
    parser = argparse.ArgumentParser(description="Run test flow for Agentic Data Cleaner")
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Comma-separated dataset IDs to test (e.g. DS01,DS02). Default: all.",
    )
    parser.add_argument(
        "--profiler-only",
        action="store_true",
        help="Only run StatisticalProfiler (no LLM calls). Much faster.",
    )
    args = parser.parse_args()

    if sys.platform == "win32":
        loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    else:
        loop_factory = None

    asyncio.run(async_main(args), loop_factory=loop_factory)


if __name__ == "__main__":
    main()
