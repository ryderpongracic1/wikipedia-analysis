import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import List

from .runner import BenchmarkResult
from .stats import compute_stats


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def write_report(results: List[BenchmarkResult], dataset_info: dict, neo4j_version: str = "4.4") -> str:
    report = {
        "schema_version": "1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "python_version": sys.version.split()[0],
        "neo4j_version": neo4j_version,
        "dataset": dataset_info,
        "benchmarks": [r.to_dict() for r in results],
    }

    output_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(output_dir, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(output_dir, f"{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)

    _print_summary(results)
    return path


def _print_summary(results: List[BenchmarkResult]) -> None:
    print("\n" + "=" * 72)
    print(f"{'Benchmark':<40} {'mean(s)':>8} {'p95(s)':>8} {'n':>4} {'err':>4}")
    print("-" * 72)
    for r in results:
        stats = compute_stats(r.durations)
        print(
            f"{r.name:<40} {stats['mean_s']:>8.4f} {stats['p95_s']:>8.4f}"
            f" {r.result_count:>4} {r.errors:>4}"
        )
    print("=" * 72)
