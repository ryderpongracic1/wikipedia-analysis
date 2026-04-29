from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from wikipedia_analysis.analysis import measure_performance
from .stats import compute_stats, throughput as _throughput


@dataclass
class BenchmarkResult:
    name: str
    category: str
    repeats: int
    durations: List[float]
    result_count: int
    throughput_items_per_s: Optional[float]
    errors: int

    def to_dict(self) -> dict:
        stats = compute_stats(self.durations)
        return {
            "name": self.name,
            "category": self.category,
            "repeats": self.repeats,
            **stats,
            "result_count": self.result_count,
            "throughput_items_per_s": self.throughput_items_per_s,
            "errors": self.errors,
        }


class BenchmarkRunner:
    def __init__(self, name: str, category: str = "general", repeats: int = 10):
        self.name = name
        self.category = category
        self.repeats = repeats

    def run(self, func: Callable, *args: Any, **kwargs: Any) -> BenchmarkResult:
        durations: List[float] = []
        last_result: Any = None
        errors = 0

        for _ in range(self.repeats):
            try:
                result, duration = measure_performance(func, *args, **kwargs)
                durations.append(duration)
                last_result = result
            except Exception:
                errors += 1

        result_count = 0
        if last_result is not None:
            try:
                result_count = len(last_result)
            except TypeError:
                result_count = 1

        return BenchmarkResult(
            name=self.name,
            category=self.category,
            repeats=self.repeats,
            durations=durations,
            result_count=result_count,
            throughput_items_per_s=None,
            errors=errors,
        )

    def run_with_throughput(self, item_count: int, func: Callable, *args: Any, **kwargs: Any) -> BenchmarkResult:
        result = self.run(func, *args, **kwargs)
        stats = compute_stats(result.durations)
        result.throughput_items_per_s = _throughput(item_count, stats["mean_s"])
        return result
