import math
from typing import List


def compute_stats(durations: List[float]) -> dict:
    if not durations:
        return {"min_s": 0.0, "max_s": 0.0, "mean_s": 0.0, "p95_s": 0.0}
    sorted_d = sorted(durations)
    n = len(sorted_d)
    p95_idx = min(int(math.ceil(0.95 * n)) - 1, n - 1)
    return {
        "min_s": sorted_d[0],
        "max_s": sorted_d[-1],
        "mean_s": sum(sorted_d) / n,
        "p95_s": sorted_d[p95_idx],
    }


def throughput(item_count: int, mean_duration_s: float) -> float:
    if mean_duration_s <= 0:
        return 0.0
    return item_count / mean_duration_s
