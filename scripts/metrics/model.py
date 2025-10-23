from __future__ import annotations
from collections import OrderedDict
from typing import Dict, List, Tuple


def build_all_metrics(metrics_json: list[dict], optimizers: list[str]) -> Tuple[Dict[str, List[str]], List[str]]:
    root_scope_metrics: list[str] = []
    unsorted: dict[str, list[str]] = {}

    for group in metrics_json:
        if "metrics" not in group:
            continue
        names: list[str] = []
        for metric in group["metrics"]:
            name = metric["name"]
            names.append(name)
            if "query_root" in metric:
                root_scope_metrics.append(name)
        names.sort()
        unsorted[group["group"]] = names

    root_scope_metrics.sort()

    # Optimizers
    optimizer_metrics = [f"OPTIMIZER_{o}" for o in optimizers]
    unsorted["optimizer"] = optimizer_metrics

    # All
    all_set = set()
    for lst in unsorted.values():
        all_set.update(lst)
    unsorted["all"] = sorted(all_set)

    all_metrics = OrderedDict(sorted(unsorted.items()))
    return all_metrics, sorted(set(root_scope_metrics))
