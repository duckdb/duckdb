from __future__ import annotations
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable

from scripts.metrics.inputs import validate_identifier


@dataclass(frozen=True)
class MetricDef:
    name: str
    group: str  # e.g., "file", "default", ...
    type: str  # e.g., "double", "uint64", "string", "map", "uint8"
    query_root: bool = False
    collection_method: Optional[str] = None
    child: Optional[str] = None


class MetricIndex:
    def __init__(self, defs: Iterable[MetricDef], optimizers: List[str]):
        self.defs: List[MetricDef] = list(defs)

        # Build group → names (existing contract for emitters)
        by_group: Dict[str, List[str]] = defaultdict(list)
        for d in self.defs:
            by_group[d.group].append(d.name)
        for g in by_group:
            by_group[g].sort()

        # Add optimizer group (names only)
        optimizer_names = [f"OPTIMIZER_{o}" for o in sorted(optimizers)]
        by_group["optimizer"] = optimizer_names

        # Add "all"
        all_set = set().union(*by_group.values()) if by_group else set()
        by_group["all"] = sorted(all_set)

        # Deterministic order of groups
        self.metrics_by_group: Dict[str, List[str]] = OrderedDict(sorted(by_group.items()))

        # List of group names from metrics_by_group
        self.group_names: List[str] = sorted(list(self.metrics_by_group.keys()))

        # Root scope list
        self.root_scope: List[str] = sorted({d.name for d in self.defs if d.query_root})

        # Type → names (for profiling utils)
        type_map: Dict[str, List[str]] = defaultdict(list)
        for d in self.defs:
            t = d.type if d.name != "OPERATOR_TYPE" else "uint8"  # preserve your special-case
            type_map[t].append(d.name)
        for t in type_map:
            type_map[t].sort()
        self.type_to_names: Dict[str, List[str]] = dict(type_map)

        # Collection method → names (only for those that have it)
        coll_map: Dict[str, List[str]] = defaultdict(list)
        for d in self.defs:
            if d.collection_method:
                coll_map[d.collection_method].append(d.name)
        for c in coll_map:
            coll_map[c].sort()
        self.collection_to_names: Dict[str, List[str]] = dict(coll_map)

        # Children mapping (name → child-key)
        self.child_of: Dict[str, str] = {d.name: d.child for d in self.defs if d.child is not None}

    def all_metrics(self) -> List[str]:
        return self.metrics_by_group["all"]

    def metrics_per_group(self, group: str) -> List[str]:
        return self.metrics_by_group[group]

    def root_scope_metrics(self) -> List[str]:
        return self.root_scope

    def types_index(self) -> Dict[str, List[str]]:
        return self.type_to_names

    def collection_index(self) -> Dict[str, List[str]]:
        return self.collection_to_names

    def metrics_per_collection(self, collection: str) -> List[str]:
        return self.collection_to_names[collection]

    def child_index(self) -> Dict[str, str]:
        return self.child_of

    def metric_type(self, metric: str) -> str:
        for d in self.defs:
            if d.name == metric:
                if d.type == "uint8":
                    return "uint8_t"
                elif d.type == "uint64":
                    return "uint64_t"
                return d.type
        raise Exception(f"Unknown metric: {metric}")

    def metric_child(self, metric: str) -> Optional[str]:
        for d in self.defs:
            if d.name == metric:
                return d.child
        return None


def build_all_metrics(metrics_json: list[dict], optimizers: list[str]) -> MetricIndex:
    defs: list[MetricDef] = []
    for group in metrics_json:
        gname = group.get("group")
        for metric in group.get("metrics", []):
            name = metric["name"]
            validate_identifier(name, gname)
            mtype = metric.get("type", "double")
            query_root = "query_root" in metric
            collection_method = metric.get("collection_method")
            child = metric.get("child")
            defs.append(
                MetricDef(
                    name=name,
                    group=gname,
                    type=mtype if name != "OPERATOR_TYPE" else "uint8",
                    query_root=query_root,
                    collection_method=collection_method,
                    child=child,
                )
            )
    return MetricIndex(defs, optimizers)
