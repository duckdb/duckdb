from __future__ import annotations
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable

from .inputs import validate_identifier


@dataclass(frozen=True)
class MetricDef:
    name: str
    group: str  # e.g., "file", "default", ...
    type: str  # e.g., "double", "uint64", "string", "map", "uint8"
    query_root: bool = False
    is_default: bool = False
    collection_method: Optional[str] = None
    child: Optional[str] = None
    description: str = None
    value: Optional[int] = None


class MetricIndex:
    def __init__(self, defs: Iterable[MetricDef], optimizers: List[str], optimizer_start_value: Optional[int] = None):
        self.defs: List[MetricDef] = list(defs)
        self.optimizer_start_value: Optional[int] = optimizer_start_value

        # Build name → explicit value mapping
        self._value_of: Dict[str, int] = {}
        for d in self.defs:
            if d.value is not None:
                self._value_of[d.name] = d.value

        # Build group → names (existing contract for emitters)
        by_group: Dict[str, List[str]] = defaultdict(list)
        for d in self.defs:
            by_group[d.group].append(d.name)
        for g in by_group:
            by_group[g].sort()

        # Add optimizer group (names only) and assign sequential values from optimizer_start_value
        optimizer_names = [f"OPTIMIZER_{o}" for o in optimizers]
        if optimizer_start_value is not None:
            for i, name in enumerate(optimizer_names):
                self._value_of[name] = optimizer_start_value + i
        by_group["optimizer"] = optimizer_names

        # Validate enum value assignments.
        # Layout: optimizer metrics must be in [34, 127], non-optimizer in [0, 33] or [128, 255].
        # This prevents optimizer metrics (which grow as new optimizers are added) from
        # colliding with non-optimizer metrics.
        OPTIMIZER_RANGE_START = 34
        OPTIMIZER_RANGE_END = 127
        value_to_name: Dict[int, str] = {}
        for name, val in self._value_of.items():
            if val < 0 or val > 255:
                raise ValueError(f"MetricType value {val} for '{name}' is outside uint8_t range (0-255)")
            if name in value_to_name.values():
                continue
            if val in value_to_name:
                raise ValueError(
                    f"Duplicate MetricType value {val}: '{value_to_name[val]}' and '{name}'"
                )
            is_optimizer = name.startswith("OPTIMIZER_")
            if is_optimizer and not (OPTIMIZER_RANGE_START <= val <= OPTIMIZER_RANGE_END):
                raise ValueError(
                    f"Optimizer metric '{name}' has value {val}, "
                    f"must be in [{OPTIMIZER_RANGE_START}, {OPTIMIZER_RANGE_END}]"
                )
            if not is_optimizer and OPTIMIZER_RANGE_START <= val <= OPTIMIZER_RANGE_END:
                raise ValueError(
                    f"Non-optimizer metric '{name}' has value {val}, "
                    f"which is in the optimizer range [{OPTIMIZER_RANGE_START}, {OPTIMIZER_RANGE_END}]"
                )
            value_to_name[val] = name

        # Add "all"
        all_set = set().union(*by_group.values()) if by_group else set()
        by_group["all"] = sorted(all_set)

        # Add "default" based on "is_default" flag
        self.is_default_metrics: List[str] = []
        for d in self.defs:
            if d.is_default:
                self.is_default_metrics.append(d.name)
        self.is_default_metrics.sort()
        by_group["default"] = self.is_default_metrics

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

    def metric_description(self, metric: str) -> Optional[str]:
        for d in self.defs:
            if d.name == metric:
                return d.description
        return None

    def metric_value(self, metric: str) -> Optional[int]:
        return self._value_of.get(metric)


def build_all_metrics(metrics_json: list[dict], optimizers: list[str]) -> MetricIndex:
    defs: list[MetricDef] = []
    optimizer_start_value: Optional[int] = None
    for group in metrics_json:
        gname = group.get("group")
        if gname == "optimizer":
            optimizer_start_value = group.get("optimizer_start_value")
        for metric in group.get("metrics", []):
            name = metric["name"]
            validate_identifier(name, gname)
            mtype = metric.get("type", "double")
            is_default = metric.get("is_default", False)
            query_root = "query_root" in metric
            collection_method = metric.get("collection_method")
            child = metric.get("child")
            description = metric.get("description", "")
            value = metric.get("value")
            defs.append(
                MetricDef(
                    name=name,
                    group=gname,
                    type=mtype if name != "OPERATOR_TYPE" else "uint8",
                    is_default=is_default,
                    query_root=query_root,
                    collection_method=collection_method,
                    child=child,
                    description=description,
                    value=value,
                )
            )
    return MetricIndex(defs, optimizers, optimizer_start_value)
