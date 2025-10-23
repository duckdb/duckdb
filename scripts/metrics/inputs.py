from __future__ import annotations
import json
import re
from pathlib import Path

IDENT_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def load_metrics_json(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"metric_type.json not found at {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_identifier(name: str, group: str) -> None:
    if not IDENT_RE.match(name):
        raise ValueError(f"Invalid metric identifier: {name}, in group {group}")


def _to_pascal_case(s: str) -> str:
    return ''.join(word.capitalize() for word in s.split('_'))


def retrieve_optimizers(optimizer_file: Path) -> list[str]:
    if not optimizer_file.exists():
        raise FileNotFoundError(f"optimizer_type.hpp not found at {optimizer_file}.")
    enum_pattern = r"\s*([A-Z_]+)\s*=\s*\d+,?|\s*([A-Z_]+),?"
    inside_enum = False
    result: list[str] = []
    with optimizer_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("enum class OptimizerType"):
                inside_enum = True
                continue

            if inside_enum and line.startswith("};"):
                break

            if inside_enum:
                m = re.match(enum_pattern, line)
                if not m:
                    continue
                name = m[1] if m[1] else m[2]
                if name == "INVALID":
                    continue
                result.append(name)

    result.sort()
    return result
