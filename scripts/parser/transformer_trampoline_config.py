import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Mapping, Optional

from grammar_types import load_grammar_types_yaml


class TrampolineRuleMode(str, Enum):
    FORWARD_IF_SINGLE_CHILD = "forward_if_single_child"
    MANUAL = "manual"
    MANUAL_FINALIZE = "manual_finalize"
    FORWARD = "forward"
    EXCLUDED = "excluded"


@dataclass(frozen=True)
class TrampolineRuleConfig:
    rule_name: str
    mode: TrampolineRuleMode
    init: Optional[str] = None
    finalize: Optional[str] = None


def _fail(config_file: Path, errors):
    print(f"Error: {config_file} contains invalid trampoline transformer metadata:", file=sys.stderr)
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)


def load_transformer_trampoline_config(
    config_file: Path, known_rules, require_known_rules: bool = True
) -> Dict[str, TrampolineRuleConfig]:
    data = load_grammar_types_yaml(config_file)
    rules = data.get("rules", {})
    errors = []

    if not isinstance(rules, Mapping):
        errors.append("top-level 'rules' entry must be a mapping")
        _fail(config_file, errors)

    forward_single_child = data.get("forward_single_child", [])
    if not isinstance(forward_single_child, list):
        errors.append("top-level 'forward_single_child' entry must be a list")
        _fail(config_file, errors)

    rules = dict(rules)
    for rule_name in forward_single_child:
        if not isinstance(rule_name, str):
            errors.append("'forward_single_child' entries must be rule names")
        elif rule_name in rules:
            errors.append(f"rule '{rule_name}' is declared more than once")
        else:
            rules[rule_name] = {"mode": TrampolineRuleMode.FORWARD_IF_SINGLE_CHILD}

    result = {}
    known_rule_set = set(known_rules)
    for rule_name, entry in rules.items():
        if not isinstance(entry, Mapping):
            errors.append(f"rule '{rule_name}' must use a mapping")
            continue
        if require_known_rules and rule_name not in known_rule_set:
            errors.append(f"rule '{rule_name}' does not exist in the grammar")
            continue

        mode_value = entry.get("mode")
        init = entry.get("init")
        finalize = entry.get("finalize")

        try:
            mode = TrampolineRuleMode(mode_value)
        except ValueError:
            valid_modes = ", ".join(mode.value for mode in TrampolineRuleMode)
            errors.append(f"rule '{rule_name}' has invalid mode '{mode_value}' (expected one of: {valid_modes})")
            continue

        if init is not None and not isinstance(init, str):
            errors.append(f"rule '{rule_name}' has non-string init hook")
        if finalize is not None and not isinstance(finalize, str):
            errors.append(f"rule '{rule_name}' has non-string finalize hook")

        if mode == TrampolineRuleMode.MANUAL:
            if not init:
                errors.append(f"manual rule '{rule_name}' must declare an init hook")
            if not finalize:
                errors.append(f"manual rule '{rule_name}' must declare a finalize hook")
        elif mode == TrampolineRuleMode.MANUAL_FINALIZE:
            if init:
                errors.append(f"manual_finalize rule '{rule_name}' must not declare an init hook")
            if not finalize:
                errors.append(f"manual_finalize rule '{rule_name}' must declare a finalize hook")
        elif mode in (TrampolineRuleMode.FORWARD, TrampolineRuleMode.FORWARD_IF_SINGLE_CHILD):
            if init or finalize:
                errors.append(f"{mode.value} rule '{rule_name}' must not declare hooks")
        elif mode == TrampolineRuleMode.EXCLUDED:
            if init or finalize:
                errors.append(f"excluded rule '{rule_name}' must not declare hooks")

        result[rule_name] = TrampolineRuleConfig(rule_name=str(rule_name), mode=mode, init=init, finalize=finalize)

    if errors:
        _fail(config_file, errors)

    return result
