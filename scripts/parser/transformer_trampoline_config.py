import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Mapping, Optional

from grammar_types import load_grammar_types_yaml


class TrampolineRuleMode(str, Enum):
    MANUAL = "manual"
    MANUAL_FINALIZE = "manual_finalize"
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
        elif mode == TrampolineRuleMode.EXCLUDED:
            if init or finalize:
                errors.append(f"excluded rule '{rule_name}' must not declare hooks")

        result[rule_name] = TrampolineRuleConfig(rule_name=str(rule_name), mode=mode, init=init, finalize=finalize)

    if errors:
        _fail(config_file, errors)

    return result
