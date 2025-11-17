#!/usr/bin/env python3

import re
from pathlib import Path
import sys

# --- Configuration ---
GRAMMAR_DIR = "../extension/autocomplete/grammar/statements"
TRANSFORMER_DIR = "../extension/autocomplete/transformer"
ENUM_RULES_FILE = "../extension/autocomplete/transformer/peg_transformer_factory.cpp"

# Regex to find grammar rule names (e.g., "AlterStatement")
# Matches: RuleName <- ...
GRAMMAR_REGEX = re.compile(r"^(\w+)\s*<-")

# Regex to find transformer rule names (e.g., "AlterStatement")
# Matches: PEGTransformerFactory::TransformRuleName(
TRANSFORMER_REGEX = re.compile(r"PEGTransformerFactory::Transform(\w+)\s*\(")

# Regex to find enum-registered rule names (e.g., "LocalScope")
# Matches: RegisterEnum<...>("RuleName", ...);
ENUM_RULE_REGEX = re.compile(r'RegisterEnum<[^>]+>\s*\(\s*"(\w+)"\s*,')

# --- Phase 1: Find all Grammar Rules ---

def find_grammar_rules(grammar_path):
    """
    Scans the grammar directory for *.gram files and extracts all rule names.

    Returns a dictionary mapping:
    { "filename.gram": ["Rule1", "Rule2"], ... }
    """
    all_rules_by_file = {}

    if not grammar_path.is_dir():
        print(f"Error: Grammar directory not found: {grammar_path}", file=sys.stderr)
        sys.exit(1)

    gram_files = sorted(list(grammar_path.glob("*.gram")))
    if not gram_files:
        print(f"Error: No *.gram files found in {grammar_path}", file=sys.stderr)
        sys.exit(1)

    print(f"🔍 Scanning {len(gram_files)} grammar files in {grammar_path}...")

    for file_path in gram_files:
        rules_in_file = []
        try:
            with file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    match = GRAMMAR_REGEX.match(line)
                    if match:
                        rules_in_file.append(match.group(1))
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
            continue

        all_rules_by_file[file_path.name] = rules_in_file

    return all_rules_by_file

# --- Phase 2: Find all Transformer Rules ---

def find_transformer_rules(transformer_path):
    """
    Scans the transformer directory for *.cpp files and extracts all rule names
    from PEGTransformerFactory::Transform...() functions.

    Returns a set of all found rule names:
    { "AlterStatement", "AlterTableStmt", ... }
    """
    transformer_rules = set()

    if not transformer_path.is_dir():
        print(f"Error: Transformer directory not found: {transformer_path}", file=sys.stderr)
        sys.exit(1)

    cpp_files = sorted(list(transformer_path.glob("*.cpp")))
    if not cpp_files:
        print(f"Error: No *.cpp files found in {transformer_path}", file=sys.stderr)
        sys.exit(1)

    print(f"🔍 Scanning {len(cpp_files)} transformer files in {transformer_path}...")

    for file_path in cpp_files:
        try:
            with file_path.open("r", encoding="utf-8") as f:
                content = f.read()
                matches = TRANSFORMER_REGEX.finditer(content)
                for match in matches:
                    transformer_rules.add(match.group(1))
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
            continue

    return transformer_rules

# --- Phase 3: Find all Enum-Registered Rules ---

def find_enum_rules(enum_file_path):
    """
    Scans the specified file for RegisterEnum<...>("RuleName", ...) calls.

    Returns a set of all found rule names:
    { "LocalScope", "GlobalScope", ... }
    """
    enum_rules = set()

    if not enum_file_path.is_file():
        print(f"Error: Enum rules file not found: {enum_file_path}", file=sys.stderr)
        return enum_rules # Return empty set, but proceed

    print(f"🔍 Scanning enum rules file: {enum_file_path}...")

    try:
        with enum_file_path.open("r", encoding="utf-8") as f:
            content = f.read()
            matches = ENUM_RULE_REGEX.finditer(content)
            for match in matches:
                # group(1) is the captured rule name (e.g., "LocalScope")
                enum_rules.add(match.group(1))
    except Exception as e:
        print(f"Error reading {enum_file_path}: {e}", file=sys.stderr)

    return enum_rules

# --- Main Execution ---

def main():
    """
    Main script to find rules, compare them, and print a report.
    """
    grammar_rules_by_file = find_grammar_rules(Path(GRAMMAR_DIR))
    transformer_rules = find_transformer_rules(Path(TRANSFORMER_DIR))
    enum_rules = find_enum_rules(Path(ENUM_RULES_FILE))

    if not grammar_rules_by_file:
        print("Error: Could not find grammar rules. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Combine both sets of "covered" rules
    covered_rules = transformer_rules.union(enum_rules)

    print("\n--- 📋 Rule Coverage Check ---")

    missing_rules_by_file = {}
    total_grammar_rules = 0
    total_found = 0
    all_grammar_rules_flat = set()

    # Iterate through each file and its rules
    for file_name, grammar_rules in sorted(grammar_rules_by_file.items()):
        print(f"\n--- File: {file_name} ---")
        missing_in_this_file = 0

        if not grammar_rules:
            print("  (No grammar rules found in this file)")
            continue

        for rule_name in sorted(grammar_rules):
            all_grammar_rules_flat.add(rule_name)
            total_grammar_rules += 1

            # Check if the grammar rule exists in the combined set of covered rules
            if rule_name in covered_rules:
                print(f"  [ ✅ FOUND ]   {rule_name}")
                total_found += 1
            else:
                print(f"  [ ❌ MISSING ] {rule_name}")
                missing_in_this_file += 1

        missing_rules_by_file[file_name] = missing_in_this_file

    total_missing = total_grammar_rules - total_found

    # --- Print Summary Report ---
    print("\n--- 📊 Summary: Missing Rules Per File ---")
    for file_name, count in sorted(missing_rules_by_file.items()):
        if count > 0:
            print(f"  {file_name:<25} : {count} missing")

    print("-----------------------------------------")
    print(f"  {'TOTAL GRAMMAR RULES':<25} : {total_grammar_rules}")
    print(f"  {'TOTAL FOUND':<25} : {total_found}")
    print(f"  {'TOTAL MISSING':<25} : {total_missing}")

    coverage = (total_found / total_grammar_rules) * 100 if total_grammar_rules > 0 else 0
    print(f"  {'COVERAGE':<25} : {coverage:.2f}%")

    # --- Check for "Orphan" Rules ---
    orphan_transformers = transformer_rules - all_grammar_rules_flat
    if orphan_transformers:
        print("\n--- ⚠️  Orphan Transformer Functions (No matching grammar rule) ---")
        for orphan_rule in sorted(list(orphan_transformers)):
            print(f"  [ ❓ ORPHAN ]  Transform{orphan_rule}")

    orphan_enums = enum_rules - all_grammar_rules_flat
    if orphan_enums:
        print("\n--- ⚠️  Orphan Enum Rules (No matching grammar rule) ---")
        for orphan_rule in sorted(list(orphan_enums)):
            print(f"  [ ❓ ORPHAN ]  RegisterEnum(\"{orphan_rule}\")")

    print("\n✅ Done.")

if __name__ == "__main__":
    main()