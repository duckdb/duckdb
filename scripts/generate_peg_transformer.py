#!/usr/bin/env python3

import re
from pathlib import Path
import sys
import argparse  # Import argparse

# --- Configuration ---
# Paths are relative to the script's location
SCRIPT_DIR = Path(__file__).parent
GRAMMAR_DIR = SCRIPT_DIR / "../extension/autocomplete/grammar/statements"
TRANSFORMER_DIR = SCRIPT_DIR / "../extension/autocomplete/transformer"
FACTORY_REG_FILE = SCRIPT_DIR / "../extension/autocomplete/transformer/peg_transformer_factory.cpp"

# Regex to find grammar rule names (e.g., "AlterStatement")
# Matches: RuleName <- ...
GRAMMAR_REGEX = re.compile(r"^(\w+)\s*<-")

# Regex to find transformer function implementations (e.g., "AlterStatement")
# Matches: PEGTransformerFactory::TransformRuleName(
TRANSFORMER_REGEX = re.compile(r"PEGTransformerFactory::Transform(\w+)\s*\(")

# Regex to find enum-registered rule names (e.g., "LocalScope")
# Matches: RegisterEnum<...>("RuleName", ...);
ENUM_RULE_REGEX = re.compile(r'RegisterEnum<[^>]+>\s*\(\s*"(\w+)"\s*,')

# Regex to find registered transformer functions (e.g., "AlterStatement")
# Matches: REGISTER_TRANSFORM(TransformRuleName)
REGISTER_TRANSFORM_REGEX = re.compile(r"REGISTER_TRANSFORM\s*\(\s*Transform(\w+)\s*\)")

# --- Phase 1: Find all Grammar Rules ---

def find_grammar_rules(grammar_path):
    """
    Scans the grammar directory for *.gram files and extracts all rule names.

    Returns a dictionary mapping:
    { "filename.gram": (Path, ["Rule1", "Rule2"]), ... }
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

        all_rules_by_file[file_path.name] = (file_path, rules_in_file)

    return all_rules_by_file

# --- Phase 2: Find all Transformer Implementations ---

def find_transformer_rules(transformer_path):
    """
    Scans the transformer directory for *.cpp files and extracts all
    PEGTransformerFactory::Transform...() function implementations.

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

# --- Phase 3 & 4: Find Enum Rules and Registrations ---

def find_factory_registrations(factory_file_path):
    """
    Scans the factory file for RegisterEnum<...> and REGISTER_TRANSFORM(...)

    Returns two sets:
    (enum_rules, registered_rules)
    """
    enum_rules = set()
    registered_rules = set()

    if not factory_file_path.is_file():
        print(f"Error: Factory file not found: {factory_file_path}", file=sys.stderr)
        return enum_rules, registered_rules

    print(f"🔍 Scanning factory file: {factory_file_path}...")

    try:
        with factory_file_path.open("r", encoding="utf-8") as f:
            content = f.read()

            # Find enums
            enum_matches = ENUM_RULE_REGEX.finditer(content)
            for match in enum_matches:
                enum_rules.add(match.group(1))

            # Find transformer registrations
            reg_matches = REGISTER_TRANSFORM_REGEX.finditer(content)
            for match in reg_matches:
                registered_rules.add(match.group(1))

    except Exception as e:
        print(f"Error reading {factory_file_path}: {e}", file=sys.stderr)

    return enum_rules, registered_rules

# --- NEW: Phase 5: Code Generation ---

def generate_stub_for_rule(rule_name):
    """
    Generates a C++ stub string for a given rule name.
    We use unique_ptr<SQLStatement> as a common placeholder,
    as requested in the prompt. This may need to be manually
    changed for rules that don't return a statement.
    """
    return f"""
unique_ptr<SQLStatement> PEGTransformerFactory::Transform{rule_name}(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {{
	throw NotImplementedException("Transform{rule_name} has not yet been implemented");
}}
"""

def generate_missing_stubs(generation_queue):
    """
    Iterates the generation queue and prints stub code.
    Checks that the target .cpp file exists before printing.
    """
    if not generation_queue:
        print("\n✅ No missing rules to generate.")
        return

    print("\n--- ✂️  Code Generation: Missing Stubs ---")
    print("Copy and paste the code below into the correct files.")

    for cpp_filename, rules in generation_queue.items():
        cpp_path = TRANSFORMER_DIR / cpp_filename

        # Constraint: Do not generate code for non-existent files
        if not cpp_path.is_file():
            print(f"\n// --- SKIPPING: {cpp_filename} (File not found) ---")
            for rule in rules:
                print(f"//   - (Skipped) Transform{rule}")
            continue

        print(f"\n// --- Add to: {cpp_path.relative_to(SCRIPT_DIR.parent)} ---")
        print("\nnamespace duckdb {\n")

        for rule_name in rules:
            print(generate_stub_for_rule(rule_name))

        print("\n} // namespace duckdb\n")
        print("// --- End of {cpp_filename} ---")


# --- Main Execution ---

def main():
    """
    Main script to find rules, compare them, and print a report.
    """
    # --- NEW: Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Check transformer coverage and optionally generate stubs."
    )
    parser.add_argument(
        "-g",
        "--generate",
        action="store_true",
        help="Generate C++ stubs for missing transformer rules."
    )
    args = parser.parse_args()

    # --- Run Phases 1-4 ---
    grammar_rules_by_file = find_grammar_rules(Path(GRAMMAR_DIR))
    transformer_impls = find_transformer_rules(Path(TRANSFORMER_DIR))
    enum_rules, registered_rules = find_factory_registrations(Path(FACTORY_REG_FILE))

    if not grammar_rules_by_file:
        print("Error: Could not find grammar rules. Exiting.", file=sys.stderr)
        sys.exit(1)

    print("\n--- 📋 Rule Coverage Check ---")

    total_grammar_rules = 0
    total_found_enum = 0
    total_found_registered = 0
    total_missing_registration = 0
    total_missing_implementation = 0
    all_grammar_rules_flat = set()
    missing_rules_by_file = {}

    # NEW: Dictionary to hold { "transform_file.cpp": ["Rule1", "Rule2"] }
    generation_queue = {}

    # Iterate through each file and its rules
    for file_name, (file_path, grammar_rules) in sorted(grammar_rules_by_file.items()):
        print(f"\n--- File: {file_name} ---")
        missing_count_this_file = 0

        # --- NEW: Prepare for generation ---
        # Map 'alter.gram' -> 'transform_alter.cpp'
        stem = file_path.stem
        cpp_filename = f"transform_{stem}.cpp"
        missing_rules_for_gen = []

        if not grammar_rules:
            print("  (No grammar rules found in this file)")
            continue

        for rule_name in sorted(grammar_rules):
            all_grammar_rules_flat.add(rule_name)
            total_grammar_rules += 1

            is_enum = rule_name in enum_rules
            is_transformer = rule_name in transformer_impls
            is_registered = rule_name in registered_rules

            if is_enum:
                status_str = "[ ✅ ENUM ]"
                total_found_enum += 1
            elif is_transformer:
                if is_registered:
                    status_str = "[ ✅ FOUND ]"
                    total_found_registered += 1
                else:
                    status_str = "[ ⚠️ NOT REG'D ]"
                    total_missing_registration += 1
                    missing_count_this_file += 1
            else:
                status_str = "[ ❌ MISSING ]"
                total_missing_implementation += 1
                missing_count_this_file += 1
                # NEW: Add to generation queue
                missing_rules_for_gen.append(rule_name)

            # Use f-string padding to align to 14 characters
            print(f"  {status_str:<14} {rule_name}")

        if missing_count_this_file > 0:
            missing_rules_by_file[file_name] = missing_count_this_file

        # NEW: Add this file's missing rules to the main queue
        if missing_rules_for_gen:
            generation_queue[cpp_filename] = missing_rules_for_gen

    # ... (Rest of the summary/orphan reports, no changes) ...
    total_covered = total_found_enum + total_found_registered
    total_issues = total_missing_implementation + total_missing_registration
    coverage = (total_covered / total_grammar_rules) * 100 if total_grammar_rules > 0 else 0

    # --- Print Summary Report ---
    print("\n--- 📊 Summary: Rule Coverage ---")
    print(f"  {'TOTAL GRAMMAR RULES':<25} : {total_grammar_rules}")
    print(f"  {'TOTAL COVERED':<25} : {total_covered} ({coverage:.2f}%)")
    print(f"    {'  - Enum':<23} : {total_found_enum}")
    print(f"    {'  - Registered':<23} : {total_found_registered}")
    print(f"  {'TOTAL ISSUES':<25} : {total_issues}")
    print(f"    {'  - Missing Impl':<23} : {total_missing_implementation}")
    print(f"    {'  - Not Registered':<23} : {total_missing_registration}")


    if missing_rules_by_file:
        print("\n--- 📊 Summary: Issues Per File ---")
        for file_name, count in sorted(missing_rules_by_file.items()):
            print(f"  {file_name:<25} : {count} issues")

    # --- Check for "Orphan" Rules (Implementation without Grammar) ---
    print("\n--- ⚠️  Orphan / Mismatch Check ---")

    orphan_transformers = transformer_impls - all_grammar_rules_flat
    if orphan_transformers:
        print("\n  [!] Orphan Transformer Functions (No matching grammar rule):")
        for rule in sorted(list(orphan_transformers)):
            print(f"    - Transform{rule}")

    orphan_enums = enum_rules - all_grammar_rules_flat
    if orphan_enums:
        print("\n  [!] Orphan Enum Rules (No matching grammar rule):")
        for rule in sorted(list(orphan_enums)):
            print(f"    - RegisterEnum(\"{rule}\")")

    orphan_registrations = registered_rules - all_grammar_rules_flat
    if orphan_registrations:
        print("\n  [!] Orphan Registrations (No matching grammar rule):")
        for rule in sorted(list(orphan_registrations)):
            print(f"    - REGISTER_TRANSFORM(Transform{rule})")

    # --- Check for Mismatches ---
    missing_impl = registered_rules - transformer_impls
    if missing_impl:
        print("\n  [!] Registered but NOT Implemented (Will cause C++ error):")
        for rule in sorted(list(missing_impl)):
            print(f"    - REGISTER_TRANSFORM(Transform{rule})")

    unnecessary_reg = registered_rules.intersection(enum_rules)
    if unnecessary_reg:
        print("\n  [!] Rule registered as BOTH Enum and Transformer (Ambiguous):")
        for rule in sorted(list(unnecessary_reg)):
            print(f"    - {rule}")

    print("\n✅ Done.")

    # --- NEW: Run Generation if Flag is Set ---
    if args.generate:
        generate_missing_stubs(generation_queue)


if __name__ == "__main__":
    main()