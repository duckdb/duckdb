import argparse
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

GRAMMAR_DIR = Path("src/parser/peg/grammar/statements")
TRANSFORMER_DIR = Path("src/parser/peg/transformer")
FACTORY_REG_FILE = Path("src/parser/peg/transformer/peg_transformer_factory.cpp")
FACTORY_HPP_FILE = Path("src/include/duckdb/parser/peg/transformer/peg_transformer.hpp")
GRAMMAR_TYPES_FILE = Path("scripts/parser/grammar_types.yml")

# Matches: RuleName <- ...
GRAMMAR_REGEX = re.compile(r"^(\w+)\s*<-")

# Matches: PEGTransformerFactory::TransformRuleName(
TRANSFORMER_REGEX = re.compile(r"PEGTransformerFactory::Transform(\w+)\s*\(")

# Matches: RegisterEnum<...>("RuleName", ...);
ENUM_RULE_REGEX = re.compile(r'RegisterEnum<[^>]+>\s*\(\s*"(\w+)"\s*,')

# Matches: REGISTER_TRANSFORM(TransformRuleName)
REGISTER_TRANSFORM_REGEX = re.compile(r"REGISTER_TRANSFORM\s*\(\s*Transform(\w+)\s*\)")

# Matches: Register("RuleName", &SomeFunction) — direct registration bypassing the macro
DIRECT_REGISTER_REGEX = re.compile(r'Register\s*\(\s*"(\w+)"\s*,')

EXCLUDED_RULES = {
    "Program",
    "FunctionType",
    "IfExists",
    "Database",
    "AbortOrRollback",
    "CommitOrEnd",
    "StartOrBegin",
    "Transaction",
    "VariableAssign",
    "MacroOrFunction",
    "SettingScope",
    "ColLabel",
    "MacroOrFunction",
    "GroupingOrGroupingId",
    "DefaultValues",
    "RowOrRows",
    "Recursive",
    "StarSymbol",
    "IfNotExists",
    "PlainIdentifier",
    "QuotedIdentifier",
    "CreateTableColumnElement",
    "OrReplace",
    "ReservedIdentifier",
    "CatalogName",
    "SchemaName",
    "ReservedSchemaName",
    "ReservedIdentifier",
    "TableName",
    "ConstraintName",
    "IntervalNumber",
    "ReservedTableName",
    "ColumnName",
    "ReservedColumnName",
    "FunctionName",
    "ReservedFunctionName",
    "TableFunctionName",
    "TypeName",
    "PragmaName",
    "SettingName",
    "CopyOptionName",
    "AtTimeZoneOperator",
    "Generated",
    "ColumnConstraint",
    "AlwaysOrByDefault",
    "Lateral",
    "ConstraintNameClause",
    "ReservedSchemaQualification",
    "UsingSample",
    "TableSample",
    "TypeList",
    "NamedParameterAssignment",
    "WithOrdinality",
    "ByName",
    "CollateOperator",
    "ExportClause",
    "ValueOrValues",
    "PivotKeyword",
    "UnpivotKeyword",
    "Unique",
    "DefArg",
    "NoneLiteral",
    "RowOrStruct",
    "ForEachRow",
    "ForEachStatement",
    "SetData",
    "CTEBodyContent",
    "SingleArrowPair",
    "OperatorLiteral"
}


def load_grammar_types(types_file):
    """
    Loads grammar_types.yml and returns a dict mapping rule name -> C++ return type.
    """
    if yaml is None:
        print("Error: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    if not types_file.is_file():
        print(f"Error: {types_file} not found.", file=sys.stderr)
        sys.exit(1)

    with types_file.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        print(f"Error: {types_file} is malformed (expected a top-level mapping).", file=sys.stderr)
        sys.exit(1)

    rule_to_type = {}
    rule_to_source = {}  # tracks where each rule was first seen for error messages
    duplicates = []

    def register(rule_name, cpp_type, source):
        rule_name = str(rule_name)
        if rule_name in rule_to_type:
            duplicates.append(
                f"  '{rule_name}' in '{source}' (already listed in '{rule_to_source[rule_name]}')"
            )
        else:
            rule_to_type[rule_name] = str(cpp_type)
            rule_to_source[rule_name] = source

    # Top-level overrides: flat RuleName -> "type" map
    overrides = data.get("overrides", {})
    if isinstance(overrides, dict):
        for rule_name, cpp_type in overrides.items():
            register(rule_name, cpp_type, "overrides")

    # Category entries: CategoryName -> {type: "...", rules: [...]}
    for key, value in data.items():
        if key == "overrides":
            continue
        if not isinstance(value, dict):
            continue
        cpp_type = value.get("type")
        rules = value.get("rules", [])
        if not cpp_type or not isinstance(rules, list):
            continue
        for rule_name in rules:
            register(rule_name, cpp_type, key)

    if duplicates:
        print(f"Error: {types_file} contains duplicate rule listings:", file=sys.stderr)
        for msg in duplicates:
            print(msg, file=sys.stderr)
        sys.exit(1)

    return rule_to_type


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


def find_transformer_rules(transformer_path):
    """
    Scans the transformer directory for *.cpp files and extracts all
    PEGTransformerFactory::Transform...() function implementations.

    Returns a set of all found rule names:
    { "AlterStatement", "AlterTableStmt", ... }
    """
    transformer_rules = set()

    if not transformer_path.is_dir():
        print(
            f"Error: Transformer directory not found: {transformer_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    cpp_files = sorted(list(transformer_path.glob("*.cpp")))
    if not cpp_files:
        print(f"Error: No *.cpp files found in {transformer_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {len(cpp_files)} transformer files in {transformer_path}...")

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


def find_factory_registrations(factory_file_path):
    """
    Scans the factory file for RegisterEnum<...>, REGISTER_TRANSFORM(...),
    and direct Register("name", &func) calls.

    Returns three sets:
    (enum_rules, registered_rules, directly_registered_rules)
    """
    enum_rules = set()
    registered_rules = set()
    directly_registered_rules = set()

    if not factory_file_path.is_file():
        print(f"Error: Factory file not found: {factory_file_path}", file=sys.stderr)
        return enum_rules, registered_rules, directly_registered_rules

    print(f"Scanning factory file: {factory_file_path}...")

    try:
        with factory_file_path.open("r", encoding="utf-8") as f:
            content = f.read()

            for match in ENUM_RULE_REGEX.finditer(content):
                enum_rules.add(match.group(1))

            for match in REGISTER_TRANSFORM_REGEX.finditer(content):
                registered_rules.add(match.group(1))

            for match in DIRECT_REGISTER_REGEX.finditer(content):
                directly_registered_rules.add(match.group(1))

    except Exception as e:
        print(f"Error reading {factory_file_path}: {e}", file=sys.stderr)

    return enum_rules, registered_rules, directly_registered_rules


def generate_declaration_stub(rule_name, cpp_type):
    """Generates the C++ method declaration (for the .hpp file)."""
    return f"\tstatic {cpp_type} Transform{rule_name}(PEGTransformer &transformer, ParseResult &parse_result);\n"


def generate_registration_stub(rule_name):
    """Generates the C++ registration line (for peg_transformer_factory.cpp)."""
    return f"REGISTER_TRANSFORM(Transform{rule_name});\n"


def generate_implementation_stub(rule_name, cpp_type):
    """Generates the C++ method implementation (for the transform_...cpp file)."""
    return f"""{cpp_type} PEGTransformerFactory::Transform{rule_name}(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {{
	throw NotImplementedException("Transform{rule_name} has not yet been implemented");
}}
"""


def generate_code_for_missing_rules(generation_queue, rule_to_type):
    """
    Iterates the generation queue and prints stub code, grouped by rule.
    Caller is responsible for ensuring all rules have types in rule_to_type.
    """
    if not generation_queue:
        print("\nNo missing rules to generate.")
        return

    rules_to_generate = []  # List of (rule_name, cpp_filename)
    for cpp_filename, rules in generation_queue.items():
        for rule in rules:
            rules_to_generate.append((rule, cpp_filename))

    print("\n--- Code Generation: Missing Stubs ---")
    print("Copy and paste the code below into the correct files.")

    for rule_name, cpp_filename in sorted(rules_to_generate):
        cpp_path = TRANSFORMER_DIR / cpp_filename
        cpp_type = rule_to_type[rule_name]

        # Constraint: Do not generate code for non-existent files
        if not cpp_path.is_file():
            print(f"\n// --- SKIPPING: {rule_name} (File not found: {cpp_filename}) ---")
            continue

        print(f"--- Generation for rule: {rule_name} ---")
        print(f"1. Add DECLARATION to: {FACTORY_HPP_FILE}")
        print(generate_declaration_stub(rule_name, cpp_type))

        print(f"2. Add REGISTRATION to: {FACTORY_REG_FILE}\nInside the appropriate Register...() function:")
        print(generate_registration_stub(rule_name))

        print(f"3. Add IMPLEMENTATION to: {cpp_path}")
        print(generate_implementation_stub(rule_name, cpp_type))
        print(f"--- End of {rule_name} ---\n")


def main():
    """
    Main script to find rules, compare them, and print a report.
    """
    parser = argparse.ArgumentParser(description="Check transformer coverage and optionally generate stubs.")
    parser.add_argument(
        "-g",
        "--generate",
        action="store_true",
        help="Generate C++ stubs (declaration, registration, implementation) for missing rules.",
    )
    parser.add_argument("-s", "--skip-found", action="store_true", help="Skip output of [ FOUND ] rules")

    args = parser.parse_args()

    rule_to_type = load_grammar_types(GRAMMAR_TYPES_FILE)
    grammar_rules_by_file = find_grammar_rules(Path(GRAMMAR_DIR))
    transformer_impls = find_transformer_rules(Path(TRANSFORMER_DIR))
    enum_rules, registered_rules, directly_registered_rules = find_factory_registrations(Path(FACTORY_REG_FILE))

    if not grammar_rules_by_file:
        print("Error: Could not find grammar rules. Exiting.", file=sys.stderr)
        sys.exit(1)

    print("\n--- Rule Coverage Check ---")

    total_grammar_rules = 0
    total_rules_scanned = 0
    total_found_enum = 0
    total_found_registered = 0
    total_missing_registration = 0
    total_missing_implementation = 0
    all_grammar_rules_flat = set()
    missing_rules_by_file = {}

    generation_queue = {}

    # Iterate through each file and its rules
    for file_name, (file_path, grammar_rules) in sorted(grammar_rules_by_file.items()):
        print(f"\n--- File: {file_name} ---")
        missing_count_this_file = 0

        stem = file_path.stem
        cpp_filename = f"transform_{stem}.cpp"
        missing_rules_for_gen = []

        if not grammar_rules:
            print("(No grammar rules found in this file)")
            continue

        for rule_name in sorted(grammar_rules):
            total_rules_scanned += 1
            if rule_name in EXCLUDED_RULES:
                print(f"{'[ EXCLUDED ]':<14} {rule_name}")
                continue

            all_grammar_rules_flat.add(rule_name)
            total_grammar_rules += 1

            is_enum = rule_name in enum_rules
            is_transformer = rule_name in transformer_impls
            is_registered = rule_name in registered_rules
            is_directly_registered = rule_name in directly_registered_rules

            if is_enum:
                status_str = "[ ENUM ]"
                total_found_enum += 1
            elif is_directly_registered:
                status_str = "[ FOUND ]"
                total_found_registered += 1
            elif is_transformer:
                if is_registered:
                    status_str = "[ FOUND ]"
                    total_found_registered += 1
                else:
                    status_str = "[ NOT REG'D ]"
                    total_missing_registration += 1
                    missing_count_this_file += 1
            else:
                status_str = "[ MISSING ]"
                total_missing_implementation += 1
                missing_count_this_file += 1
                missing_rules_for_gen.append(rule_name)

            if args.skip_found and ("FOUND" in status_str or "ENUM" in status_str):
                continue

            print(f"{status_str:<14} {rule_name}")

        if missing_count_this_file > 0:
            missing_rules_by_file[file_name] = missing_count_this_file

        if missing_rules_for_gen:
            generation_queue[cpp_filename] = missing_rules_for_gen

    total_covered = total_found_enum + total_found_registered
    total_issues = total_missing_implementation + total_missing_registration
    coverage = (total_covered / total_grammar_rules) * 100 if total_grammar_rules > 0 else 0

    print("\n--- Summary: Rule Coverage ---")
    print(f"{'TOTAL RULES SCANNED':<25} : {total_rules_scanned}")
    print(f"  {'  - Excluded':<23} : {len(EXCLUDED_RULES)}")
    print("---------------------------------------")
    print(f"{'TOTAL ACTIONABLE RULES':<25} : {total_grammar_rules}")
    print(f"{'TOTAL COVERED':<25} : {total_covered} ({coverage:.2f}%)")
    print(f"  {'  - Enum':<23} : {total_found_enum}")
    print(f"  {'  - Registered':<23} : {total_found_registered}")
    print(f"{'TOTAL ISSUES':<25} : {total_issues}")

    if missing_rules_by_file:
        print("\n--- Summary: Issues Per File ---")
        for file_name, count in sorted(missing_rules_by_file.items()):
            print(f"{file_name:<25} : {count} issues")

    print("\n--- Orphan / Mismatch Check ---")
    orphan_transformers = transformer_impls - all_grammar_rules_flat - EXCLUDED_RULES
    if orphan_transformers:
        print("\n[!] Orphan Transformer Functions (No matching grammar rule):")
        for rule in sorted(list(orphan_transformers)):
            print(f"  - Transform{rule}")

    orphan_enums = enum_rules - all_grammar_rules_flat - EXCLUDED_RULES
    if orphan_enums:
        print("\n[!] Orphan Enum Rules (No matching grammar rule):")
        for rule in sorted(list(orphan_enums)):
            print(f'  - RegisterEnum("{rule}")')

    orphan_registrations = registered_rules - all_grammar_rules_flat - EXCLUDED_RULES
    if orphan_registrations:
        print("\n[!] Orphan Registrations (No matching grammar rule):")
        for rule in sorted(list(orphan_registrations)):
            print(f"  - REGISTER_TRANSFORM(Transform{rule})")

    orphan_direct = directly_registered_rules - all_grammar_rules_flat - EXCLUDED_RULES
    if orphan_direct:
        print("\n[!] Orphan Direct Registrations (No matching grammar rule):")
        for rule in sorted(list(orphan_direct)):
            print(f"  - Register(\"{rule}\", ...)")

    missing_impl = registered_rules - transformer_impls
    if missing_impl:
        print("\n  [!] Registered but NOT Implemented (Will cause C++ error):")
        for rule in sorted(list(missing_impl)):
            print(f"  - REGISTER_TRANSFORM(Transform{rule})")

    unnecessary_reg = registered_rules.intersection(enum_rules)
    if unnecessary_reg:
        print("\n[!] Rule registered as BOTH Enum and Transformer (Ambiguous):")
        for rule in sorted(list(unnecessary_reg)):
            print(f"  - {rule}")

    if args.generate:
        all_rules_to_generate = [r for rules in generation_queue.values() for r in rules]
        missing_from_yaml = [r for r in all_rules_to_generate if r not in rule_to_type]
        if missing_from_yaml:
            print("\n--- Error: Missing Return Types in grammar_types.yml ---")
            print("Add the following rules before generating stubs:")
            for rule in sorted(missing_from_yaml):
                print(f"  {rule}")
            sys.exit(1)
        generate_code_for_missing_rules(generation_queue, rule_to_type)


if __name__ == "__main__":
    main()
