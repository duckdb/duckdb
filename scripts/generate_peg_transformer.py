import argparse
import re
import sys
from enum import Enum
from pathlib import Path

GRAMMAR_DIR = Path("extension/autocomplete/grammar/statements")
TRANSFORMER_DIR = Path("extension/autocomplete/transformer")
FACTORY_REG_FILE = Path(
    "extension/autocomplete/transformer/peg_transformer_factory.cpp"
)
FACTORY_HPP_FILE = Path(
    "extension/autocomplete/include/transformer/peg_transformer.hpp"
)

# Matches: PEGTransformerFactory::TransformRuleName(
TRANSFORMER_REGEX = re.compile(r"PEGTransformerFactory::Transform(\w+)\s*\(")

# Matches: static ReturnType TransformRuleName(PEGTransformer ...
# Used to extract declared return types from the header file.
DECLARATION_REGEX = re.compile(
    r"static\s+([\w<>:, *]+?)\s+Transform(\w+)\s*\(PEGTransformer"
)

# Matches: RegisterEnum<...>("RuleName", ...);
ENUM_RULE_REGEX = re.compile(r'RegisterEnum<[^>]+>\s*\(\s*"(\w+)"\s*,')

# Matches: REGISTER_TRANSFORM(TransformRuleName)
REGISTER_TRANSFORM_REGEX = re.compile(r"REGISTER_TRANSFORM\s*\(\s*Transform(\w+)\s*\)")

# Matches: Register("RuleName", &TransformFunctionName)
# This catches direct registrations where the function name differs from the rule name
DIRECT_REGISTER_REGEX = re.compile(r'Register\s*\(\s*"(\w+)"\s*,\s*&Transform(\w+)\s*\)')

EXCLUDED_RULES = {
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
}


class RuleStatus(Enum):
    EXCLUDED = "[ EXCLUDED ]"
    ENUM = "[ ENUM ]"
    FOUND = "[ FOUND ]"
    NOT_REGISTERED = "[ NOT REG'D ]"
    TYPE_MISMATCH = "[ TYPE MISMATCH ]"
    MISSING = "[ MISSING ]"

    @property
    def is_issue(self):
        return self in {RuleStatus.NOT_REGISTERED, RuleStatus.TYPE_MISMATCH, RuleStatus.MISSING}

    @property
    def is_covered(self):
        return self in {RuleStatus.ENUM, RuleStatus.FOUND}


def find_grammar_rules(grammar_path):
    """
    Scans the grammar directory for *.gram files and extracts all rule names
    along with their full definition text (including multi-line rules).

    Returns a dictionary mapping:
    { "filename.gram": (Path, [(rule_name, rule_text), ...]) }

    Assumes well-formed grammar (validated upstream by inline_grammar.py).
    '<-' appears only as the rule assignment operator, never in rule bodies,
    so it is used as a reliable structural delimiter.
    """
    all_rules_by_file = {}

    if not grammar_path.is_dir():
        print(f"Error: Grammar directory not found: {grammar_path}", file=sys.stderr)
        sys.exit(1)

    gram_files = sorted(grammar_path.glob("*.gram"))
    if not gram_files:
        print(f"Error: No *.gram files found in {grammar_path}", file=sys.stderr)
        sys.exit(1)

    rule_start = re.compile(r"^(\w+)(?:\[([^\]]*)\])?\s*<-", re.MULTILINE)

    for file_path in gram_files:
        content = file_path.read_text(encoding="utf-8")
        matches = list(rule_start.finditer(content))
        rules_in_file = []
        for i, match in enumerate(matches):
            rule_name = match.group(1)
            return_type = match.group(2)  # None if no annotation
            end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
            rule_text = " ".join(content[match.start():end].split())
            rules_in_file.append((rule_name, rule_text, return_type))
        all_rules_by_file[file_path.name] = (file_path, rules_in_file)

    return all_rules_by_file


def find_transformer_rules(transformer_path):
    """
    Scans the transformer directory for *.cpp files and extracts all
    PEGTransformerFactory::Transform...() function implementations.

    Returns:
    - transformer_rules: set of all found rule names
    - transformer_rule_files: dict mapping rule_name -> filename
    """
    transformer_rules = set()
    transformer_rule_files = {}

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
                    rule_name = match.group(1)
                    transformer_rules.add(rule_name)
                    transformer_rule_files[rule_name] = file_path.name
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
            continue

    return transformer_rules, transformer_rule_files


def find_transformer_return_types(hpp_path):
    """
    Scans the transformer header for static TransformXxx declarations and returns
    a dict mapping rule_name -> return_type (whitespace-normalised).
    """
    if not hpp_path.is_file():
        print(f"Error: Header file not found: {hpp_path}", file=sys.stderr)
        return {}

    content = hpp_path.read_text(encoding="utf-8")
    result = {}
    for match in DECLARATION_REGEX.finditer(content):
        return_type = match.group(1)
        rule_name = match.group(2)
        # Normalise whitespace so "pair<string, T>" == "pair<string,T>"
        result[rule_name] = re.sub(r"\s+", "", return_type)
    return result


def find_factory_registrations(factory_file_path):
    """
    Scans the factory file for RegisterEnum<...>, REGISTER_TRANSFORM(...),
    and direct Register("RuleName", &TransformFunctionName) calls.

    Returns four sets:
    (enum_rules, registered_rules, direct_registered_functions, direct_registered_rules)

    - enum_rules: rule names registered via RegisterEnum
    - registered_rules: rule names registered via REGISTER_TRANSFORM or direct Register()
    - direct_registered_functions: function names used in direct Register() calls
      (these map multiple rules to a single generic transformer)
    - direct_registered_rules: rule names registered via direct Register() calls
    """
    enum_rules = set()
    registered_rules = set()
    direct_registered_functions = set()
    direct_registered_rules = set()

    if not factory_file_path.is_file():
        print(f"Error: Factory file not found: {factory_file_path}", file=sys.stderr)
        return enum_rules, registered_rules, direct_registered_functions, direct_registered_rules

    print(f"Scanning factory file: {factory_file_path}...")

    try:
        with factory_file_path.open("r", encoding="utf-8") as f:
            content = f.read()

            # Find enums
            enum_matches = ENUM_RULE_REGEX.finditer(content)
            for match in enum_matches:
                enum_rules.add(match.group(1))

            # Find transformer registrations via macro
            reg_matches = REGISTER_TRANSFORM_REGEX.finditer(content)
            for match in reg_matches:
                registered_rules.add(match.group(1))

            # Find direct Register("RuleName", &TransformFunctionName) calls
            direct_matches = DIRECT_REGISTER_REGEX.finditer(content)
            for match in direct_matches:
                rule_name = match.group(1)
                func_name = match.group(2)
                registered_rules.add(rule_name)
                direct_registered_functions.add(func_name)
                direct_registered_rules.add(rule_name)

    except Exception as e:
        print(f"Error reading {factory_file_path}: {e}", file=sys.stderr)

    return enum_rules, registered_rules, direct_registered_functions, direct_registered_rules


def generate_declaration_stub(rule_name, rule_text="", return_type=None):
    """Generates the C++ method declaration (for the .hpp file)."""
    comment = f"// {rule_text}\n" if rule_text else ""
    if return_type:
        type_str = return_type
        todo = ""
    else:
        type_str = "unique_ptr<SQLStatement>"
        todo = "// TODO: Verify this return type is correct\n"
    return (
        f"{comment}"
        f"{todo}"
        f"static {type_str} Transform{rule_name}(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);\n"
    )


def generate_registration_stub(rule_name):
    """Generates the C++ registration line (for peg_transformer_factory.cpp)."""
    return f"REGISTER_TRANSFORM(Transform{rule_name});\n"


def generate_implementation_stub(rule_name, rule_text="", return_type=None):
    """Generates the C++ method implementation (for the transform_...cpp file)."""
    comment = f"// {rule_text}\n" if rule_text else ""
    if return_type:
        type_str = return_type
        todo = ""
    else:
        type_str = "unique_ptr<SQLStatement>"
        todo = "// TODO: Verify this return type is correct\n"
    return (
        f"{comment}"
        f"{todo}"
        f"{type_str} PEGTransformerFactory::Transform{rule_name}(PEGTransformer &transformer,\n"
        f"                                                                   optional_ptr<ParseResult> parse_result) {{\n"
        f"\tthrow NotImplementedException(\"Transform{rule_name} has not yet been implemented\");\n"
        f"}}\n"
    )


def generate_code_for_missing_rules(generation_queue, rule_definitions=None):
    """
    Iterates the generation queue and prints stub code, grouped by rule.
    """
    if rule_definitions is None:
        rule_definitions = {}

    if not generation_queue:
        print("\nNo missing rules to generate.")
        return

    print("\n--- Code Generation: Missing Stubs ---")
    print("Copy and paste the code below into the correct files.")

    rules_to_generate = []  # List of (rule_name, cpp_filename)
    for cpp_filename, rules in generation_queue.items():
        for rule in rules:
            rules_to_generate.append((rule, cpp_filename))

    # Sort by rule name
    for rule_name, cpp_filename in sorted(rules_to_generate):
        cpp_path = TRANSFORMER_DIR / cpp_filename

        # Constraint: Do not generate code for non-existent files
        if not cpp_path.is_file():
            print(
                f"\n// --- SKIPPING: {rule_name} (File not found: {cpp_filename}) ---"
            )
            continue

        rule_text, return_type = rule_definitions.get(rule_name, ("", None))

        print(f"--- Generation for rule: {rule_name} ---")
        print(f"1. Add DECLARATION to: {FACTORY_HPP_FILE}")
        print(generate_declaration_stub(rule_name, rule_text, return_type))

        print(
            f"2. Add REGISTRATION to: {FACTORY_REG_FILE}\nInside the appropriate Register...() function:"
        )
        print(generate_registration_stub(rule_name))

        print(f"3. Add IMPLEMENTATION to: {cpp_path}")
        print(generate_implementation_stub(rule_name, rule_text, return_type))
        print(f"--- End of {rule_name} ---\n")


def main():
    """
    Main script to find rules, compare them, and print a report.
    """
    parser = argparse.ArgumentParser(
        description="Check transformer coverage and optionally generate stubs."
    )
    parser.add_argument(
        "-g",
        "--generate",
        action="store_true",
        help="Generate C++ stubs (declaration, registration, implementation) for missing rules.",
    )
    parser.add_argument(
        "-s", "--skip-found", action="store_true", help="Skip output of [ FOUND ] rules"
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only print summary and issues (suppress FOUND, ENUM, EXCLUDED, and file headers).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 when there are MISSING rules (useful for CI).",
    )

    args = parser.parse_args()

    grammar_rules_by_file = find_grammar_rules(Path(GRAMMAR_DIR))
    transformer_impls, transformer_rule_files = find_transformer_rules(
        Path(TRANSFORMER_DIR)
    )
    transformer_return_types = find_transformer_return_types(Path(FACTORY_HPP_FILE))
    enum_rules, registered_rules, direct_registered_functions, direct_registered_rules = find_factory_registrations(Path(FACTORY_REG_FILE))

    if not grammar_rules_by_file:
        print("Error: Could not find grammar rules. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Build a flat dict of rule_name -> (rule_text, return_type) for stub generation
    rule_definitions = {}
    for file_name, (file_path, rules_with_text) in grammar_rules_by_file.items():
        for rule_name, rule_text, return_type in rules_with_text:
            rule_definitions[rule_name] = (rule_text, return_type)

    print("\n--- Rule Coverage Check ---")

    total_grammar_rules = 0
    total_rules_scanned = 0
    total_found_enum = 0
    total_found_registered = 0
    total_missing_registration = 0
    total_missing_implementation = 0
    total_type_mismatches = 0
    all_grammar_rules_flat = set()
    missing_rules_by_file = {}

    generation_queue = {}

    # Iterate through each file and its rules
    for file_name, (file_path, grammar_rules) in sorted(grammar_rules_by_file.items()):
        if not args.quiet:
            print(f"\n--- File: {file_name} ---")
        missing_count_this_file = 0

        stem = file_path.stem
        cpp_filename = f"transform_{stem}.cpp"
        missing_rules_for_gen = []

        if not grammar_rules:
            if not args.quiet:
                print("(No grammar rules found in this file)")
            continue

        for rule_name, _rule_text, grammar_return_type in sorted(grammar_rules, key=lambda x: x[0]):
            total_rules_scanned += 1
            if rule_name in EXCLUDED_RULES:
                if not args.quiet and not args.skip_found:
                    print(f"{RuleStatus.EXCLUDED.value:<14} {rule_name}")
                continue

            all_grammar_rules_flat.add(rule_name)
            total_grammar_rules += 1

            is_enum = rule_name in enum_rules
            is_transformer = rule_name in transformer_impls
            is_registered = rule_name in registered_rules

            if is_enum:
                status = RuleStatus.ENUM
                total_found_enum += 1
            elif is_transformer:
                if is_registered:
                    if grammar_return_type is not None:
                        declared = transformer_return_types.get(rule_name)
                        normalised_grammar = re.sub(r"\s+", "", grammar_return_type)
                        if declared is not None and declared != normalised_grammar:
                            status = RuleStatus.TYPE_MISMATCH
                            total_type_mismatches += 1
                            missing_count_this_file += 1
                        else:
                            status = RuleStatus.FOUND
                            total_found_registered += 1
                    else:
                        status = RuleStatus.FOUND
                        total_found_registered += 1
                else:
                    status = RuleStatus.NOT_REGISTERED
                    total_missing_registration += 1
                    missing_count_this_file += 1
            else:
                status = RuleStatus.MISSING
                total_missing_implementation += 1
                missing_count_this_file += 1
                missing_rules_for_gen.append(rule_name)

            if args.quiet:
                if status.is_issue:
                    detail = ""
                    if status == RuleStatus.TYPE_MISMATCH:
                        declared = transformer_return_types.get(rule_name, "?")
                        detail = f"  (grammar: {grammar_return_type}, declared: {declared})"
                    print(f"{status.value:<14} {rule_name}{detail}")
                continue

            if args.skip_found and status.is_covered:
                continue

            detail = ""
            if status == RuleStatus.TYPE_MISMATCH:
                declared = transformer_return_types.get(rule_name, "?")
                detail = f"  (grammar: {grammar_return_type}, declared: {declared})"
            print(f"{status.value:<14} {rule_name}{detail}")

        if missing_count_this_file > 0:
            missing_rules_by_file[file_name] = missing_count_this_file

        if missing_rules_for_gen:
            generation_queue[cpp_filename] = missing_rules_for_gen

    total_covered = total_found_enum + total_found_registered
    total_issues = total_missing_implementation + total_missing_registration + total_type_mismatches
    coverage = (
        (total_covered / total_grammar_rules) * 100 if total_grammar_rules > 0 else 0
    )

    print("\n--- Summary: Rule Coverage ---")
    print(f"{'TOTAL RULES SCANNED':<25} : {total_rules_scanned}")
    print(f"  {'  - Excluded':<23} : {len(EXCLUDED_RULES)}")
    print("---------------------------------------")
    print(f"{'TOTAL ACTIONABLE RULES':<25} : {total_grammar_rules}")
    print(f"{'TOTAL COVERED':<25} : {total_covered} ({coverage:.2f}%)")
    print(f"  {'  - Enum':<23} : {total_found_enum}")
    print(f"  {'  - Registered':<23} : {total_found_registered}")
    print(f"{'TOTAL ISSUES':<25} : {total_issues}")
    print(f"  {'  - Missing impl':<23} : {total_missing_implementation}")
    print(f"  {'  - Not registered':<23} : {total_missing_registration}")
    print(f"  {'  - Type mismatches':<23} : {total_type_mismatches}")

    if missing_rules_by_file:
        print("\n--- Summary: Issues Per File ---")
        for file_name, count in sorted(missing_rules_by_file.items()):
            print(f"{file_name:<25} : {count} issues")

    print("\n--- Orphan / Mismatch Check ---")
    orphan_transformers = transformer_impls - all_grammar_rules_flat - EXCLUDED_RULES - direct_registered_functions
    if orphan_transformers:
        print("\n[!] Orphan Transformer Functions (No matching grammar rule):")
        for rule in sorted(list(orphan_transformers)):
            cpp_file = transformer_rule_files.get(rule, "unknown")
            print(f"  - Transform{rule}  ({cpp_file})")

    orphan_enums = enum_rules - all_grammar_rules_flat - EXCLUDED_RULES
    if orphan_enums:
        print("\n[!] Orphan Enum Rules (No matching grammar rule):")
        for rule in sorted(list(orphan_enums)):
            print(f'  - RegisterEnum("{rule}")')

    orphan_registrations = registered_rules - all_grammar_rules_flat - EXCLUDED_RULES - direct_registered_rules
    if orphan_registrations:
        print("\n[!] Orphan Registrations (No matching grammar rule):")
        for rule in sorted(list(orphan_registrations)):
            print(f"  - REGISTER_TRANSFORM(Transform{rule})")

    # Direct Register() rules map to a differently-named function
    # (e.g. Register("PragmaName", &TransformIdentifierOrKeyword))
    # so they won't have a TransformPragmaName implementation — that's expected.
    missing_impl = registered_rules - transformer_impls - direct_registered_rules
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
        generate_code_for_missing_rules(generation_queue, rule_definitions)

    if args.strict and total_issues > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
