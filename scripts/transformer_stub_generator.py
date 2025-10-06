import os
import re
from collections import defaultdict

# --- Configuration ---
GRAMMAR_DIR = "../extension/autocomplete/grammar/statements"
HEADER_FILE = "../extension/autocomplete/transformer/peg_transformer.hpp"
FACTORY_FILE = "../extension/autocomplete/transformer/peg_transformer_factory.cpp"
SOURCE_DIR = "../extension/autocomplete/transformer"


def extract_registrations_from_factory(code_content):
    """Extracts registered transformer rule names from the factory C++ code."""
    all_rules = set()
    if not code_content:
        return all_rules

    # Pattern for Register("RuleName", TransformRuleName)
    pattern_register = re.compile(r'Register\s*\(\s*"([A-Za-z0-9_]+)"')
    all_rules.update(pattern_register.findall(code_content))
    # Pattern for RegisterEnum<...>("RuleName", ...)
    pattern_enum = re.compile(r'RegisterEnum<.*?>\s*\(\s*"([A-Za-z0-9_]+)"')
    all_rules.update(pattern_enum.findall(code_content))
    return all_rules


def extract_definitions_from_sources(directory_path):
    """Scans all .cpp files in a directory for implemented Transform functions."""
    defined_rules = set()
    if not os.path.isdir(directory_path):
        print(f"Warning: Source directory not found at '{directory_path}'")
        return defined_rules

    # Pattern for unique_ptr<SQLStatement> PEGTransformerFactory::TransformRuleName(...)
    func_def_pattern = re.compile(r'PEGTransformerFactory::Transform([A-Za-z0-9_]+)\s*\(')

    for filename in os.listdir(directory_path):
        if filename.endswith(".cpp"):
            filepath = os.path.join(directory_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = func_def_pattern.findall(content)
                    defined_rules.update(matches)
            except Exception as e:
                print(f"Error reading source file {filepath}: {e}")
    return defined_rules


def extract_grammar_rules_from_dir(directory_path):
    """Extracts grammar rule names and content from .gram files."""
    rules_map = {}
    if not os.path.isdir(directory_path):
        print(f"Warning: Grammar directory not found at '{directory_path}'")
        return rules_map

    rule_pattern = re.compile(r'^([A-Za-z0-9_]+)\s*<-(.*?)(?=\n[A-Za-z0-9_]+\s*<-|\Z)', re.MULTILINE | re.DOTALL)
    for filename in sorted(os.listdir(directory_path)):
        if filename.endswith(".gram"):
            filepath = os.path.join(directory_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = rule_pattern.findall(content)
                    for rule_name, rule_content in matches:
                        rules_map[rule_name] = {"file": filename, "content": f"{rule_name} <- {rule_content.strip()}"}
            except Exception as e:
                print(f"Error reading file {filepath}: {e}")
    return rules_map


def format_rule_as_comment(rule_content):
    """Formats a grammar rule as a C++ comment."""
    lines = rule_content.strip().split('\n')
    comment_lines = [f"// {line.strip()}" for line in lines]
    return "\n".join(comment_lines)


if __name__ == "__main__":
    print("--- Running Transformer Stub Generator ---")

    # --- MODIFIED: Scan both the factory for registrations and source files for definitions ---
    try:
        with open(FACTORY_FILE, 'r', encoding='utf-8') as f:
            factory_content = f.read()
        registered_rules = extract_registrations_from_factory(factory_content)
    except FileNotFoundError:
        print(f"\nFATAL: Factory file not found at '{FACTORY_FILE}'. Exiting.")
        exit(1)

    defined_rules = extract_definitions_from_sources(SOURCE_DIR)

    # A rule is considered implemented if it's either registered or defined.
    implemented_rules = registered_rules.union(defined_rules)

    # --- The rest of the script continues as before ---

    all_grammar_rules_map = extract_grammar_rules_from_dir(GRAMMAR_DIR)
    all_grammar_rules = set(all_grammar_rules_map.keys())

    missing_rules = sorted(list(all_grammar_rules - implemented_rules))

    if not missing_rules:
        print(
            "\nSUCCESS: All grammar rules have a corresponding transformer registration and/or definition. Nothing to do."
        )
        exit(0)

    print(f"\n{len(implemented_rules)} rules are already implemented (found in factory or source files).")
    print(f"\nFound {len(missing_rules)} missing transformer functions. Generating stubs...\n")

    grouped_snippets = defaultdict(lambda: {'headers': [], 'factories': []})
    generated_files = set()
    newly_created_files = set()

    for rule_name in missing_rules:
        rule_info = all_grammar_rules_map.get(rule_name)
        if not rule_info:
            continue

        transform_func_name = f"Transform{rule_name}"
        source_gram_file = rule_info["file"]
        rule_content_comment = format_rule_as_comment(rule_info["content"])
        target_cpp_filename = f"transform_{source_gram_file.replace('.gram', '.cpp')}"
        target_cpp_filepath = os.path.join(SOURCE_DIR, target_cpp_filename)

        header_snippet = f"static unique_ptr<SQLStatement> {transform_func_name}(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);"
        factory_snippet = f"    REGISTER_TRANSFORM({transform_func_name});"
        source_body = (
            f'\n\n{rule_content_comment}\n'
            f'unique_ptr<SQLStatement> PEGTransformerFactory::{transform_func_name}(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {{\n'
            f'    auto &list_pr = parse_result->Cast<ListParseResult>();\n'
            f'    throw NotImplementedException("Rule \'{rule_name}\' has not been implemented yet");\n'
            f'}}'
        )

        try:
            is_new_file = not os.path.exists(target_cpp_filepath)
            with open(target_cpp_filepath, "a", encoding="utf-8") as f:
                if is_new_file:
                    f.write('#include "transformer/peg_transformer.hpp"\n\n')
                    f.write("namespace duckdb {\n")
                    newly_created_files.add(target_cpp_filepath)
                f.write(source_body)
            generated_files.add(target_cpp_filepath)
        except Exception as e:
            print(f"Error writing to source file {target_cpp_filepath}: {e}")

        grouped_snippets[source_gram_file]['headers'].append(header_snippet)
        grouped_snippets[source_gram_file]['factories'].append(factory_snippet)

    for new_file in newly_created_files:
        try:
            with open(new_file, "a", encoding="utf-8") as f:
                f.write("\n} // namespace duckdb\n")
        except Exception as e:
            print(f"Error finalizing file {new_file}: {e}")

    final_manual_stubs = ""
    for gram_file, snippets in sorted(grouped_snippets.items()):
        final_manual_stubs += f"\n// =============== Stubs for: {gram_file} ===============\n"
        if snippets['headers']:
            final_manual_stubs += f"\n// 1. Add to header file: '{HEADER_FILE}'\n"
            final_manual_stubs += "\n".join(snippets['headers']) + "\n"
        if snippets['factories']:
            final_manual_stubs += f"\n// 2. Add to factory constructor: '{FACTORY_FILE}'\n"
            final_manual_stubs += "\n".join(snippets['factories']) + "\n"

    OUTPUT_FILENAME = "transformer_stubs_manual.txt"
    try:
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            f.write(final_manual_stubs)
        print(f"\nSUCCESS: Manual stubs for header/factory written to '{OUTPUT_FILENAME}'")
        print("The following source files were created or appended to:")
        for fname in sorted(list(generated_files)):
            print(f"  - {fname}")

    except Exception as e:
        print(f"Error writing to output file: {e}")
