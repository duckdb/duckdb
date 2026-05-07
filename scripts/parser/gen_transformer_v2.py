import re
import sys
from pathlib import Path

# NOTE: inline_grammar.py has module-level side effects (writes keyword_map.cpp
# and inlined_grammar.hpp on import). This is acceptable for a generation script.
sys.path.insert(0, str(Path(__file__).parent))
from inline_grammar import parse_peg_grammar, PEGTokenType
from generate_transformer import load_grammar_types

scripts_dir = Path(__file__).parent.parent
peg_dir = scripts_dir.parent / 'src' / 'parser' / 'peg'
statements_dir = peg_dir / 'grammar' / 'statements'
type_dir = scripts_dir / 'parser'


def to_snake_case(name):
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def is_simple_rule(rule):
    """True if all tokens are LITERAL or REFERENCE (no operators, choices, groups)."""
    return all(t.type in (PEGTokenType.LITERAL, PEGTokenType.REFERENCE) for t in rule.tokens)


def get_semantic_children(rule):
    """Returns [(index, rule_name)] for every REFERENCE token in the rule."""
    return [(i, tok.text) for i, tok in enumerate(rule.tokens) if tok.type == PEGTokenType.REFERENCE]


def generate_internal_declaration(rule_name, return_type):
    return (f"\tstatic {return_type} Transform{rule_name}Internal"
            f"(PEGTransformer &transformer, ParseResult &parse_result);\n")


def generate_body_declaration(rule_name, return_type, semantic_children, rule_to_type):
    params = ", ".join(
        f"{rule_to_type[name]} {to_snake_case(name)}" for _, name in semantic_children
    )
    return f"\tstatic {return_type} Transform{rule_name}({params});\n"


def generate_internal_wrapper(rule_name, return_type, semantic_children, rule_to_type):
    """Generates the Internal .cpp function that extracts children and calls the body."""
    arg_lines = []
    arg_names = []
    for idx, child_name in semantic_children:
        var = to_snake_case(child_name)
        child_type = rule_to_type[child_name]
        arg_lines.append(f"\tauto {var} = transformer.Transform<{child_type}>(list_pr, {idx});")
        arg_names.append(var)

    body = []
    if arg_lines:
        body.append("\tauto &list_pr = parse_result.Cast<ListParseResult>();")
        body.extend(arg_lines)
    body.append(f"\treturn Transform{rule_name}({', '.join(arg_names)});")

    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        + "\n".join(body)
        + "\n}\n"
    )


def generate_registration(rule_name):
    return f'Register("{rule_name}", &PEGTransformerFactory::Transform{rule_name}Internal);\n'


def generate_output(rules, rule_to_type, gram_stem):
    declarations = []
    implementations = []
    registrations = []
    skipped = []

    for rule_name, rule in rules.items():
        return_type = rule.return_type
        if return_type is None:
            skipped.append((rule_name, "no return type in grammar_types.yml"))
            continue
        if not is_simple_rule(rule):
            skipped.append((rule_name, "complex rule (has operators/choices/groups)"))
            continue

        children = get_semantic_children(rule)
        unknown = [name for _, name in children if name not in rule_to_type]
        if unknown:
            skipped.append((rule_name, f"unknown child types: {unknown}"))
            continue

        declarations.append(generate_internal_declaration(rule_name, return_type))
        declarations.append(generate_body_declaration(rule_name, return_type, children, rule_to_type))
        implementations.append(generate_internal_wrapper(rule_name, return_type, children, rule_to_type))
        registrations.append(generate_registration(rule_name))

    if skipped:
        print("=== SKIPPED (manual implementation required) ===")
        for rule_name, reason in skipped:
            print(f"  {rule_name}: {reason}")
        print()

    print("=== DECLARATIONS (peg_transformer_generated.hpp) ===")
    print("".join(declarations))

    print(f"=== IMPLEMENTATION (generated/transform_{gram_stem}_generated.cpp) ===")
    print("".join(implementations))

    print(f"=== REGISTRATION (in Register{gram_stem.capitalize()}() in peg_transformer_factory.cpp) ===")
    print("".join(registrations))


def main():
    use_file_path = statements_dir / 'use.gram'
    rules = {}
    with open(use_file_path, 'r') as f:
        file_content = f.read()
        try:
            rules = parse_peg_grammar(file_content)
        except Exception as e:
            raise Exception(f"{use_file_path.name}: {e}") from None

    rule_to_type = load_grammar_types(type_dir / 'grammar_types.yml')
    for rule_name, return_type in rule_to_type.items():
        if rule_name in rules:
            rules[rule_name].return_type = return_type

    generate_output(rules, rule_to_type, gram_stem="use")


if __name__ == "__main__":
    main()