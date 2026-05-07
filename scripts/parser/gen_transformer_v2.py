import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent))
from inline_grammar import parse_peg_grammar, PEGTokenType
from generate_transformer import load_grammar_types


# ---------------------------------------------------------------------------
# Grammar AST - mirrors the Matcher class hierarchy in matcher.cpp
# ---------------------------------------------------------------------------

class GrammarNode:
    pass


@dataclass
class LiteralNode(GrammarNode):
    """Keyword literal ('keyword'). Corresponds to KeywordMatcher."""
    text: str


@dataclass
class ReferenceNode(GrammarNode):
    """Reference to a named grammar rule. Resolved to another Matcher at build time."""
    name: str


@dataclass
class RegexNode(GrammarNode):
    """Regex or character-class match ([...] or <...>).
    In practice, rules that contain raw regex patterns are overridden in the
    matcher via AddRuleOverride() and therefore never exercise this path at
    runtime.  We keep the node so the AST parser stays complete."""
    pattern: str


@dataclass
class FunctionCallNode(GrammarNode):
    """Macro call like Parens(inner) or List(inner)."""
    func_name: str
    inner: GrammarNode


@dataclass
class SequenceNode(GrammarNode):
    """Ordered sequence of matchers. Corresponds to ListMatcher."""
    children: List[GrammarNode]


@dataclass
class ChoiceNode(GrammarNode):
    """Ordered choice A / B / C. Corresponds to ChoiceMatcher."""
    alternatives: List[GrammarNode]


@dataclass
class OptionalNode(GrammarNode):
    """Optional match A?. Corresponds to OptionalMatcher."""
    child: GrammarNode


@dataclass
class RepeatNode(GrammarNode):
    """Repeat match A+ (min=1) or A* (min=0). Corresponds to RepeatMatcher."""
    child: GrammarNode
    min_count: int


@dataclass
class NegationNode(GrammarNode):
    """Negative lookahead !A."""
    child: GrammarNode


def tokens_to_ast(tokens):
    """
    Parse a flat PEGToken list into a GrammarNode AST tree.

    Grammar of PEG rule bodies (simplified):
      choice   = sequence ('/' sequence)*
      sequence = term+
      term     = atom ('?' | '*' | '+')?
      atom     = LITERAL | REFERENCE | REGEX
                 | FUNCTION_CALL choice ')'
                 | '(' choice ')'
                 | '!' atom
    """
    pos = [0]

    def peek():
        return tokens[pos[0]] if pos[0] < len(tokens) else None

    def consume():
        tok = tokens[pos[0]]
        pos[0] += 1
        return tok

    def parse_choice():
        alts = [parse_sequence()]
        while peek() and peek().type == PEGTokenType.OPERATOR and peek().text == '/':
            consume()  # consume '/'
            alts.append(parse_sequence())
        return ChoiceNode(alts) if len(alts) > 1 else alts[0]

    def parse_sequence():
        children = []
        while True:
            t = peek()
            if t is None:
                break
            if t.type == PEGTokenType.OPERATOR and t.text in ('/', ')'):
                break
            children.append(parse_term())
        if not children:
            return SequenceNode([])
        return SequenceNode(children) if len(children) > 1 else children[0]

    def parse_term():
        node = parse_atom()
        t = peek()
        if t and t.type == PEGTokenType.OPERATOR and t.text in ('?', '*', '+'):
            op = consume().text
            if op == '?':
                return OptionalNode(node)
            elif op == '*':
                return RepeatNode(node, 0)
            else:
                return RepeatNode(node, 1)
        return node

    def parse_atom():
        t = peek()
        if t is None:
            raise Exception("Unexpected end of tokens in grammar AST parse")
        if t.type == PEGTokenType.LITERAL:
            return LiteralNode(consume().text)
        elif t.type == PEGTokenType.REFERENCE:
            return ReferenceNode(consume().text)
        elif t.type == PEGTokenType.REGEX:
            return RegexNode(consume().text)
        elif t.type == PEGTokenType.FUNCTION_CALL:
            # inline_grammar already consumed the '(' and bumped bracket_count
            func_name = consume().text
            inner = parse_choice()
            if peek() and peek().type == PEGTokenType.OPERATOR and peek().text == ')':
                consume()
            return FunctionCallNode(func_name, inner)
        elif t.type == PEGTokenType.OPERATOR and t.text == '(':
            consume()
            inner = parse_choice()
            if peek() and peek().type == PEGTokenType.OPERATOR and peek().text == ')':
                consume()
            return inner  # anonymous group - transparent node
        elif t.type == PEGTokenType.OPERATOR and t.text == '!':
            consume()
            return NegationNode(parse_atom())
        else:
            raise Exception(f"Unexpected token in grammar AST parse: {t}")

    result = parse_choice()
    if pos[0] < len(tokens):
        raise Exception(f"Tokens remaining after grammar AST parse: {tokens[pos[0]:]}")
    return result


def rule_to_ast(rule):
    """Convert a PEGGrammarRule (flat token list) to a GrammarNode AST."""
    return tokens_to_ast(rule.tokens)


# ---------------------------------------------------------------------------
# Rule overrides - mirrors AddRuleOverride() calls in matcher.cpp.
# These rules are replaced with special matchers that produce IdentifierParseResult
# (or similar) directly, bypassing the generic ListParseResult path.
# When one of these appears as an alternative in a choice rule the generated
# Internal must check ParseResultType rather than calling transformer.Transform<>.
# ---------------------------------------------------------------------------

IDENTIFIER_OVERRIDE_RULES = {
    'Identifier', 'ReservedIdentifier',
    'CatalogName', 'SchemaName', 'ReservedSchemaName',
    'TableName', 'ReservedTableName',
    'ColumnName', 'ReservedColumnName',
    'IndexName', 'SequenceName',
    'FunctionName', 'ReservedFunctionName', 'TableFunctionName',
    'TypeName', 'PragmaName', 'SettingName', 'CopyOptionName',
}

# Rules overridden with non-identifier special matchers (kept separate so
# callers can distinguish the parse-result type if needed in the future).
NUMBER_LITERAL_OVERRIDE_RULES = {'NumberLiteral'}
STRING_LITERAL_OVERRIDE_RULES = {'StringLiteral'}
OPERATOR_LITERAL_OVERRIDE_RULES = {'OperatorLiteral'}

# Union of all override rules for quick membership tests.
ALL_OVERRIDE_RULES = (
    IDENTIFIER_OVERRIDE_RULES
    | NUMBER_LITERAL_OVERRIDE_RULES
    | STRING_LITERAL_OVERRIDE_RULES
    | OPERATOR_LITERAL_OVERRIDE_RULES
)


scripts_dir = Path(__file__).parent.parent
src_dir = scripts_dir.parent / 'src'
peg_dir = src_dir / 'parser' / 'peg'
statements_dir = peg_dir / 'grammar' / 'statements'
type_dir = scripts_dir / 'parser'
transformer_dir = peg_dir / 'transformer'
generated_dir = transformer_dir / 'generated'
include_peg_dir = src_dir / 'include' / 'duckdb' / 'parser' / 'peg' / 'transformer'

GENERATED_HEADER = "// AUTO-GENERATED by scripts/parser/gen_transformer_v2.py -- DO NOT EDIT\n"


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


# ---------------------------------------------------------------------------
# Choice-rule helpers
# ---------------------------------------------------------------------------

def is_pure_reference_choice(ast):
    """True if ast is a ChoiceNode whose every alternative is a ReferenceNode."""
    return isinstance(ast, ChoiceNode) and all(isinstance(a, ReferenceNode) for a in ast.alternatives)


def classify_choice_alternatives(alternatives, rule_to_type):
    """
    Split choice alternatives into two groups:
      - transformer_alts: names with a registered transformer (in rule_to_type)
      - identifier_alts:  names that are identifier overrides (produce IdentifierParseResult)
    Returns (transformer_alts, identifier_alts, unknown_alts).
    unknown_alts are neither registered nor known overrides - these need manual handling.
    """
    transformer_alts = []
    identifier_alts = []
    unknown_alts = []
    for ref in alternatives:
        name = ref.name
        if name in rule_to_type:
            transformer_alts.append(name)
        elif name in IDENTIFIER_OVERRIDE_RULES:
            identifier_alts.append(name)
        else:
            unknown_alts.append(name)
    return transformer_alts, identifier_alts, unknown_alts


def generate_choice_internal_full(rule_name, return_type):
    """
    Fully auto-generated Internal for a pure-transformer choice rule.
    All alternatives have registered transformers so we can delegate directly.
    """
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\treturn transformer.Transform<{return_type}>(choice_pr.GetResult());\n"
        f"}}\n"
    )


def generate_choice_internal_with_body(rule_name, return_type):
    """
    Internal for a choice rule that has identifier-override alternatives.
    Extracts the ChoiceParseResult then delegates to a hand-written body.
    """
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\treturn Transform{rule_name}(transformer, choice_pr.GetResult());\n"
        f"}}\n"
    )


def generate_choice_body_declaration(rule_name, return_type):
    """Declaration for the manual body that handles identifier alternatives."""
    return (
        f"\tstatic {return_type} Transform{rule_name}"
        f"(PEGTransformer &transformer, ParseResult &choice_result);\n"
    )


def collect_generated(rules, rule_to_type):
    """Classify all rules; return lists of generated content and skipped rules."""
    declarations = []
    implementations = []
    registrations = []
    skipped = []

    for rule_name, rule in rules.items():
        return_type = rule.return_type
        if return_type is None:
            skipped.append((rule_name, "no return type in grammar_types.yml"))
            continue

        if is_simple_rule(rule):
            children = get_semantic_children(rule)
            unknown = [name for _, name in children if name not in rule_to_type]
            if unknown:
                skipped.append((rule_name, f"unknown child types: {unknown}"))
                continue
            declarations.append(generate_internal_declaration(rule_name, return_type))
            declarations.append(generate_body_declaration(rule_name, return_type, children, rule_to_type))
            implementations.append(generate_internal_wrapper(rule_name, return_type, children, rule_to_type))
            registrations.append(generate_registration(rule_name))
            continue

        try:
            ast = rule_to_ast(rule)
        except Exception as e:
            skipped.append((rule_name, f"AST parse error: {e}"))
            continue

        if is_pure_reference_choice(ast):
            transformer_alts, identifier_alts, unknown_alts = classify_choice_alternatives(
                ast.alternatives, rule_to_type
            )
            if unknown_alts:
                skipped.append((rule_name, f"choice has unknown alternatives: {unknown_alts}"))
                continue

            declarations.append(generate_internal_declaration(rule_name, return_type))
            registrations.append(generate_registration(rule_name))

            if not identifier_alts:
                # All alternatives have registered transformers - fully auto-generate.
                implementations.append(generate_choice_internal_full(rule_name, return_type))
            else:
                # Some alternatives are identifier overrides - need a manual body.
                declarations.append(generate_choice_body_declaration(rule_name, return_type))
                implementations.append(generate_choice_internal_with_body(rule_name, return_type))
                skipped.append((
                    f"{rule_name} (choice body)",
                    f"manual body needed; identifier alternatives: {identifier_alts}",
                ))
            continue

        skipped.append((rule_name, "complex rule (has operators/choices/groups)"))

    return declarations, implementations, registrations, skipped


def print_output(declarations, implementations, registrations, skipped, gram_stem):
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


def cpp_file_content(implementations):
    return (
        GENERATED_HEADER
        + '#include "duckdb/parser/peg/transformer/peg_transformer.hpp"\n'
        + "\nnamespace duckdb {\n\n"
        + "\n".join(implementations)
        + "\n} // namespace duckdb\n"
    )


def cmake_content(cpp_filenames):
    files = "\n  ".join(cpp_filenames)
    return (
        "add_library_unity(\n"
        "  duckdb_parser_peg_transformer_generated\n"
        "  OBJECT\n"
        f"  {files})\n\n"
        "set(ALL_OBJECT_FILES\n"
        "    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_peg_transformer_generated>\n"
        "    PARENT_SCOPE)\n"
    )


def write_files(implementations, declarations, gram_stem):
    generated_dir.mkdir(parents=True, exist_ok=True)

    cpp_path = generated_dir / f"transform_{gram_stem}_generated.cpp"
    cpp_path.write_text(cpp_file_content(implementations))
    print(f"Wrote {cpp_path}")

    hpp_path = include_peg_dir / "peg_transformer_generated.hpp"
    hpp_path.write_text(GENERATED_HEADER + "".join(declarations))
    print(f"Wrote {hpp_path}")

    existing_cpp = sorted(p.name for p in generated_dir.glob("*_generated.cpp"))
    cmake_path = generated_dir / "CMakeLists.txt"
    cmake_path.write_text(cmake_content(existing_cpp))
    print(f"Wrote {cmake_path}")

    print()
    print("Remaining manual steps:")
    print(f"  1. In {include_peg_dir / 'peg_transformer.hpp'}, inside PEGTransformerFactory class:")
    print(f"       Add: #include \"duckdb/parser/peg/transformer/peg_transformer_generated.hpp\"")
    print(f"       Remove superseded TransformUseStatement(PEGTransformer &, ParseResult &) declaration")
    print(f"  2. Add 'add_subdirectory(generated)' in {transformer_dir / 'CMakeLists.txt'}")
    print(f"  3. In peg_transformer_factory.cpp RegisterUse(), replace:")
    print(f"       REGISTER_TRANSFORM(TransformUseStatement)")
    print(f"     with:")
    print(f"       Register(\"UseStatement\", &PEGTransformerFactory::TransformUseStatementInternal);")
    print(f"  4. Remove TransformUseStatementInternal from transform_use.cpp")


def main():
    arg_parser = argparse.ArgumentParser(description="Generate Internal transformer wrappers from grammar rules.")
    arg_parser.add_argument("--write", action="store_true", help="Write generated files to disk.")
    args = arg_parser.parse_args()

    use_file_path = statements_dir / 'use.gram'
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

    declarations, implementations, registrations, skipped = collect_generated(rules, rule_to_type)

    if args.write:
        write_files(implementations, declarations, gram_stem="use")
    else:
        print_output(declarations, implementations, registrations, skipped, gram_stem="use")


if __name__ == "__main__":
    main()