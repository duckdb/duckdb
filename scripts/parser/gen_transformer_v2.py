import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent))
from inline_grammar import parse_peg_grammar, PEGTokenType
from generate_transformer import GrammarTypeInfo, load_grammar_types


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
class ParensNode(GrammarNode):
    """Parens(D) <- '(' D ')'. Anonymous ListMatcher; child[1] is D's result.
    Use ExtractResultFromParens() to reach inside."""

    inner: GrammarNode


@dataclass
class ListMacroNode(GrammarNode):
    """List(D) <- D (',' D)* ','?. Anonymous ListMatcher.
    Use ExtractParseResultsFromList() to get all D results."""

    inner: GrammarNode


@dataclass
class FunctionCallNode(GrammarNode):
    """Unknown macro call (not Parens or List). Not auto-generated."""

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
    """Repeat match A+ (one or more). Corresponds to RepeatMatcher.
    A* (zero or more) is represented as OptionalNode(RepeatNode), matching
    the runtime OptionalMatcher(RepeatMatcher) structure."""

    child: GrammarNode


def tokens_to_ast(tokens):
    """
    Parse a flat PEGToken list into a GrammarNode AST tree.

    Grammar of PEG rule bodies (simplified):
      choice   = sequence ('/' sequence)*
      sequence = term+
      term     = atom ('?' | '*' | '+')?
      atom     = LITERAL | REFERENCE
                 | FUNCTION_CALL choice ')'
                 | '(' choice ')'
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
                return OptionalNode(RepeatNode(node))
            elif op == '+':
                return RepeatNode(node)
            else:
                raise Exception("Unknown operator '{}'".format(op))
        return node

    def parse_atom():
        t = peek()
        if t is None:
            raise Exception("Unexpected end of tokens in grammar AST parse")
        if t.type == PEGTokenType.LITERAL:
            return LiteralNode(consume().text)
        elif t.type == PEGTokenType.REFERENCE:
            return ReferenceNode(consume().text)
        elif t.type == PEGTokenType.FUNCTION_CALL:
            # inline_grammar already consumed the '(' and bumped bracket_count
            func_name = consume().text
            inner = parse_choice()
            if peek() and peek().type == PEGTokenType.OPERATOR and peek().text == ')':
                consume()
            if func_name == 'Parens':
                return ParensNode(inner)
            elif func_name == 'List':
                return ListMacroNode(inner)
            return FunctionCallNode(func_name, inner)
        elif t.type == PEGTokenType.OPERATOR and t.text == '(':
            consume()
            inner = parse_choice()
            if peek() and peek().type == PEGTokenType.OPERATOR and peek().text == ')':
                consume()
            return inner  # anonymous group - transparent node
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
    'Identifier',
    'ReservedIdentifier',
    'CatalogName',
    'SchemaName',
    'ReservedSchemaName',
    'TableName',
    'ReservedTableName',
    'ColumnName',
    'ReservedColumnName',
    'IndexName',
    'SequenceName',
    'FunctionName',
    'ReservedFunctionName',
    'TableFunctionName',
    'TypeName',
    'PragmaName',
    'SettingName',
    'CopyOptionName',
}


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


def generate_internal_declaration(rule_name, return_type):
    return (
        f"\tstatic {return_type} Transform{rule_name}Internal"
        f"(PEGTransformer &transformer, ParseResult &parse_result);\n"
    )


def generate_registration(rule_name):
    return f'Register("{rule_name}", &PEGTransformerFactory::Transform{rule_name}Internal);\n'


# ---------------------------------------------------------------------------
# Choice-rule helpers
# ---------------------------------------------------------------------------


def is_pure_reference_choice(ast):
    """True if ast is a ChoiceNode whose every alternative is a ReferenceNode."""
    return isinstance(ast, ChoiceNode) and all(isinstance(a, ReferenceNode) for a in ast.alternatives)


def classify_choice_alternatives(alternatives, rule_types):
    """
    Split choice alternatives into three groups:
      - transformer_alts: names with a registered transformer (in rule_types)
      - identifier_alts:  names that are identifier overrides (produce IdentifierParseResult)
      - unknown_alts:     neither registered nor known overrides -- need manual handling
    Returns (transformer_alts, identifier_alts, unknown_alts).
    """
    transformer_alts = []
    identifier_alts = []
    unknown_alts = []
    for ref in alternatives:
        name = ref.name
        if name in rule_types:
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
        f"\tstatic {return_type} Transform{rule_name}" f"(PEGTransformer &transformer, ParseResult &choice_result);\n"
    )


# ---------------------------------------------------------------------------
# Sequence-element classification
#
# Mirrors the per-token-type dispatch inside MatcherFactory::CreateMatcher()
# in matcher.cpp.  Each helper handles one matcher/parse-result kind:
#
#   _classify_literal           LiteralNode       -> KeywordParseResult           (skip)
#   _classify_reference         ReferenceNode     -> IdentifierParseResult OR Transform<T>
#   _classify_optional_reference OptionalNode(Ref) -> optional identifier OR TransformOptional<T>
#   _classify_star_repeat       OptionalNode(Rep) -> OptionalParseResult(RepeatParseResult) vector<T>
#   _classify_plus_repeat       RepeatNode        -> RepeatParseResult             vector<T>
#   _classify_parens            ParensNode(Ref)   -> ExtractResultFromParens       T
#   _classify_list_macro        ListMacroNode(Ref)-> ExtractParseResultsFromList   vector<T>
#   _classify_parens_list       ParensNode(List)  -> ExtractParseResultsFromList(ExtractResultFromParens) vector<T>
#
# classify_sequence_element() is the top-level dispatch (= the switch in CreateMatcher).
# classify_sequence_elements() iterates all children of a SequenceNode (= the token loop).
# ---------------------------------------------------------------------------


@dataclass
class SeqElement:
    """One classified position in a sequence rule."""

    skip: bool  # True for LiteralNode - no semantic value
    var_name: str = ""
    cpp_type: str = ""
    by_value: bool = False  # True for unique_ptr<T>, vector<unique_ptr<T>>, bool, int64_t
    extraction_lines: List[str] = field(default_factory=list)


def _is_by_value(rule_name, rule_types):
    """
    Return True if the C++ value for rule_name is move-only and must be passed by value with std::move.
    by_value=True in grammar_types.yml means: unique_ptr<T> or vector<unique_ptr<T>> (non-copyable).
    Primitives (bool, int64_t) use by_value=False and are passed as const T & (harmless, no tidy warning).
    Fallback: override rules without a by_value annotation use the unique_ptr< prefix heuristic.
    """
    info = rule_types.get(rule_name)
    if info is None:
        return False
    return info.by_value or info.cpp_type.startswith('unique_ptr<')


def _classify_literal():
    """LITERAL token -> KeywordMatcher -> KeywordParseResult.  No semantic value."""
    return SeqElement(skip=True)


def _classify_reference(name, idx, rule_types, excluded_rules):
    """
    REFERENCE token -> CreateMatcher(rule_name).
    Priority order mirrors runtime dispatch:
      1. IDENTIFIER_OVERRIDE_RULES  -> IdentifierMatcher -> Child<IdentifierParseResult>()
      2. excluded_rules             -> keyword-only rule, no semantic value -> skip
      3. rule_types                 -> regular ListMatcher -> transformer.Transform<T>()
    """
    if name in IDENTIFIER_OVERRIDE_RULES:
        var_name = to_snake_case(name)
        lines = [f"\tauto {var_name} = list_pr.Child<IdentifierParseResult>({idx}).identifier;"]
        return SeqElement(skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in excluded_rules:
        return _classify_literal()
    if name in rule_types:
        cpp_type = rule_types[name].cpp_type
        var_name = to_snake_case(name)
        lines = [f"\tauto {var_name} = transformer.Transform<{cpp_type}>(list_pr, {idx});"]
        return SeqElement(
            skip=False,
            var_name=var_name,
            cpp_type=cpp_type,
            by_value=_is_by_value(name, rule_types),
            extraction_lines=lines,
        )
    return None


def _classify_optional_reference(name, idx, rule_types, excluded_rules):
    """
    OptionalNode(ReferenceNode) -> OptionalMatcher wrapping a named rule.
    Priority order matches _classify_reference:
      1. excluded_rules             -> keyword-only optional (Transaction?) -> skip
      2. IDENTIFIER_OVERRIDE_RULES  -> optional identifier, extracted via HasResult()
      3. rule_types                 -> optional typed rule, extracted via TransformOptional
    """
    if name in excluded_rules:
        return _classify_literal()
    var_name = to_snake_case(name)
    if name in IDENTIFIER_OVERRIDE_RULES:
        lines = [
            f"\tstring {var_name};",
            f"\tauto &{var_name}_opt = list_pr.Child<OptionalParseResult>({idx});",
            f"\tif ({var_name}_opt.HasResult()) {{",
            f"\t\t{var_name} = {var_name}_opt.GetResult().Cast<IdentifierParseResult>().identifier;",
            f"\t}}",
        ]
        return SeqElement(skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in rule_types:
        cpp_type = rule_types[name].cpp_type
        lines = [
            f"\t{cpp_type} {var_name} {{}};",
            f"\ttransformer.TransformOptional(list_pr, {idx}, {var_name});",
        ]
        return SeqElement(
            skip=False,
            var_name=var_name,
            cpp_type=cpp_type,
            by_value=_is_by_value(name, rule_types),
            extraction_lines=lines,
        )
    return None


def _classify_parens(inner_node, idx, rule_types):
    """
    ParensNode -> Parens(D) <- '(' D ')'.
    Uses ExtractResultFromParens() to reach child[1].
    Only supported when inner is a plain ReferenceNode.
    """
    if not isinstance(inner_node, ReferenceNode):
        return None
    name = inner_node.name
    var_name = to_snake_case(name)
    if name in IDENTIFIER_OVERRIDE_RULES:
        lines = [
            f"\tauto {var_name} = ExtractResultFromParens(list_pr.GetChild({idx}))"
            f".Cast<IdentifierParseResult>().identifier;",
        ]
        return SeqElement(skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in rule_types:
        cpp_type = rule_types[name].cpp_type
        lines = [
            f"\tauto {var_name} = transformer.Transform<{cpp_type}>"
            f"(ExtractResultFromParens(list_pr.GetChild({idx})));",
        ]
        return SeqElement(
            skip=False,
            var_name=var_name,
            cpp_type=cpp_type,
            by_value=_is_by_value(name, rule_types),
            extraction_lines=lines,
        )
    return None


def _classify_list_macro(inner_node, idx, rule_types):
    """
    ListMacroNode -> List(D) <- D (',' D)* ','?.
    Uses ExtractParseResultsFromList() to collect all D results.
    Only supported when inner is a plain ReferenceNode with a known type.
    Produces vector<T>.
    """
    if not isinstance(inner_node, ReferenceNode):
        return None
    name = inner_node.name
    if name not in rule_types:
        return None
    child_type = rule_types[name].cpp_type
    var_name = to_snake_case(name)
    lines = [
        f"\tauto {var_name}_items = ExtractParseResultsFromList(list_pr.GetChild({idx}));",
        f"\tvector<{child_type}> {var_name};",
        f"\tfor (auto &{var_name}_item : {var_name}_items) {{",
        f"\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
        f"\t}}",
    ]
    return SeqElement(
        skip=False,
        var_name=var_name,
        cpp_type=f"vector<{child_type}>",
        by_value=_is_by_value(name, rule_types),
        extraction_lines=lines,
    )


def _classify_parens_list(inner_list_node, idx, rule_types):
    """
    ParensNode(ListMacroNode(D)) -> Parens(List(D)).
    Uses ExtractParseResultsFromList(ExtractResultFromParens(...)) to collect all D results.
    Only supported when the ListMacroNode's inner is a plain ReferenceNode with a known type.
    Produces vector<T>.
    """
    if not isinstance(inner_list_node.inner, ReferenceNode):
        return None
    name = inner_list_node.inner.name
    if name not in rule_types:
        return None
    child_type = rule_types[name].cpp_type
    var_name = to_snake_case(name)
    lines = [
        f"\tauto {var_name}_items = ExtractParseResultsFromList(" f"ExtractResultFromParens(list_pr.GetChild({idx})));",
        f"\tvector<{child_type}> {var_name};",
        f"\tfor (auto &{var_name}_item : {var_name}_items) {{",
        f"\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
        f"\t}}",
    ]
    return SeqElement(
        skip=False,
        var_name=var_name,
        cpp_type=f"vector<{child_type}>",
        by_value=_is_by_value(name, rule_types),
        extraction_lines=lines,
    )


def _classify_repeat(node, idx, rule_types, optional):
    """
    Shared helper for A* and A+.
    A* -> OptionalNode(RepeatNode(A)) -> OptionalParseResult wrapping RepeatParseResult.
    A+ -> RepeatNode(A)              -> RepeatParseResult directly (guaranteed >= 1 element).
    Only supported when the repeated element is a plain reference with a known type.
    Produces vector<T>.
    """
    if not isinstance(node.child, ReferenceNode):
        return None
    ref_name = node.child.name
    if ref_name not in rule_types:
        return None
    child_type = rule_types[ref_name].cpp_type
    var_name = to_snake_case(ref_name)
    if optional:
        lines = [
            f"\tauto &{var_name}_opt = list_pr.Child<OptionalParseResult>({idx});",
            f"\tvector<{child_type}> {var_name};",
            f"\tif ({var_name}_opt.HasResult()) {{",
            f"\t\tauto &{var_name}_repeat = {var_name}_opt.GetResult().Cast<RepeatParseResult>();",
            f"\t\tfor (auto &{var_name}_item : {var_name}_repeat.GetChildren()) {{",
            f"\t\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
            f"\t\t}}",
            f"\t}}",
        ]
    else:
        lines = [
            f"\tauto &{var_name}_repeat = list_pr.Child<RepeatParseResult>({idx});",
            f"\tvector<{child_type}> {var_name};",
            f"\tfor (auto &{var_name}_item : {var_name}_repeat.GetChildren()) {{",
            f"\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
            f"\t}}",
        ]
    return SeqElement(
        skip=False,
        var_name=var_name,
        cpp_type=f"vector<{child_type}>",
        by_value=_is_by_value(ref_name, rule_types),
        extraction_lines=lines,
    )


def _classify_star_repeat(node, idx, rule_types):
    return _classify_repeat(node, idx, rule_types, optional=True)


def _classify_plus_repeat(node, idx, rule_types):
    return _classify_repeat(node, idx, rule_types, optional=False)


def classify_sequence_element(child, idx, rule_types, excluded_rules):
    """
    Classify one element of a SequenceNode.
    Mirrors the token-type switch in MatcherFactory::CreateMatcher().
    Returns SeqElement or None if the element cannot be auto-generated.
    """
    if isinstance(child, LiteralNode):
        return _classify_literal()
    if isinstance(child, ReferenceNode):
        return _classify_reference(child.name, idx, rule_types, excluded_rules)
    if isinstance(child, OptionalNode):
        inner = child.child
        if isinstance(inner, LiteralNode):
            return _classify_literal()
        if isinstance(inner, ReferenceNode):
            return _classify_optional_reference(inner.name, idx, rule_types, excluded_rules)
        if isinstance(inner, RepeatNode):
            # A* is represented as OptionalNode(RepeatNode(A)), matching the runtime
            # OptionalMatcher(RepeatMatcher(A)) structure. Delegate to star-repeat classifier.
            return _classify_star_repeat(inner, idx, rule_types)
        return None  # OptionalNode(ParensNode) etc. - deferred
    if isinstance(child, RepeatNode):
        return _classify_plus_repeat(child, idx, rule_types)
    if isinstance(child, ParensNode):
        if isinstance(child.inner, ListMacroNode):
            return _classify_parens_list(child.inner, idx, rule_types)
        return _classify_parens(child.inner, idx, rule_types)
    if isinstance(child, ListMacroNode):
        return _classify_list_macro(child.inner, idx, rule_types)
    return None


def classify_sequence_elements(children, rule_types, excluded_rules):
    """
    Classify all children of a SequenceNode.
    Mirrors the token loop in MatcherFactory::CreateMatcher().
    Returns list of SeqElement, or None if any element cannot be classified.
    """
    elements = []
    for idx, child in enumerate(children):
        elem = classify_sequence_element(child, idx, rule_types, excluded_rules)
        if elem is None:
            return None
        elements.append(elem)
    return elements


# ---------------------------------------------------------------------------
# Extended sequence-rule code generation
# ---------------------------------------------------------------------------


def generate_sequence_body_decl(rule_name, return_type, elements):
    """Declaration for the hand-written body that receives extracted typed args."""

    def _param_decl(e):
        # Move-only types (unique_ptr<T>, vector<unique_ptr<T>>) are passed by value.
        # Everything else (structs, strings, primitives) uses const T & to avoid tidy warnings.
        if e.by_value:
            return f"{e.cpp_type} {e.var_name}"
        return f"const {e.cpp_type} &{e.var_name}"

    params = ", ".join(_param_decl(e) for e in elements if not e.skip)
    return f"\tstatic {return_type} Transform{rule_name}({params});\n"


def generate_sequence_internal(rule_name, return_type, elements):
    """
    Generate the Internal wrapper for a sequence rule.
    Casts parse_result to ListParseResult, extracts each semantic element
    into a typed local variable, then calls the hand-written body with those args.
    """
    semantic = [e for e in elements if not e.skip]
    has_semantic_elements = len(semantic) > 0

    body = []
    # Only emit the list_pr cast when there are elements to extract from it.
    # All-skip rules (e.g. CommitTransaction <- CommitOrEnd Transaction?)
    # produce no arguments and must not declare an unused list_pr variable.
    if has_semantic_elements:
        body.append("\tauto &list_pr = parse_result.Cast<ListParseResult>();")
        for elem in semantic:
            body.extend(elem.extraction_lines)

    def _param_arg(e):
        # by_value=True means move-only; transfer ownership to body via std::move.
        if e.by_value:
            return f"std::move({e.var_name})"
        return e.var_name

    arg_names = ", ".join(_param_arg(e) for e in semantic)
    body.append(f"\treturn Transform{rule_name}({arg_names});")
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n" + "\n".join(body) + "\n}\n"
    )


@dataclass
class GramFileResult:
    gram_stem: str
    declarations: list
    implementations: list
    registrations: list
    skipped: list  # (rule_name, reason) — nothing generated
    manual_bodies: list  # (rule_name, reason) — Internal generated, body is hand-written


def collect_generated(gram_stem, rules, rule_types, excluded_rules):
    """Classify all rules; return a GramFileResult."""
    declarations = []
    implementations = []
    registrations = []
    skipped = []
    manual_bodies = []

    for rule_name, rule in rules.items():
        return_type = rule.return_type
        if return_type is None:
            skipped.append((rule_name, "no return type in grammar_types.yml"))
            continue

        try:
            ast = rule_to_ast(rule)
        except Exception as e:
            skipped.append((rule_name, f"AST parse error: {e}"))
            continue

        if is_pure_reference_choice(ast):
            _, identifier_alts, unknown_alts = classify_choice_alternatives(ast.alternatives, rule_types)
            if unknown_alts:
                skipped.append((rule_name, f"choice has unknown alternatives: {unknown_alts}"))
                continue

            declarations.append(generate_internal_declaration(rule_name, return_type))
            registrations.append(generate_registration(rule_name))

            if not identifier_alts:
                implementations.append(generate_choice_internal_full(rule_name, return_type))
            else:
                declarations.append(generate_choice_body_declaration(rule_name, return_type))
                implementations.append(generate_choice_internal_with_body(rule_name, return_type))
                manual_bodies.append(
                    (
                        rule_name,
                        f"choice body; identifier alternatives: {identifier_alts}",
                    )
                )
            continue

        if isinstance(ast, SequenceNode):
            elements = classify_sequence_elements(ast.children, rule_types, excluded_rules)
            if elements is not None:
                declarations.append(generate_internal_declaration(rule_name, return_type))
                declarations.append(generate_sequence_body_decl(rule_name, return_type, elements))
                implementations.append(generate_sequence_internal(rule_name, return_type, elements))
                registrations.append(generate_registration(rule_name))
                continue

        skipped.append((rule_name, "complex rule (has operators/choices/groups)"))

    return GramFileResult(gram_stem, declarations, implementations, registrations, skipped, manual_bodies)


def print_output(result: GramFileResult):
    if result.skipped:
        print("=== SKIPPED (nothing generated) ===")
        for rule_name, reason in result.skipped:
            print(f"  {rule_name}: {reason}")
        print()

    if result.manual_bodies:
        print("=== MANUAL BODY NEEDED (Internal generated, body must be hand-written) ===")
        for rule_name, reason in result.manual_bodies:
            print(f"  {rule_name}: {reason}")
        print()

    print("=== DECLARATIONS (peg_transformer_generated.hpp) ===")
    print("".join(result.declarations))

    print(f"=== IMPLEMENTATION (generated/transform_{result.gram_stem}_generated.cpp) ===")
    print("".join(result.implementations))

    print(f"=== REGISTRATION (in Register{result.gram_stem.capitalize()}() in peg_transformer_factory.cpp) ===")
    print("".join(result.registrations))


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


def write_cpp_file(implementations, gram_stem):
    generated_dir.mkdir(parents=True, exist_ok=True)
    cpp_path = generated_dir / f"transform_{gram_stem}_generated.cpp"
    cpp_path.write_text(cpp_file_content(implementations))
    print(f"Wrote {cpp_path}")


def write_hpp(all_declarations):
    hpp_path = include_peg_dir / "peg_transformer_generated.hpp"
    # This file is #include-d inside the PEGTransformerFactory class body, so it is not a
    # valid standalone header. Suppress clang-tidy to avoid false positives from unknown types.
    content = GENERATED_HEADER + "// NOLINTBEGIN\n" + "".join(all_declarations) + "// NOLINTEND\n"
    hpp_path.write_text(content)
    print(f"Wrote {hpp_path}")


def write_cmake():
    existing_cpp = sorted(p.name for p in generated_dir.glob("*_generated.cpp"))
    cmake_path = generated_dir / "CMakeLists.txt"
    cmake_path.write_text(cmake_content(existing_cpp))
    print(f"Wrote {cmake_path}")


def print_manual_steps(registrations, gram_stem):
    reg_lines = "".join(f"           {r.strip()}\n" for r in registrations)
    print(
        f"""
Remaining manual steps:
  1. In {include_peg_dir / 'peg_transformer.hpp'}:
       - Add inside class PEGTransformerFactory:
           #include "duckdb/parser/peg/transformer/peg_transformer_generated.hpp"
       - Remove any declarations now covered by peg_transformer_generated.hpp
  2. In {transformer_dir / 'CMakeLists.txt'}:
       - Add: add_subdirectory(generated)
  3. In peg_transformer_factory.cpp Register{gram_stem.capitalize()}():
       - Replace REGISTER_TRANSFORM macros for generated rules with:
{reg_lines}  4. In transform_{gram_stem}.cpp:
       - Remove Internal wrappers now generated (keep only hand-written bodies)
       - Update body function signatures to match the generated declarations"""
    )


def process_gram_file(gram_filename, rule_types, excluded_rules):
    """Parse a .gram file and classify all its rules into a GramFileResult."""
    gram_stem = gram_filename.removesuffix('.gram')
    gram_path = statements_dir / gram_filename
    try:
        rules = parse_peg_grammar(gram_path.read_text())
    except Exception as e:
        raise Exception(f"{gram_filename}: {e}") from None

    for rule_name, info in rule_types.items():
        if rule_name in rules:
            rules[rule_name].return_type = info.cpp_type

    return collect_generated(gram_stem, rules, rule_types, excluded_rules)


def main():
    arg_parser = argparse.ArgumentParser(description="Generate Internal transformer wrappers from grammar rules.")
    arg_parser.add_argument("--write", action="store_true", help="Write generated files to disk.")
    args = arg_parser.parse_args()

    gram_files_to_gen = ['use.gram', 'transaction.gram', 'detach.gram', 'export.gram']
    rule_types, excluded_rules = load_grammar_types(type_dir / 'grammar_types.yml')
    results = [process_gram_file(f, rule_types, excluded_rules) for f in gram_files_to_gen]

    if args.write:
        all_declarations = [d for r in results for d in r.declarations]
        write_hpp(all_declarations)
        for r in results:
            write_cpp_file(r.implementations, r.gram_stem)
        write_cmake()
        for r in results:
            print_manual_steps(r.registrations, r.gram_stem)
    else:
        for r in results:
            print(f"\n{'=' * 60}")
            print(f"  {r.gram_stem}.gram")
            print(f"{'=' * 60}")
            print_output(r)


if __name__ == "__main__":
    main()
