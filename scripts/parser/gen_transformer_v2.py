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
    """Repeat match A+ (min=1) or A* (min=0). Corresponds to RepeatMatcher."""

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
    return (
        f"\tstatic {return_type} Transform{rule_name}Internal"
        f"(PEGTransformer &transformer, ParseResult &parse_result);\n"
    )


def generate_body_declaration(rule_name, return_type, semantic_children, rule_to_type):
    params = ", ".join(f"{rule_to_type[name]} {to_snake_case(name)}" for _, name in semantic_children)
    return f"\tstatic {return_type} Transform{rule_name}({params});\n"


def generate_internal_wrapper(rule_name, return_type, semantic_children, rule_to_type):
    """Generates the Internal .cpp function that extracts children and calls the body."""
    arg_lines = []
    arg_names = []
    for idx, child_name in semantic_children:
        var = to_snake_case(child_name)
        if child_name in IDENTIFIER_OVERRIDE_RULES:
            arg_lines.append(f"\tauto {var} = list_pr.Child<IdentifierParseResult>({idx}).identifier;")
        else:
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
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n" + "\n".join(body) + "\n}\n"
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
        f"\tstatic {return_type} Transform{rule_name}" f"(PEGTransformer &transformer, ParseResult &choice_result);\n"
    )


# ---------------------------------------------------------------------------
# Sequence-element classification
#
# Mirrors the per-token-type dispatch inside MatcherFactory::CreateMatcher()
# in matcher.cpp.  Each helper handles exactly one matcher/parse-result kind:
#
#   _classify_literal       <- LITERAL   -> KeywordMatcher   -> KeywordParseResult  (skip)
#   _classify_reference     <- REFERENCE -> named rule OR identifier override
#   _classify_star_repeat   <- OPERATOR* -> Optional(Repeat) -> OptionalParseResult(RepeatParseResult)
#
# classify_sequence_element() is the top-level dispatch (= the switch in CreateMatcher).
# classify_sequence_elements() iterates all children of a SequenceNode (= the token loop).
# ---------------------------------------------------------------------------


@dataclass
class SeqElement:
    """One classified position in a sequence rule."""

    idx: int
    skip: bool  # True for LiteralNode - no semantic value
    var_name: str = ""
    cpp_type: str = ""
    extraction_lines: List[str] = field(default_factory=list)


def _classify_literal(idx):
    """LITERAL token -> KeywordMatcher -> KeywordParseResult.  No semantic value."""
    return SeqElement(idx=idx, skip=True)


def _classify_reference(name, idx, rule_to_type, excluded_rules):
    """
    REFERENCE token -> CreateMatcher(rule_name).
    Priority order mirrors runtime dispatch:
      1. IDENTIFIER_OVERRIDE_RULES  -> IdentifierMatcher -> Child<IdentifierParseResult>()
      2. excluded_rules             -> keyword-only rule, no semantic value -> skip
      3. rule_to_type               -> regular ListMatcher -> transformer.Transform<T>()
    """
    if name in IDENTIFIER_OVERRIDE_RULES:
        var_name = to_snake_case(name)
        lines = [f"\tauto {var_name} = list_pr.Child<IdentifierParseResult>({idx}).identifier;"]
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in excluded_rules:
        return _classify_literal(idx)
    if name in rule_to_type:
        cpp_type = rule_to_type[name]
        var_name = to_snake_case(name)
        lines = [f"\tauto {var_name} = transformer.Transform<{cpp_type}>(list_pr, {idx});"]
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=cpp_type, extraction_lines=lines)
    return None


def _classify_optional_reference(name, idx, rule_to_type, excluded_rules):
    """
    OptionalNode(ReferenceNode) -> OptionalMatcher wrapping a named rule.
    Priority order matches _classify_reference:
      1. excluded_rules        -> keyword-only optional (Transaction?) -> skip
      2. IDENTIFIER_OVERRIDE_RULES -> optional identifier, extracted via HasResult()
      3. rule_to_type          -> optional typed rule, extracted via TransformOptional
    """
    if name in excluded_rules:
        return _classify_literal(idx)
    var_name = to_snake_case(name)
    if name in IDENTIFIER_OVERRIDE_RULES:
        lines = [
            f"\tstring {var_name};",
            f"\tauto &{var_name}_opt = list_pr.Child<OptionalParseResult>({idx});",
            f"\tif ({var_name}_opt.HasResult()) {{",
            f"\t\t{var_name} = {var_name}_opt.GetResult().Cast<IdentifierParseResult>().identifier;",
            f"\t}}",
        ]
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in rule_to_type:
        cpp_type = rule_to_type[name]
        lines = [
            f"\t{cpp_type} {var_name} {{}};",
            f"\ttransformer.TransformOptional(list_pr, {idx}, {var_name});",
        ]
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=cpp_type, extraction_lines=lines)
    return None


def _classify_parens(inner_node, idx, rule_to_type):
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
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type="string", extraction_lines=lines)
    if name in rule_to_type:
        cpp_type = rule_to_type[name]
        lines = [
            f"\tauto {var_name} = transformer.Transform<{cpp_type}>"
            f"(ExtractResultFromParens(list_pr.GetChild({idx})));",
        ]
        return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=cpp_type, extraction_lines=lines)
    return None


def _classify_list_macro(inner_node, idx, rule_to_type):
    """
    ListMacroNode -> List(D) <- D (',' D)* ','?.
    Uses ExtractParseResultsFromList() to collect all D results.
    Only supported when inner is a plain ReferenceNode with a known type.
    Produces vector<T>.
    """
    if not isinstance(inner_node, ReferenceNode):
        return None
    name = inner_node.name
    if name not in rule_to_type:
        return None
    child_type = rule_to_type[name]
    var_name = to_snake_case(name)
    lines = [
        f"\tauto {var_name}_items = ExtractParseResultsFromList(list_pr.GetChild({idx}));",
        f"\tvector<{child_type}> {var_name};",
        f"\tfor (auto &{var_name}_item : {var_name}_items) {{",
        f"\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
        f"\t}}",
    ]
    return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=f"vector<{child_type}>", extraction_lines=lines)


def _classify_parens_list(inner_list_node, idx, rule_to_type):
    """
    ParensNode(ListMacroNode(D)) -> Parens(List(D)).
    Uses ExtractParseResultsFromList(ExtractResultFromParens(...)) to collect all D results.
    Only supported when the ListMacroNode's inner is a plain ReferenceNode with a known type.
    Produces vector<T>.
    """
    if not isinstance(inner_list_node.inner, ReferenceNode):
        return None
    name = inner_list_node.inner.name
    if name not in rule_to_type:
        return None
    child_type = rule_to_type[name]
    var_name = to_snake_case(name)
    lines = [
        f"\tauto {var_name}_items = ExtractParseResultsFromList(" f"ExtractResultFromParens(list_pr.GetChild({idx})));",
        f"\tvector<{child_type}> {var_name};",
        f"\tfor (auto &{var_name}_item : {var_name}_items) {{",
        f"\t\t{var_name}.push_back(transformer.Transform<{child_type}>({var_name}_item));",
        f"\t}}",
    ]
    return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=f"vector<{child_type}>", extraction_lines=lines)


def _classify_repeat(node, idx, rule_to_type, optional):
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
    if ref_name not in rule_to_type:
        return None
    child_type = rule_to_type[ref_name]
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
    return SeqElement(idx=idx, skip=False, var_name=var_name, cpp_type=f"vector<{child_type}>", extraction_lines=lines)


def _classify_star_repeat(node, idx, rule_to_type):
    return _classify_repeat(node, idx, rule_to_type, optional=True)


def _classify_plus_repeat(node, idx, rule_to_type):
    return _classify_repeat(node, idx, rule_to_type, optional=False)


def classify_sequence_element(child, idx, rule_to_type, excluded_rules):
    """
    Classify one element of a SequenceNode.
    Mirrors the token-type switch in MatcherFactory::CreateMatcher().
    Returns SeqElement or None if the element cannot be auto-generated.
    """
    if isinstance(child, LiteralNode):
        return _classify_literal(idx)
    if isinstance(child, ReferenceNode):
        return _classify_reference(child.name, idx, rule_to_type, excluded_rules)
    if isinstance(child, OptionalNode):
        inner = child.child
        if isinstance(inner, LiteralNode):
            return _classify_literal(idx)
        if isinstance(inner, ReferenceNode):
            return _classify_optional_reference(inner.name, idx, rule_to_type, excluded_rules)
        if isinstance(inner, RepeatNode):
            # A* is represented as OptionalNode(RepeatNode(A)), matching the runtime
            # OptionalMatcher(RepeatMatcher(A)) structure. Delegate to star-repeat classifier.
            return _classify_star_repeat(inner, idx, rule_to_type)
        return None  # OptionalNode(ParensNode) etc. - deferred
    if isinstance(child, RepeatNode):
        return _classify_plus_repeat(child, idx, rule_to_type)
    if isinstance(child, ParensNode):
        if isinstance(child.inner, ListMacroNode):
            return _classify_parens_list(child.inner, idx, rule_to_type)
        return _classify_parens(child.inner, idx, rule_to_type)
    if isinstance(child, ListMacroNode):
        return _classify_list_macro(child.inner, idx, rule_to_type)
    return None


def classify_sequence_elements(children, rule_to_type, excluded_rules):
    """
    Classify all children of a SequenceNode.
    Mirrors the token loop in MatcherFactory::CreateMatcher().
    Returns list of SeqElement, or None if any element cannot be classified.
    """
    elements = []
    for idx, child in enumerate(children):
        elem = classify_sequence_element(child, idx, rule_to_type, excluded_rules)
        if elem is None:
            return None
        elements.append(elem)
    return elements


# ---------------------------------------------------------------------------
# Extended sequence-rule code generation
# ---------------------------------------------------------------------------


def is_auto_sequence_ast(ast, rule_to_type, excluded_rules):
    """True if ast is a SequenceNode whose every element can be classified."""
    return (
        isinstance(ast, SequenceNode)
        and classify_sequence_elements(ast.children, rule_to_type, excluded_rules) is not None
    )


def generate_sequence_body_decl(rule_name, return_type, elements):
    """Declaration for the hand-written body that receives extracted typed args."""
    params = ", ".join(f"{e.cpp_type} {e.var_name}" for e in elements if not e.skip)
    return f"\tstatic {return_type} Transform{rule_name}({params});\n"


def generate_sequence_internal(rule_name, return_type, elements):
    """
    Internal wrapper that casts to ListParseResult, extracts each element,
    then calls the hand-written body.  Mirrors what ListMatcher::MatchParseResult
    does at runtime but in the code-generation direction.
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

    arg_names = ", ".join(e.var_name for e in semantic)
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


def collect_generated(gram_stem, rules, rule_to_type, excluded_rules):
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

        if is_auto_sequence_ast(ast, rule_to_type, excluded_rules):
            elements = classify_sequence_elements(ast.children, rule_to_type, excluded_rules)
            declarations.append(generate_internal_declaration(rule_name, return_type))
            declarations.append(generate_sequence_body_decl(rule_name, return_type, elements))
            implementations.append(generate_sequence_internal(rule_name, return_type, elements))
            registrations.append(generate_registration(rule_name))
            continue

        skipped.append((rule_name, "complex rule (has operators/choices/groups)"))

    return GramFileResult(gram_stem, declarations, implementations, registrations, skipped, manual_bodies)


def print_output(declarations, implementations, registrations, skipped, manual_bodies, gram_stem):
    if skipped:
        print("=== SKIPPED (nothing generated) ===")
        for rule_name, reason in skipped:
            print(f"  {rule_name}: {reason}")
        print()

    if manual_bodies:
        print("=== MANUAL BODY NEEDED (Internal generated, body must be hand-written) ===")
        for rule_name, reason in manual_bodies:
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


def write_cpp_file(implementations, gram_stem):
    generated_dir.mkdir(parents=True, exist_ok=True)
    cpp_path = generated_dir / f"transform_{gram_stem}_generated.cpp"
    cpp_path.write_text(cpp_file_content(implementations))
    print(f"Wrote {cpp_path}")


def write_shared_files(all_declarations):
    hpp_path = include_peg_dir / "peg_transformer_generated.hpp"
    hpp_path.write_text(GENERATED_HEADER + "".join(all_declarations))
    print(f"Wrote {hpp_path}")

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


def process_gram_file(gram_filename, rule_to_type, excluded_rules):
    gram_stem = gram_filename.removesuffix('.gram')
    gram_path = statements_dir / gram_filename
    with open(gram_path, 'r') as f:
        file_content = f.read()
    try:
        rules = parse_peg_grammar(file_content)
    except Exception as e:
        raise Exception(f"{gram_filename}: {e}") from None

    for rule_name, return_type in rule_to_type.items():
        if rule_name in rules:
            rules[rule_name].return_type = return_type

    return collect_generated(gram_stem, rules, rule_to_type, excluded_rules)


def main():
    arg_parser = argparse.ArgumentParser(description="Generate Internal transformer wrappers from grammar rules.")
    arg_parser.add_argument("--write", action="store_true", help="Write generated files to disk.")
    args = arg_parser.parse_args()

    gram_files_to_gen = ['use.gram', 'transaction.gram']
    rule_to_type, excluded_rules = load_grammar_types(type_dir / 'grammar_types.yml')
    results = [process_gram_file(f, rule_to_type, excluded_rules) for f in gram_files_to_gen]

    if args.write:
        all_declarations = [d for r in results for d in r.declarations]
        write_shared_files(all_declarations)
        for r in results:
            write_cpp_file(r.implementations, r.gram_stem)
            print_manual_steps(r.registrations, r.gram_stem)
    else:
        for r in results:
            print(f"\n{'=' * 60}")
            print(f"  {r.gram_stem}.gram")
            print(f"{'=' * 60}")
            print_output(
                r.declarations, r.implementations, r.registrations, r.skipped, r.manual_bodies, gram_stem=r.gram_stem
            )


if __name__ == "__main__":
    main()
