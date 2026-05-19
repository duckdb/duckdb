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
include_peg_dir = src_dir / 'include' / 'duckdb' / 'parser' / 'peg' / 'transformer'

GENERATED_HEADER = "// AUTO-GENERATED by scripts/parser/gen_transformer_v2.py -- DO NOT EDIT\n"


def to_snake_case(name):
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def generate_internal_declaration(rule_name):
    return (
        f"\tstatic unique_ptr<TransformResultValue> Transform{rule_name}Internal"
        f"(PEGTransformer &transformer, ParseResult &parse_result);\n"
    )


def generate_registration(rule_name):
    return f'\t{{"{rule_name}", &PEGTransformerFactory::Transform{rule_name}Internal}},\n'


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
        assert isinstance(ref, ReferenceNode)
        name = ref.name
        if name in rule_types:
            transformer_alts.append(name)
        elif name in IDENTIFIER_OVERRIDE_RULES:
            identifier_alts.append(name)
        else:
            unknown_alts.append(name)
    return transformer_alts, identifier_alts, unknown_alts


def _box_result(return_type, return_by_value):
    """
    Generate the boxing return statement for an Internal function.
    Use std::move only for move-only types (unique_ptr<T>, vector<unique_ptr<T>>).
    Trivially-copyable types (enums, primitives) and copyable structs use a plain copy to
    avoid the performance-move-const-arg clang-tidy warning.
    """
    arg = "std::move(result)" if return_by_value else "result"
    return f"\treturn make_uniq<TypedTransformResult<{return_type}>>({arg});\n"


def generate_choice_internal_full(rule_name, return_type, return_by_value):
    """
    Fully auto-generated Internal for a pure-transformer choice rule.
    Static class member matching transform_function_t for the static TransformRule table.
    """
    return (
        f"unique_ptr<TransformResultValue> PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\tauto result = transformer.Transform<{return_type}>(choice_pr.GetResult());\n"
        + _box_result(return_type, return_by_value)
        + f"}}\n"
    )


def generate_choice_internal_with_body(rule_name, return_type, return_by_value):
    """
    Internal for a choice rule that has identifier-override alternatives.
    Static class member matching transform_function_t for the static TransformRule table.
    """
    return (
        f"unique_ptr<TransformResultValue> PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\tauto result = Transform{rule_name}(transformer, choice_pr.GetResult());\n"
        + _box_result(return_type, return_by_value)
        + f"}}\n"
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
#   _classify_literal           LiteralNode          -> KeywordParseResult                     (skip)
#   _classify_reference         ReferenceNode        -> IdentifierParseResult OR Transform<T>
#   _classify_optional_reference OptionalNode(Ref)   -> optional identifier OR TransformOptional<T>
#   _classify_repeat            OptionalNode(Rep)/Rep -> OptionalParseResult(Repeat)/Repeat     vector<T>
#   _classify_macro             ParensNode/ListMacroNode (any depth) -> scalar T or vector<T>
#     _analyze_macro_node       recursively unwraps to (leaf_name, ['parens'/'list', ...])
#     _build_wrapped_expr       builds nested ExtractResultFromParens(...) call chains
#     Examples: D, Parens(D), List(D), Parens(List(D)), List(Parens(D)), ...
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
    by_value: bool = False  # True for unique_ptr<T> and vector<unique_ptr<T>> (non-copyable)
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
    return info.by_value or 'unique_ptr<' in info.cpp_type


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
      1. IDENTIFIER_OVERRIDE_RULES  -> optional identifier, extracted via HasResult()
      2. excluded_rules             -> keyword-only optional (Transaction?) -> skip
      3. rule_types                 -> optional typed rule, extracted via TransformOptional
    """
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
    if name in excluded_rules:
        return _classify_literal()
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


def _analyze_macro_node(node):
    """
    Recursively unwrap Parens/List nesting to find the leaf ReferenceNode.
    Returns (leaf_name, ops) where ops is a list of 'parens'/'list' tokens
    ordered outermost to innermost.  Returns None for unsupported structures.
    """
    if isinstance(node, ReferenceNode):
        return (node.name, [])
    if isinstance(node, ParensNode):
        result = _analyze_macro_node(node.inner)
        if result is None:
            return None
        leaf_name, ops = result
        return (leaf_name, ['parens'] + ops)
    if isinstance(node, ListMacroNode):
        result = _analyze_macro_node(node.inner)
        if result is None:
            return None
        leaf_name, ops = result
        return (leaf_name, ['list'] + ops)
    return None


def _build_wrapped_expr(base_expr, parens_count):
    """Wrap base_expr in parens_count layers of ExtractResultFromParens."""
    expr = base_expr
    for _ in range(parens_count):
        expr = f"ExtractResultFromParens({expr})"
    return expr


def _classify_macro(node, idx, rule_types, optional=False):
    """
    Unified classifier for arbitrary Parens/List nesting around a leaf rule.

    Scalar result (no List in the chain):
      D, Parens(D), Parens(Parens(D)), ...

    Vector result (exactly one List in the chain):
      List(D), Parens(List(D)), List(Parens(D)), Parens(List(Parens(D))), ...

    Nested lists (List(List(D))) produce vector<vector<T>> and are not supported.

    When optional=True the node is wrapped in an OptionalParseResult: the vector
    case emits a HasResult() guard (scalar optional macros are not supported).
    """
    result = _analyze_macro_node(node)
    if result is None:
        return None
    leaf_name, ops = result

    var_name = to_snake_case(leaf_name)
    is_identifier = leaf_name in IDENTIFIER_OVERRIDE_RULES

    if not is_identifier and leaf_name not in rule_types:
        return None

    child_type = "string" if is_identifier else rule_types[leaf_name].cpp_type

    list_positions = [i for i, op in enumerate(ops) if op == 'list']
    if len(list_positions) > 1:
        return None  # nested lists not supported

    if not list_positions:
        if optional:
            return None  # scalar optional macro not supported
        # Scalar path: all ops are 'parens'.
        access_expr = _build_wrapped_expr(f"list_pr.GetChild({idx})", len(ops))
        if is_identifier:
            line = f"\tauto {var_name} = {access_expr}.Cast<IdentifierParseResult>().identifier;"
        else:
            line = f"\tauto {var_name} = transformer.Transform<{child_type}>({access_expr});"
        return SeqElement(
            skip=False,
            var_name=var_name,
            cpp_type=child_type,
            by_value=False if is_identifier else _is_by_value(leaf_name, rule_types),
            extraction_lines=[line],
        )

    # Vector path: parens before the list wrap the collection access;
    # parens after the list unwrap each individual item.
    list_pos = list_positions[0]
    pre_parens = ops[:list_pos].count('parens')
    post_parens = ops[list_pos + 1 :].count('parens')

    item_var = f"{var_name}_item"
    item_access = _build_wrapped_expr(item_var, post_parens)

    if is_identifier:
        # item_var is reference<ParseResult> (std::reference_wrapper); need .get() when not already
        # unwrapped by ExtractResultFromParens (which returns ParseResult&).
        ident_access = f"{item_access}.get()" if post_parens == 0 else item_access
        push_content = f"{var_name}.push_back({ident_access}.Cast<IdentifierParseResult>().identifier);"
    else:
        push_content = f"{var_name}.push_back(transformer.Transform<{child_type}>({item_access}));"

    if optional:
        opt_var = f"{var_name}_opt"
        base_expr = f"{opt_var}.GetResult()"
        ind = "\t\t"
    else:
        base_expr = f"list_pr.GetChild({idx})"
        ind = "\t"

    outer_expr = _build_wrapped_expr(base_expr, pre_parens)
    loop_lines = [
        f"{ind}auto {var_name}_items = ExtractParseResultsFromList({outer_expr});",
        f"{ind}for (auto &{item_var} : {var_name}_items) {{",
        f"{ind}\t{push_content}",
        f"{ind}}}",
    ]

    if optional:
        lines = [
            f"\tauto &{opt_var} = list_pr.Child<OptionalParseResult>({idx});",
            f"\tvector<{child_type}> {var_name};",
            f"\tif ({opt_var}.HasResult()) {{",
            *loop_lines,
            f"\t}}",
        ]
    else:
        lines = [
            f"\tvector<{child_type}> {var_name};",
            *loop_lines,
        ]
    return SeqElement(
        skip=False,
        var_name=var_name,
        cpp_type=f"vector<{child_type}>",
        by_value=False if is_identifier else _is_by_value(leaf_name, rule_types),
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
    child = node.child
    if not isinstance(child, ReferenceNode):
        return None
    ref_name = child.name
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
            # OptionalMatcher(RepeatMatcher(A)) structure.
            return _classify_repeat(inner, idx, rule_types, optional=True)
        if isinstance(inner, (ParensNode, ListMacroNode)):
            return _classify_macro(inner, idx, rule_types, optional=True)
        return None
    if isinstance(child, RepeatNode):
        return _classify_repeat(child, idx, rule_types, optional=False)
    if isinstance(child, (ParensNode, ListMacroNode)):
        return _classify_macro(child, idx, rule_types)
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


def _sequence_skip_reason(children, rule_types, excluded_rules):
    """Return a specific reason string explaining why classify_sequence_elements failed."""
    for idx, child in enumerate(children):
        if classify_sequence_element(child, idx, rule_types, excluded_rules) is not None:
            continue
        inner = child.child if isinstance(child, OptionalNode) else child
        if isinstance(inner, ReferenceNode):
            name = inner.name
            if name not in rule_types and name not in excluded_rules and name not in IDENTIFIER_OVERRIDE_RULES:
                return f"child rule '{name}' is missing from grammar_types.yml and excluded_rules"
        return f"cannot classify element {idx} ({type(child).__name__})"
    return "unknown reason"


# ---------------------------------------------------------------------------
# Extended sequence-rule code generation
# ---------------------------------------------------------------------------


def _seq_param_decl(e):
    """Format one SeqElement as a C++ parameter declaration."""
    if e.by_value:
        return f"{e.cpp_type} {e.var_name}"
    return f"const {e.cpp_type} &{e.var_name}"


def generate_sequence_body_decl(rule_name, return_type, elements):
    """Declaration for the hand-written body that receives extracted typed args."""
    typed_params = ", ".join(_seq_param_decl(e) for e in elements if not e.skip)
    params = f"PEGTransformer &transformer, {typed_params}" if typed_params else "PEGTransformer &transformer"
    return f"\tstatic {return_type} Transform{rule_name}({params});\n"


def generate_sequence_body_stub(rule_name, return_type, elements):
    """Stub .cpp definition for a sequence body that must be hand-implemented."""
    typed_params = ", ".join(_seq_param_decl(e) for e in elements if not e.skip)
    params = f"PEGTransformer &transformer, {typed_params}" if typed_params else "PEGTransformer &transformer"
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}({params}) {{\n"
        f"\tthrow NotImplementedException(\"Transform{rule_name}\");\n"
        f"}}\n"
    )


def generate_choice_body_stub(rule_name, return_type):
    """Stub .cpp definition for a choice body that must be hand-implemented."""
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}"
        f"(PEGTransformer &transformer, ParseResult &choice_result) {{\n"
        f"\tthrow NotImplementedException(\"Transform{rule_name}\");\n"
        f"}}\n"
    )


def generate_sequence_internal(rule_name, return_type, return_by_value, elements):
    """
    Generate the Internal static class member for a sequence rule.
    Returns unique_ptr<TransformResultValue> matching transform_function_t for the static table.
    Extracts typed args from parse_result, calls the hand-written body, then boxes via TypedTransformResult.
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

    args = ["transformer"] + [_param_arg(e) for e in semantic]
    body.append(f"\tauto result = Transform{rule_name}({', '.join(args)});")
    box = _box_result(return_type, return_by_value).rstrip('\n')
    body.append(box)
    return (
        f"unique_ptr<TransformResultValue> PEGTransformerFactory::Transform{rule_name}Internal(\n"
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
    body_stubs: list  # cpp definition stubs for bodies that need hand-implementation


def collect_generated(gram_stem, rules, rule_types, excluded_rules):
    """Classify all rules; return a GramFileResult."""
    declarations = []
    implementations = []
    registrations = []
    skipped = []
    manual_bodies = []
    body_stubs = []

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

        return_by_value = _is_by_value(rule_name, rule_types)

        if is_pure_reference_choice(ast):
            _, identifier_alts, unknown_alts = classify_choice_alternatives(ast.alternatives, rule_types)
            if unknown_alts:
                skipped.append((rule_name, f"choice has unknown alternatives: {unknown_alts}"))
                continue

            declarations.append(generate_internal_declaration(rule_name))
            registrations.append(generate_registration(rule_name))

            if not identifier_alts:
                implementations.append(generate_choice_internal_full(rule_name, return_type, return_by_value))
            else:
                declarations.append(generate_choice_body_declaration(rule_name, return_type))
                implementations.append(generate_choice_internal_with_body(rule_name, return_type, return_by_value))
                manual_bodies.append((rule_name, f"choice body; identifier alternatives: {identifier_alts}"))
                body_stubs.append((rule_name, generate_choice_body_stub(rule_name, return_type)))
            continue

        # Normalize: a single non-sequence, non-choice token (e.g. a lone keyword literal)
        # is treated as a one-element sequence so the all-skip path handles it.
        if not isinstance(ast, (SequenceNode, ChoiceNode)):
            ast = SequenceNode([ast])

        if isinstance(ast, SequenceNode):
            elements = classify_sequence_elements(ast.children, rule_types, excluded_rules)
            if elements is not None:
                declarations.append(generate_internal_declaration(rule_name))
                declarations.append(generate_sequence_body_decl(rule_name, return_type, elements))
                implementations.append(generate_sequence_internal(rule_name, return_type, return_by_value, elements))
                registrations.append(generate_registration(rule_name))
                body_stubs.append((rule_name, generate_sequence_body_stub(rule_name, return_type, elements)))
                continue
            skipped.append((rule_name, _sequence_skip_reason(ast.children, rule_types, excluded_rules)))
            continue

        skipped.append((rule_name, "complex rule (has operators/choices/groups)"))

    return GramFileResult(
        gram_stem=gram_stem,
        declarations=declarations,
        implementations=implementations,
        registrations=registrations,
        skipped=skipped,
        manual_bodies=manual_bodies,
        body_stubs=body_stubs,
    )


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

    print("=== DECLARATIONS (peg_transformer.hpp, between generated markers) ===")
    print("".join(result.declarations))

    print(f"=== IMPLEMENTATION (generated/transform_{result.gram_stem}_generated.cpp) ===")
    print("".join(result.implementations))

    print(f"=== REGISTRATION (transform_generated.cpp static table) ===")
    print("".join(result.registrations))

    if result.body_stubs:
        print(f"=== BODY STUBS (add to transform_{result.gram_stem}.cpp) ===")
        print("".join(stub for _, stub in result.body_stubs))


def generate_table_and_register(all_registrations):
    entries = "".join("\t\t" + e.lstrip() for e in all_registrations)
    return (
        "void PEGTransformerFactory::RegisterGenerated() {\n"
        + "\tstatic const TransformRule builtin_transform_rules[] = {\n"
        + entries
        + "\t};\n"
        + "\tfor (const auto &rule : builtin_transform_rules) {\n"
        + "\t\tsql_transform_functions[rule.name] = rule.transform;\n"
        + "\t}\n"
        + "}\n"
    )


def write_cpp(all_implementations, all_registrations):
    cpp_path = transformer_dir / "transform_generated.cpp"
    content = (
        GENERATED_HEADER
        + '#include "duckdb/parser/peg/transformer/peg_transformer.hpp"\n'
        + "\nnamespace duckdb {\n\n"
        + "\n".join(all_implementations)
        + "\n"
        + generate_table_and_register(all_registrations)
        + "\n} // namespace duckdb\n"
    )
    cpp_path.write_text(content)
    print(f"Wrote {cpp_path}")


_SEPARATOR = "\t//===--------------------------------------------------------------------===//\n"
_START_BLOCK = _SEPARATOR + "\t// START GENERATED RULES\n" + _SEPARATOR
_END_BLOCK = _SEPARATOR + "\t// END GENERATED RULES\n" + _SEPARATOR


def write_hpp(all_declarations):
    hpp_path = include_peg_dir / "peg_transformer.hpp"
    content = hpp_path.read_text()

    start_idx = content.find(_START_BLOCK)
    if start_idx == -1:
        raise RuntimeError(f"Could not find START GENERATED RULES marker in {hpp_path}")
    end_idx = content.find(_END_BLOCK, start_idx + len(_START_BLOCK))
    if end_idx == -1:
        raise RuntimeError(f"Could not find END GENERATED RULES marker in {hpp_path}")

    block_end = end_idx + len(_END_BLOCK)
    generated_block = _START_BLOCK + "".join(all_declarations) + _END_BLOCK
    new_content = content[:start_idx] + generated_block + content[block_end:]
    hpp_path.write_text(new_content)
    print(f"Updated {hpp_path}")


def _extract_func_signature(text, func_name):
    """Extract 'ReturnType PEGTransformerFactory::func_name(params)' from C++ source text."""
    m = re.search(rf'\bPEGTransformerFactory::{re.escape(func_name)}\s*\(', text)
    if not m:
        return None
    line_start = text.rfind('\n', 0, m.start()) + 1
    paren_start = text.index('(', m.start())
    depth = 0
    for i, char in enumerate(text[paren_start:]):
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
            if depth == 0:
                return text[line_start : paren_start + i + 1]
    return None


def _norm_ws(s):
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\(\s+', '(', s)
    s = re.sub(r'\s+\)', ')', s)
    return s


def _check_implementations(gram_stem, body_stubs):
    """Check which body stubs are implemented in transform_{gram_stem}.cpp.

    Compares from PEGTransformerFactory:: onwards (normalized whitespace) so that return types
    split across lines and multi-line param lists are handled correctly.

    Returns (implemented, mismatched):
      implemented: set of rule_names whose signature exactly matches the stub
      mismatched:  set of rule_names that exist in the file but with a different signature
    """
    cpp_path = transformer_dir / f"transform_{gram_stem}.cpp"
    if not cpp_path.exists():
        return set(), set()
    text = cpp_path.read_text()
    prefix = 'PEGTransformerFactory::'
    implemented = set()
    mismatched = set()
    for rule_name, stub_cpp in body_stubs:
        actual = _extract_func_signature(text, f'Transform{rule_name}')
        if actual is None:
            continue
        first_line = stub_cpp.split('\n')[0]
        expected = _norm_ws(first_line.rstrip('{').rstrip())
        actual = _norm_ws(actual)
        expected_norm = expected[expected.find(prefix) :] if prefix in expected else expected
        actual_norm = actual[actual.find(prefix) :] if prefix in actual else actual
        if expected_norm == actual_norm:
            implemented.add(rule_name)
        else:
            mismatched.add(rule_name)
    return implemented, mismatched


def print_manual_steps(all_results):
    print("\nRemaining manual steps:")

    step = 1

    all_skipped = [(r.gram_stem, rule_name, reason) for r in all_results for rule_name, reason in r.skipped]
    if all_skipped:
        print(f"\n  {step}. Skipped rules (not auto-generated):")
        current_stem = None
        for gram_stem, rule_name, reason in all_skipped:
            if gram_stem != current_stem:
                print(f"\n       {gram_stem}.gram:")
                current_stem = gram_stem
            print(f"         {rule_name}: {reason}")
        step += 1

    transform_files = [r for r in all_results if r.declarations]
    if transform_files:
        file_list = ", ".join(f"transform_{r.gram_stem}.cpp" for r in transform_files)
        print(f"\n  {step}. {transformer_dir}/[{file_list}]:")
        print("       - Remove Internal wrappers that are now generated (keep only hand-written bodies)")
        print("       - Update body function signatures to match the generated declarations")
        step += 1

    has_any_stubs = any(r.body_stubs for r in all_results)
    if has_any_stubs:
        pending_stubs = []
        sig_mismatches = []
        for r in all_results:
            if not r.body_stubs:
                continue
            implemented, mismatched = _check_implementations(r.gram_stem, r.body_stubs)
            for rule_name, stub in r.body_stubs:
                if rule_name in implemented:
                    pass
                elif rule_name in mismatched:
                    sig_mismatches.append((r.gram_stem, rule_name, stub.split('\n')[0].rstrip('{').rstrip()))
                else:
                    pending_stubs.append((r.gram_stem, stub))

        if sig_mismatches:
            print(f"\n  {step}. Signature mismatches (function exists but params don't match generated declaration):")
            current_stem = None
            for gram_stem, rule_name, expected_sig in sig_mismatches:
                if gram_stem != current_stem:
                    print(f"\n       transform_{gram_stem}.cpp:")
                    current_stem = gram_stem
                print(f"         Transform{rule_name} -- update to: {expected_sig}")
            step += 1

        if pending_stubs:
            print(f"\n  {step}. Body stubs to implement (copy into respective transform_*.cpp files):")
            current_stem = None
            for gram_stem, stub in pending_stubs:
                if gram_stem != current_stem:
                    print(f"\n       // transform_{gram_stem}.cpp")
                    current_stem = gram_stem
                for line in stub.splitlines():
                    print(f"       {line}")
                print()
            step += 1

        if not sig_mismatches and not pending_stubs:
            print(f"\n  {step}. All user-implemented body stubs already found in transform_*.cpp files.")
            step += 1


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

    gram_files_to_gen = [
        # 'alter.gram'
        'analyze.gram',
        'attach.gram',
        'call.gram',
        'checkpoint.gram',
        # 'comment.gram',
        # 'common.gram',
        'connect.gram',
        # 'copy.gram',
        # 'create_index.gram',
        # 'create_macro.gram',
        'create_schema.gram',
        'create_secret.gram',
        # 'create_sequence.gram',
        # 'create_table.gram',
        # 'create_trigger.gram',
        # 'create_type.gram',
        'create_view.gram',
        'deallocate.gram',
        # 'delete.gram',
        # 'describe.gram',
        'detach.gram',
        # 'drop.gram',
        'execute.gram',
        'explain.gram',
        'export.gram',
        # 'expression.gram',
        # 'insert.gram',
        'load.gram',
        # 'merge_into.gram',
        # 'pivot.gram',
        'pragma.gram',
        'prepare.gram',
        # 'select.gram',
        # 'set.gram',
        'transaction.gram',
        'update.gram',
        'use.gram',
        'vacuum.gram',
    ]
    rule_types, excluded_rules = load_grammar_types(type_dir / 'grammar_types.yml')
    results = [process_gram_file(f, rule_types, excluded_rules) for f in gram_files_to_gen]

    if args.write:
        all_declarations = [d for r in results for d in r.declarations]
        write_hpp(all_declarations)
        all_implementations = [impl for r in results for impl in r.implementations]
        all_registrations = [reg for r in results for reg in r.registrations]
        write_cpp(all_implementations, all_registrations)
        print_manual_steps(results)
    else:
        for r in results:
            print(f"\n{'=' * 60}")
            print(f"  {r.gram_stem}.gram")
            print(f"{'=' * 60}")
            print_output(r)


if __name__ == "__main__":
    main()
