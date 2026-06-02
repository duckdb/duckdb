import argparse
import re
import sys
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent))
from inline_grammar import parse_peg_grammar, PEGTokenType
from generate_transformer import load_grammar_types, load_matcher_rule_overrides


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

scripts_dir = Path(__file__).parent.parent
src_dir = scripts_dir.parent / 'src'
peg_dir = src_dir / 'parser' / 'peg'
statements_dir = peg_dir / 'grammar' / 'statements'
type_dir = scripts_dir / 'parser'
transformer_dir = peg_dir / 'transformer'
include_peg_dir = src_dir / 'include' / 'duckdb' / 'parser' / 'peg' / 'transformer'
matcher_cpp_path = peg_dir / 'matcher.cpp'

GENERATED_HEADER = "// AUTO-GENERATED by scripts/parser/gen_transformer_v2.py -- DO NOT EDIT\n"


def load_identifier_override_rules(grammar_types_file):
    """Return matcher overrides that produce IdentifierParseResult."""
    matcher_overrides = load_matcher_rule_overrides(grammar_types_file)
    return {
        rule_name
        for rule_name, info in matcher_overrides.items()
        if isinstance(info, dict) and info.get("matcher") in ("identifier", "reserved_identifier")
    }


def to_snake_case(name):
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def manual_body_exists(gram_stem, rule_name):
    pattern = re.compile(rf'\bPEGTransformerFactory::Transform{re.escape(rule_name)}\s*\(')
    preferred_path = transformer_dir / f"transform_{gram_stem}.cpp"
    paths = [preferred_path] if preferred_path.exists() else []
    paths.extend(path for path in transformer_dir.glob("transform_*.cpp") if path != preferred_path)
    return any(pattern.search(path.read_text()) for path in paths)


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


def classify_choice_alternatives(alternatives, rule_types, excluded_rules, identifier_override_rules):
    """
    Split choice alternatives into three groups:
      - transformer_alts: names with a registered transformer (in rule_types)
      - identifier_alts:  names that are identifier overrides (produce IdentifierParseResult)
      - excluded_alts:    syntax-only alternatives with no semantic value
      - unknown_alts:     neither registered nor known overrides -- need manual handling
    Returns (transformer_alts, identifier_alts, excluded_alts, unknown_alts).
    """
    transformer_alts = []
    identifier_alts = []
    excluded_alts = []
    unknown_alts = []
    for ref in alternatives:
        assert isinstance(ref, ReferenceNode)
        name = ref.name
        if name in rule_types:
            transformer_alts.append(name)
        elif name in identifier_override_rules:
            identifier_alts.append(name)
        elif name in excluded_rules:
            excluded_alts.append(name)
        else:
            unknown_alts.append(name)
    return transformer_alts, identifier_alts, excluded_alts, unknown_alts


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


def generate_choice_internal_with_default_alternatives(rule_name, return_type, return_by_value, excluded_alts):
    """Choice wrapper where syntax-only alternatives map to a default-constructed result."""
    excluded_conditions = " || ".join([f'choice_result.name == "{alt}"' for alt in excluded_alts])
    return (
        f"unique_ptr<TransformResultValue> PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\tauto &choice_result = choice_pr.GetResult();\n"
        f"\t{return_type} result {{}};\n"
        f"\tif (!({excluded_conditions})) {{\n"
        f"\t\tresult = transformer.Transform<{return_type}>(choice_result);\n"
        f"\t}}\n" + _box_result(return_type, return_by_value) + f"}}\n"
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


def generate_choice_internal_with_typed_body(rule_name, return_type, return_by_value, child_type, child_by_value):
    """
    Internal for a choice rule where all alternatives have the same transform type,
    but the rule itself returns a different type. The generated wrapper extracts the
    child value and the manual body performs the type conversion.
    """
    arg = "std::move(child)" if child_by_value else "child"
    return (
        f"unique_ptr<TransformResultValue> PEGTransformerFactory::Transform{rule_name}Internal(\n"
        f"    PEGTransformer &transformer, ParseResult &parse_result) {{\n"
        f"\tauto &list_pr = parse_result.Cast<ListParseResult>();\n"
        f"\tauto &choice_pr = list_pr.Child<ChoiceParseResult>(0);\n"
        f"\tauto child = transformer.Transform<{child_type}>(choice_pr.GetResult());\n"
        f"\tauto result = Transform{rule_name}(transformer, {arg});\n"
        + _box_result(return_type, return_by_value)
        + f"}}\n"
    )


def generate_choice_body_declaration(rule_name, return_type):
    """Declaration for the manual body that handles identifier alternatives."""
    return (
        f"\tstatic {return_type} Transform{rule_name}" f"(PEGTransformer &transformer, ParseResult &choice_result);\n"
    )


def generate_typed_choice_body_declaration(rule_name, return_type, child_type, child_by_value):
    """Declaration for a manual choice body that receives the already-transformed child value."""
    child_param = f"{child_type} child" if child_by_value else f"const {child_type} &child"
    return f"\tstatic {return_type} Transform{rule_name}(PEGTransformer &transformer, {child_param});\n"


def generate_typed_choice_body_stub(rule_name, return_type, child_type, child_by_value):
    """Stub .cpp definition for a typed choice body that must be hand-implemented."""
    child_param = f"{child_type} child" if child_by_value else f"const {child_type} &child"
    return (
        f"{return_type} PEGTransformerFactory::Transform{rule_name}(PEGTransformer &transformer, {child_param}) {{\n"
        f"\tthrow NotImplementedException(\"Transform{rule_name}\");\n"
        f"}}\n"
    )


# ---------------------------------------------------------------------------
# Sequence-element classification
#
# Mirrors the per-token-type dispatch inside MatcherFactory::CreateMatcher()
# in matcher.cpp.  Each helper handles one matcher/parse-result kind:
#
#   _classify_recursive         matcher AST          -> recursively extracted scalar/vector<T>
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


class ExtractionKind(Enum):
    SKIP = auto()
    REFERENCE = auto()
    PARENS = auto()
    OPTIONAL = auto()
    LIST = auto()
    REPEAT = auto()
    SEQUENCE = auto()
    CHOICE = auto()


@dataclass
class ExtractionPlan:
    """Recursive description of how to extract one semantic value from a ParseResult."""

    kind: ExtractionKind
    cpp_type: str = ""
    var_name: str = ""
    by_value: bool = False
    child: "ExtractionPlan | None" = None
    child_index: int = 0
    identifier: bool = False


def _plan_extraction(node, rule_types, excluded_rules, identifier_override_rules):
    """Build a recursive extraction plan. Nested sequences must collapse to one semantic value."""
    if isinstance(node, LiteralNode):
        return ExtractionPlan(kind=ExtractionKind.SKIP)
    if isinstance(node, ReferenceNode):
        if node.name in identifier_override_rules:
            return ExtractionPlan(
                kind=ExtractionKind.REFERENCE, cpp_type="string", var_name=to_snake_case(node.name), identifier=True
            )
        if node.name in rule_types:
            return ExtractionPlan(
                kind=ExtractionKind.REFERENCE,
                cpp_type=rule_types[node.name].cpp_type,
                var_name=to_snake_case(node.name),
                by_value=_is_by_value(node.name, rule_types),
            )
        if node.name in excluded_rules:
            return ExtractionPlan(kind=ExtractionKind.SKIP)
        return None
    if isinstance(node, ParensNode):
        child = _plan_extraction(node.inner, rule_types, excluded_rules, identifier_override_rules)
        return _wrap_extraction(ExtractionKind.PARENS, child)
    if isinstance(node, OptionalNode):
        child = _plan_extraction(node.child, rule_types, excluded_rules, identifier_override_rules)
        return _wrap_extraction(ExtractionKind.OPTIONAL, child)
    if isinstance(node, (ListMacroNode, RepeatNode)):
        child_node = node.inner if isinstance(node, ListMacroNode) else node.child
        child = _plan_extraction(child_node, rule_types, excluded_rules, identifier_override_rules)
        if child is None or child.kind == ExtractionKind.SKIP:
            return None
        return ExtractionPlan(
            kind=ExtractionKind.LIST if isinstance(node, ListMacroNode) else ExtractionKind.REPEAT,
            cpp_type=f"vector<{child.cpp_type}>",
            var_name=child.var_name,
            by_value=child.by_value,
            child=child,
        )
    if isinstance(node, SequenceNode):
        semantic = []
        for idx, sequence_child in enumerate(node.children):
            child = _plan_extraction(sequence_child, rule_types, excluded_rules, identifier_override_rules)
            if child is None:
                return None
            if child.kind != ExtractionKind.SKIP:
                semantic.append((idx, child))
        if not semantic:
            return ExtractionPlan(kind=ExtractionKind.SKIP)
        if len(semantic) != 1:
            return None
        child_index, child = semantic[0]
        return ExtractionPlan(
            kind=ExtractionKind.SEQUENCE, child=child, child_index=child_index, **_plan_value_args(child)
        )
    if isinstance(node, ChoiceNode):
        alternatives = [
            _plan_extraction(alternative, rule_types, excluded_rules, identifier_override_rules)
            for alternative in node.alternatives
        ]
        if all(alternative is not None and alternative.kind == ExtractionKind.SKIP for alternative in alternatives):
            return ExtractionPlan(kind=ExtractionKind.SKIP)
        if any(alternative is None or alternative.kind != ExtractionKind.REFERENCE for alternative in alternatives):
            return None
        first = alternatives[0]
        if any(
            alternative.cpp_type != first.cpp_type or alternative.identifier != first.identifier
            for alternative in alternatives[1:]
        ):
            return None
        return ExtractionPlan(kind=ExtractionKind.CHOICE, child=first, **_plan_value_args(first))
    return None


def _plan_value_args(plan):
    """Copy the semantic properties preserved by transparent wrapper nodes."""
    return {"cpp_type": plan.cpp_type, "var_name": plan.var_name, "by_value": plan.by_value}


def _wrap_extraction(kind, child):
    """Wrap a child plan while preserving syntax-only and unsupported results."""
    if child is None or child.kind == ExtractionKind.SKIP:
        return child
    return ExtractionPlan(kind=kind, child=child, **_plan_value_args(child))


def _temp_name(target_name, suffix, depth):
    """Build readable temporary names while avoiding collisions in nested wrappers."""
    depth_suffix = f"_{depth}" if depth else ""
    return f"{target_name}_{suffix}{depth_suffix}"


def _emit_extraction(plan, source_expr, target_name=None, indent="\t", declare=True, depth=0):
    """Emit recursive extraction code for a plan rooted at source_expr."""
    target_name = target_name or plan.var_name
    if plan.kind == ExtractionKind.SKIP:
        return []
    if plan.kind == ExtractionKind.REFERENCE:
        if plan.identifier:
            value_expr = f"{source_expr}.Cast<IdentifierParseResult>().identifier"
        else:
            value_expr = f"transformer.Transform<{plan.cpp_type}>({source_expr})"
        prefix = "auto " if declare else ""
        return [f"{indent}{prefix}{target_name} = {value_expr};"]
    if plan.kind == ExtractionKind.PARENS:
        return _emit_extraction(
            plan.child, f"ExtractResultFromParens({source_expr})", target_name, indent, declare, depth
        )
    if plan.kind == ExtractionKind.SEQUENCE:
        list_expr = f"{source_expr}.Cast<ListParseResult>()"
        return _emit_extraction(
            plan.child, f"{list_expr}.GetChild({plan.child_index})", target_name, indent, declare, depth
        )
    if plan.kind == ExtractionKind.CHOICE:
        choice_expr = f"{source_expr}.Cast<ChoiceParseResult>()"
        return _emit_extraction(plan.child, f"{choice_expr}.GetResult()", target_name, indent, declare, depth)
    if plan.kind == ExtractionKind.OPTIONAL:
        opt_name = _temp_name(target_name, "opt", depth)
        lines = []
        if declare:
            lines.append(f"{indent}{plan.cpp_type} {target_name} {{}};")
        lines.extend(
            [
                f"{indent}auto &{opt_name} = {source_expr}.Cast<OptionalParseResult>();",
                f"{indent}if ({opt_name}.HasResult()) {{",
            ]
        )
        lines.extend(
            _emit_extraction(
                plan.child, f"{opt_name}.GetResult()", target_name, indent + "\t", declare=False, depth=depth + 1
            )
        )
        lines.append(f"{indent}}}")
        return lines
    if plan.kind in (ExtractionKind.LIST, ExtractionKind.REPEAT):
        item_name = _temp_name(target_name, "item", depth)
        value_name = _temp_name(target_name, "value", depth)
        lines = [f"{indent}{plan.cpp_type} {target_name};"] if declare else []
        if plan.kind == ExtractionKind.LIST:
            items_name = _temp_name(target_name, "items", depth)
            lines.append(f"{indent}auto {items_name} = ExtractParseResultsFromList({source_expr});")
            children_expr = items_name
        else:
            repeat_name = _temp_name(target_name, "repeat", depth)
            lines.append(f"{indent}auto &{repeat_name} = {source_expr}.Cast<RepeatParseResult>();")
            children_expr = f"{repeat_name}.GetChildren()"
        lines.append(f"{indent}for (auto &{item_name} : {children_expr}) {{")
        lines.extend(_emit_extraction(plan.child, f"{item_name}.get()", value_name, indent + "\t", depth=depth + 1))
        value_expr = f"std::move({value_name})" if plan.child.by_value else value_name
        lines.append(f"{indent}\t{target_name}.push_back({value_expr});")
        lines.append(f"{indent}}}")
        return lines
    raise ValueError(f"Unsupported extraction plan kind: {plan.kind}")


def _classify_recursive(node, idx, rule_types, excluded_rules, identifier_override_rules):
    """Classify nested matcher structures by recursively planning and emitting extraction."""
    plan = _plan_extraction(node, rule_types, excluded_rules, identifier_override_rules)
    if plan is None:
        return None
    if plan.kind == ExtractionKind.SKIP:
        return _classify_literal()
    return SeqElement(
        skip=False,
        var_name=plan.var_name,
        cpp_type=plan.cpp_type,
        by_value=plan.by_value,
        extraction_lines=_emit_extraction(plan, f"list_pr.GetChild({idx})"),
    )


def classify_sequence_element(child, idx, rule_types, excluded_rules, identifier_override_rules):
    """
    Classify one element of a SequenceNode.
    Mirrors the token-type switch in MatcherFactory::CreateMatcher().
    Returns SeqElement or None if the element cannot be auto-generated.
    """
    return _classify_recursive(child, idx, rule_types, excluded_rules, identifier_override_rules)


def classify_sequence_elements(children, rule_types, excluded_rules, identifier_override_rules):
    """
    Classify all children of a SequenceNode.
    Mirrors the token loop in MatcherFactory::CreateMatcher().
    Returns list of SeqElement, or None if any element cannot be classified.
    """
    elements = []
    seen = {}  # var_name -> occurrence count, for deduplication
    for idx, child in enumerate(children):
        elem = classify_sequence_element(child, idx, rule_types, excluded_rules, identifier_override_rules)
        if elem is None:
            return None
        if not elem.skip:
            count = seen.get(elem.var_name, 0)
            seen[elem.var_name] = count + 1
            if count > 0:
                old_name = elem.var_name
                new_name = f"{old_name}_{count}"
                elem.extraction_lines = [line.replace(old_name, new_name) for line in elem.extraction_lines]
                # For identifier rules the field access is always '.identifier'; restore it if renamed.
                if old_name == "identifier":
                    elem.extraction_lines = [
                        line.replace(f".{new_name}", ".identifier") for line in elem.extraction_lines
                    ]
                elem.var_name = new_name
        elements.append(elem)
    return elements


def _sequence_skip_reason(children, rule_types, excluded_rules, identifier_override_rules):
    """Return a specific reason string explaining why classify_sequence_elements failed."""
    for idx, child in enumerate(children):
        if classify_sequence_element(child, idx, rule_types, excluded_rules, identifier_override_rules) is not None:
            continue
        inner = child.child if isinstance(child, OptionalNode) else child
        if isinstance(inner, ReferenceNode):
            name = inner.name
            if name not in rule_types and name not in excluded_rules and name not in identifier_override_rules:
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


def generate_sequence_forward_internal(rule_name, return_type, return_by_value, elements):
    semantic = [e for e in elements if not e.skip]
    assert len(semantic) == 1
    elem = semantic[0]
    body = ["\tauto &list_pr = parse_result.Cast<ListParseResult>();"]
    body.extend(elem.extraction_lines)
    result_expr = f"std::move({elem.var_name})" if elem.by_value else elem.var_name
    body.append(f"\tauto result = {result_expr};")
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
    generated_rule_names: list  # names of rules that received an auto-generated Internal
    skipped: list  # (rule_name, reason) — nothing generated
    manual_bodies: list  # (rule_name, reason) — Internal generated, body is hand-written
    body_stubs: list  # cpp definition stubs for bodies that need hand-implementation


def collect_generated(gram_stem, rules, rule_types, excluded_rules, provided_rule_names, identifier_override_rules):
    """Classify all rules; return a GramFileResult."""
    declarations = []
    implementations = []
    registrations = []
    generated_rule_names = []
    skipped = []
    manual_bodies = []
    body_stubs = []

    for rule_name, rule in rules.items():
        if rule_name in provided_rule_names:
            skipped.append((rule_name, "provided by matcher_rule_overrides"))
            continue

        if rule_name in excluded_rules:
            skipped.append((rule_name, "in excluded_rules"))
            continue

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
            transformer_alts, identifier_alts, excluded_alts, unknown_alts = classify_choice_alternatives(
                ast.alternatives, rule_types, excluded_rules, identifier_override_rules
            )
            if unknown_alts:
                skipped.append((rule_name, f"choice has unknown alternatives: {unknown_alts}"))
                continue

            declarations.append(generate_internal_declaration(rule_name))
            registrations.append(generate_registration(rule_name))
            generated_rule_names.append(rule_name)

            if not identifier_alts and not excluded_alts:
                alt_types = {rule_types[alt].cpp_type for alt in transformer_alts}
                if len(alt_types) == 1 and return_type in alt_types:
                    implementations.append(generate_choice_internal_full(rule_name, return_type, return_by_value))
                elif len(alt_types) == 1:
                    child_type = next(iter(alt_types))
                    child_by_value = any(_is_by_value(alt, rule_types) for alt in transformer_alts)
                    declarations.append(
                        generate_typed_choice_body_declaration(rule_name, return_type, child_type, child_by_value)
                    )
                    implementations.append(
                        generate_choice_internal_with_typed_body(
                            rule_name, return_type, return_by_value, child_type, child_by_value
                        )
                    )
                    manual_bodies.append((rule_name, f"choice body; wraps alternative return type: {child_type}"))
                    body_stubs.append(
                        (rule_name, generate_typed_choice_body_stub(rule_name, return_type, child_type, child_by_value))
                    )
                else:
                    declarations.append(generate_choice_body_declaration(rule_name, return_type))
                    implementations.append(generate_choice_internal_with_body(rule_name, return_type, return_by_value))
                    manual_bodies.append((rule_name, f"choice body; alternative return types: {sorted(alt_types)}"))
                    body_stubs.append((rule_name, generate_choice_body_stub(rule_name, return_type)))
            elif excluded_alts and not identifier_alts:
                alt_types = {rule_types[alt].cpp_type for alt in transformer_alts}
                if len(alt_types) == 1 and return_type in alt_types:
                    implementations.append(
                        generate_choice_internal_with_default_alternatives(
                            rule_name, return_type, return_by_value, excluded_alts
                        )
                    )
                else:
                    declarations.append(generate_choice_body_declaration(rule_name, return_type))
                    implementations.append(generate_choice_internal_with_body(rule_name, return_type, return_by_value))
                    manual_bodies.append(
                        (
                            rule_name,
                            f"choice body; syntax-only alternatives: {excluded_alts}, alternative return types: {sorted(alt_types)}",
                        )
                    )
                    body_stubs.append((rule_name, generate_choice_body_stub(rule_name, return_type)))
            else:
                declarations.append(generate_choice_body_declaration(rule_name, return_type))
                implementations.append(generate_choice_internal_with_body(rule_name, return_type, return_by_value))
                reason_parts = []
                if identifier_alts:
                    reason_parts.append(f"identifier alternatives: {identifier_alts}")
                if excluded_alts:
                    reason_parts.append(f"syntax-only alternatives: {excluded_alts}")
                manual_bodies.append((rule_name, f"choice body; {', '.join(reason_parts)}"))
                body_stubs.append((rule_name, generate_choice_body_stub(rule_name, return_type)))
            continue

        # Normalize a single token or syntax-only choice to a one-element sequence so
        # the all-skip path generates a no-argument semantic body.
        ast_plan = _plan_extraction(ast, rule_types, excluded_rules, identifier_override_rules)
        if not isinstance(ast, (SequenceNode, ChoiceNode)) or (
            ast_plan is not None and ast_plan.kind == ExtractionKind.SKIP
        ):
            ast = SequenceNode([ast])

        if isinstance(ast, SequenceNode):
            elements = classify_sequence_elements(ast.children, rule_types, excluded_rules, identifier_override_rules)
            if elements is not None:
                declarations.append(generate_internal_declaration(rule_name))
                registrations.append(generate_registration(rule_name))
                generated_rule_names.append(rule_name)
                semantic = [e for e in elements if not e.skip]
                if (
                    len(semantic) == 1
                    and semantic[0].cpp_type == return_type
                    and not manual_body_exists(gram_stem, rule_name)
                ):
                    implementations.append(
                        generate_sequence_forward_internal(rule_name, return_type, return_by_value, elements)
                    )
                else:
                    declarations.append(generate_sequence_body_decl(rule_name, return_type, elements))
                    implementations.append(
                        generate_sequence_internal(rule_name, return_type, return_by_value, elements)
                    )
                    body_stubs.append((rule_name, generate_sequence_body_stub(rule_name, return_type, elements)))
                continue
            skipped.append(
                (rule_name, _sequence_skip_reason(ast.children, rule_types, excluded_rules, identifier_override_rules))
            )
            continue

        skipped.append((rule_name, "complex rule (has operators/choices/groups)"))

    return GramFileResult(
        gram_stem=gram_stem,
        declarations=declarations,
        implementations=implementations,
        registrations=registrations,
        generated_rule_names=generated_rule_names,
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
_MATCHER_START_BLOCK = _SEPARATOR + "\t// START GENERATED RULE OVERRIDES\n" + _SEPARATOR
_MATCHER_END_BLOCK = _SEPARATOR + "\t// END GENERATED RULE OVERRIDES\n" + _SEPARATOR


def _matcher_override_expr(rule_name, override):
    matcher = override.get("matcher")
    suggestion = override.get("suggestion")
    if matcher == "identifier":
        if suggestion:
            return f"allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::{suggestion}))"
    if matcher == "reserved_identifier":
        if suggestion:
            return f"allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::{suggestion}))"
    if matcher == "number_literal":
        return "allocator.Allocate(make_uniq<NumberLiteralMatcher>())"
    if matcher == "string_literal":
        return "allocator.Allocate(make_uniq<StringLiteralMatcher>())"
    if matcher == "operator":
        return "allocator.Allocate(make_uniq<OperatorMatcher>())"
    raise RuntimeError(f"Unsupported matcher_rule_overrides entry for {rule_name}: {override}")


def write_matcher_rule_overrides(matcher_overrides):
    content = matcher_cpp_path.read_text()
    start_idx = content.find(_MATCHER_START_BLOCK)
    if start_idx == -1:
        raise RuntimeError(f"Could not find START GENERATED RULE OVERRIDES marker in {matcher_cpp_path}")
    end_idx = content.find(_MATCHER_END_BLOCK, start_idx + len(_MATCHER_START_BLOCK))
    if end_idx == -1:
        raise RuntimeError(f"Could not find END GENERATED RULE OVERRIDES marker in {matcher_cpp_path}")

    lines = []
    for rule_name, override in matcher_overrides.items():
        if not isinstance(override, dict):
            raise RuntimeError(f"matcher_rule_overrides entry for {rule_name} must be a mapping")
        expr = _matcher_override_expr(rule_name, override)
        line = f'\tAddRuleOverride("{rule_name}", {expr});\n'
        if len(line.rstrip()) <= 120:
            lines.append(line)
        else:
            lines.append(f'\tAddRuleOverride("{rule_name}",\n')
            lines.append(f'\t                {expr});\n')

    block_end = end_idx + len(_MATCHER_END_BLOCK)
    generated_block = _MATCHER_START_BLOCK + "".join(lines) + _MATCHER_END_BLOCK
    new_content = content[:start_idx] + generated_block + content[block_end:]
    matcher_cpp_path.write_text(new_content)
    print(f"Updated {matcher_cpp_path}")


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
    """Check which body stubs are implemented in any transform_*.cpp file.

    Compares from PEGTransformerFactory:: onwards (normalized whitespace) so that return types
    split across lines and multi-line param lists are handled correctly.

    Returns (implemented, mismatched):
      implemented: set of rule_names whose signature exactly matches the stub
      mismatched:  set of rule_names that exist but only with a different signature
    """
    preferred_path = transformer_dir / f"transform_{gram_stem}.cpp"
    paths = [preferred_path] if preferred_path.exists() else []
    paths.extend(path for path in transformer_dir.glob("transform_*.cpp") if path != preferred_path)
    prefix = 'PEGTransformerFactory::'
    implemented = set()
    mismatched = set()
    for rule_name, stub_cpp in body_stubs:
        first_line = stub_cpp.split('\n')[0]
        expected = _norm_ws(first_line.rstrip('{').rstrip())
        expected_norm = expected[expected.find(prefix) :] if prefix in expected else expected
        found_mismatch = False
        for path in paths:
            actual = _extract_func_signature(path.read_text(), f'Transform{rule_name}')
            if actual is None:
                continue
            actual = _norm_ws(actual)
            actual_norm = actual[actual.find(prefix) :] if prefix in actual else actual
            if expected_norm == actual_norm:
                implemented.add(rule_name)
                found_mismatch = False
                break
            found_mismatch = True
        if found_mismatch and rule_name not in implemented:
            mismatched.add(rule_name)
    return implemented, mismatched


def _find_stale_manual_internals(gram_stem, generated_rule_names):
    """Return rules whose Internal is now auto-generated but still exists in transform_{gram_stem}.cpp."""
    cpp_path = transformer_dir / f"transform_{gram_stem}.cpp"
    if not cpp_path.exists():
        return []
    text = cpp_path.read_text()
    return [
        name
        for name in sorted(generated_rule_names)
        if re.search(rf'\bPEGTransformerFactory::Transform{re.escape(name)}Internal\s*\(', text)
    ]


def print_manual_steps(all_results):
    print("\nRemaining manual steps:")

    step = 1

    all_skipped = [
        (r.gram_stem, rule_name, reason)
        for r in all_results
        for rule_name, reason in r.skipped
        if not reason.startswith("provided by ")
    ]
    if all_skipped:
        print(f"\n  {step}. Skipped rules (not auto-generated):")
        current_stem = None
        for gram_stem, rule_name, reason in all_skipped:
            if gram_stem != current_stem:
                print(f"\n       {gram_stem}.gram:")
                current_stem = gram_stem
            print(f"         {rule_name}: {reason}")
        step += 1

    stale_by_stem = {}
    for r in all_results:
        if r.generated_rule_names:
            stale = _find_stale_manual_internals(r.gram_stem, r.generated_rule_names)
            if stale:
                stale_by_stem[r.gram_stem] = stale
    if stale_by_stem:
        print(f"\n  {step}. Stale manual Internal wrappers (now auto-generated; remove from transform_*.cpp):")
        for gram_stem, rule_names in sorted(stale_by_stem.items()):
            print(f"\n       transform_{gram_stem}.cpp:")
            for name in rule_names:
                print(f"         Transform{name}Internal")
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


def process_gram_file(gram_filename, rule_types, excluded_rules, provided_rule_names, identifier_override_rules):
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

    return collect_generated(
        gram_stem, rules, rule_types, excluded_rules, provided_rule_names, identifier_override_rules
    )


def main():
    arg_parser = argparse.ArgumentParser(description="Generate Internal transformer wrappers from grammar rules.")
    arg_parser.add_argument("--write", action="store_true", help="Write generated files to disk.")
    args = arg_parser.parse_args()

    gram_files_to_gen = [
        'alter.gram',
        'analyze.gram',
        'attach.gram',
        'call.gram',
        'checkpoint.gram',
        'comment.gram',
        'common.gram',
        'connect.gram',
        'copy.gram',
        'create_index.gram',
        'create_macro.gram',
        'create_schema.gram',
        'create_secret.gram',
        'create_sequence.gram',
        # 'create_table.gram',
        'create_trigger.gram',
        'create_type.gram',
        'create_view.gram',
        'deallocate.gram',
        'delete.gram',
        'describe.gram',
        'detach.gram',
        'drop.gram',
        'execute.gram',
        'explain.gram',
        'export.gram',
        # 'expression.gram',
        'insert.gram',
        'load.gram',
        'merge_into.gram',
        'pivot.gram',
        'pragma.gram',
        'prepare.gram',
        # 'select.gram',
        'set.gram',
        'transaction.gram',
        'update.gram',
        'use.gram',
        'vacuum.gram',
    ]
    grammar_types_file = type_dir / 'grammar_types.yml'
    matcher_overrides = load_matcher_rule_overrides(grammar_types_file)
    matcher_rule_names = set(matcher_overrides.keys())
    identifier_override_rules = load_identifier_override_rules(grammar_types_file)
    rule_types, excluded_rules = load_grammar_types(grammar_types_file)
    results = [
        process_gram_file(f, rule_types, excluded_rules, matcher_rule_names, identifier_override_rules)
        for f in gram_files_to_gen
    ]

    if args.write:
        all_declarations = [d for r in results for d in r.declarations]
        write_hpp(all_declarations)
        all_implementations = [impl for r in results for impl in r.implementations]
        all_registrations = [reg for r in results for reg in r.registrations]
        write_cpp(all_implementations, all_registrations)
        write_matcher_rule_overrides(matcher_overrides)
        print_manual_steps(results)
    else:
        for r in results:
            print(f"\n{'=' * 60}")
            print(f"  {r.gram_stem}.gram")
            print(f"{'=' * 60}")
            print_output(r)


if __name__ == "__main__":
    main()
