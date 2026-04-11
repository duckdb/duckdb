#!/usr/bin/env python3
"""
Generate a bison nonterminal that is the non-overlapping union of FIRST sets.
Uses bison's XML output for 100% correct parsing.

Prerequisites:
  bison --xml=/tmp/grammar.xml -o /dev/null grammar.y.tmp

Usage:
  python3 merge_grammar_rules_xml.py <grammar.xml> <new_name> <nt1> <nt2> [...]

Example:
  python3 merge_grammar_rules_xml.py /tmp/grammar.xml \
      param_name_or_typename param_name Typename
"""

import sys
import xml.etree.ElementTree as ET
from collections import defaultdict


def parse_xml_grammar(filename):
    """Parse bison XML output, return rules and terminal/nonterminal sets."""
    tree = ET.parse(filename)
    root = tree.getroot()
    grammar = root.find('grammar')

    # Collect terminal and nonterminal names
    terminals_set = set()
    for term in grammar.find('terminals'):
        name = term.get('name', '')
        if name and name != '$end':
            terminals_set.add(name)

    nonterminals_set = set()
    for nt in grammar.find('nonterminals'):
        name = nt.get('name', '')
        if name:
            nonterminals_set.add(name)

    # Collect rules: lhs -> [rhs_symbols, ...]
    rules = defaultdict(list)
    for rule in grammar.find('rules'):
        lhs = rule.find('lhs').text.strip()
        rhs_syms = []
        rhs = rule.find('rhs')
        if rhs is not None:
            for sym in rhs.findall('symbol'):
                rhs_syms.append(sym.text.strip())
            # Handle empty RHS
            if rhs.find('empty') is not None:
                rhs_syms = []
        rules[lhs].append(rhs_syms)

    return dict(rules), terminals_set, nonterminals_set


def compute_first(nt, rules, terminals, cache=None, visiting=None):
    """Compute FIRST set (terminal tokens that can start a derivation)."""
    if cache is None:
        cache = {}
    if visiting is None:
        visiting = set()
    if nt in cache:
        return cache[nt]
    if nt in visiting:
        return set()
    if nt in terminals or nt not in rules:
        return {nt}

    visiting.add(nt)
    result = set()
    for alt in rules[nt]:
        if not alt:
            continue
        first_sym = alt[0]
        if first_sym in terminals:
            result.add(first_sym)
        else:
            result |= compute_first(first_sym, rules, terminals, cache, visiting)
    visiting.discard(nt)
    cache[nt] = result
    return result


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <grammar.xml> <new_name> <nt1> <nt2> [...]",
              file=sys.stderr)
        sys.exit(1)

    xml_file = sys.argv[1]
    new_rule_name = sys.argv[2]
    nonterminals = sys.argv[3:]

    rules, terminals, nt_set = parse_xml_grammar(xml_file)
    print(f"Parsed: {len(rules)} rules, {len(terminals)} terminals, "
          f"{len(nt_set)} nonterminals", file=sys.stderr)

    for nt in nonterminals:
        if nt not in rules:
            print(f"Error: '{nt}' not found in grammar", file=sys.stderr)
            sys.exit(1)

    cache = {}
    first_sets = []
    for nt in nonterminals:
        tokens = compute_first(nt, rules, terminals, cache)
        first_sets.append(tokens)
        print(f"FIRST({nt}) = {len(tokens)} tokens", file=sys.stderr)

    # First nt kept as-is; extras are individual tokens not in first nt's FIRST set
    base_tokens = first_sets[0]
    extra = set()
    for s in first_sets[1:]:
        extra |= (s - base_tokens)

    extra_sorted = sorted(extra)
    overlap = set()
    for s in first_sets[1:]:
        overlap |= (s & base_tokens)

    print(f"Extra tokens: {len(extra_sorted)}", file=sys.stderr)
    print(f"Overlapping tokens (handled by {nonterminals[0]}): {len(overlap)}",
          file=sys.stderr)

    # Generate bison rule
    print(f"/* Auto-generated: non-overlapping union of "
          f"FIRST({', '.join(nonterminals)}) */")
    print(f"/* {nonterminals[0]}: {len(base_tokens)} tokens, "
          f"{len(extra_sorted)} extra from {', '.join(nonterminals[1:])} */")
    print(f"{new_rule_name}:")
    print(f"\t\t{nonterminals[0]}")
    print(f"\t\t\t{{ $$ = $1; }}")
    for token in extra_sorted:
        print(f"\t\t| {token}")
        print(f"\t\t\t{{ $$ = pstrdup($1); }}")
    print(f"\t;")


if __name__ == '__main__':
    main()
