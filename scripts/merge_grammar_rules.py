#!/usr/bin/env python3
"""
Generate a bison nonterminal that is the non-overlapping union of FIRST sets.

Parses the bison .output file (from --report=all) to extract rules,
computes FIRST sets, and generates a merged rule.

Usage:
  python3 merge_grammar_rules.py <grammar.y.tmp> <new_name> <nt1> <nt2> [...]
"""

import re
import sys
from collections import defaultdict


def parse_grammar_from_output(filename):
    """Parse rules from bison .output file's Grammar section."""
    rules = defaultdict(list)

    with open(filename) as f:
        text = f.read()

    # Find Grammar section
    m = re.search(r'\nGrammar\n', text)
    if not m:
        print("Error: 'Grammar' section not found in .output file", file=sys.stderr)
        sys.exit(1)

    grammar_text = text[m.end():]

    # Parse rules like:
    #   123 nonterminal: sym1 sym2 sym3
    #   124            | sym4 sym5
    current_nt = None
    for line in grammar_text.split('\n'):
        # Empty line or section header ends grammar
        if not line.strip():
            continue
        if line.strip() and not line[0].isspace() and not line[0].isdigit():
            break

        # Rule: "  123 name: sym sym sym"
        m = re.match(r'\s+\d+\s+(\w+):\s*(.*)', line)
        if m:
            current_nt = m.group(1)
            rhs = m.group(2).strip()
            if rhs and rhs != '/*' and rhs != 'empty':
                syms = [s for s in rhs.split()
                        if s not in ('%prec', '/*', 'empty', 'empty*/')]
                rules[current_nt].append(syms)
            else:
                rules[current_nt].append([])
            continue

        # Continuation: "  124            | sym sym"
        m = re.match(r'\s+\d+\s+\|\s*(.*)', line)
        if m and current_nt:
            rhs = m.group(1).strip()
            if rhs and rhs != '/*' and rhs != 'empty':
                syms = [s for s in rhs.split()
                        if s not in ('%prec', '/*', 'empty', 'empty*/')]
                rules[current_nt].append(syms)
            else:
                rules[current_nt].append([])

    return dict(rules)


def compute_first(nt, rules, cache=None, visiting=None):
    """Compute FIRST set of a nonterminal."""
    if cache is None:
        cache = {}
    if visiting is None:
        visiting = set()
    if nt in cache:
        return cache[nt]
    if nt in visiting:
        return set()
    if nt not in rules:
        return {nt}  # Terminal

    visiting.add(nt)
    result = set()
    for alt in rules[nt]:
        if not alt:
            continue
        sym = alt[0]
        if sym not in rules:
            result.add(sym)
        else:
            result |= compute_first(sym, rules, cache, visiting)
    visiting.discard(nt)
    cache[nt] = result
    return result


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <grammar_out.output> <new_name> <nt1> <nt2> [...]",
              file=sys.stderr)
        sys.exit(1)

    output_file = sys.argv[1]
    new_rule_name = sys.argv[2]
    nonterminals = sys.argv[3:]

    rules = parse_grammar_from_output(output_file)
    print(f"Parsed {len(rules)} nonterminals", file=sys.stderr)

    for nt in nonterminals:
        if nt not in rules:
            print(f"Error: '{nt}' not found", file=sys.stderr)
            sys.exit(1)

    cache = {}
    first_sets = []
    for nt in nonterminals:
        tokens = compute_first(nt, rules, cache)
        first_sets.append(tokens)
        print(f"FIRST({nt}) = {len(tokens)} tokens", file=sys.stderr)

    # First nt as-is, extras as individual tokens
    base_tokens = first_sets[0]
    extra = set()
    for s in first_sets[1:]:
        extra |= (s - base_tokens)

    extra_sorted = sorted(extra)

    print(f"/* Auto-generated: FIRST({' | '.join(nonterminals)}) */")
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
