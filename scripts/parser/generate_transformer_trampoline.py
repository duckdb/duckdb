import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from inline_grammar import parse_peg_grammar
from transformer_trampoline_config import load_transformer_trampoline_config


scripts_dir = Path(__file__).resolve().parents[1]
src_dir = scripts_dir.parent / "src"
statements_dir = src_dir / "parser" / "peg" / "grammar" / "statements"
type_dir = scripts_dir / "parser"


def load_grammar_rules(gram_files):
    rules = {}
    for gram_file in gram_files:
        gram_path = statements_dir / gram_file
        try:
            parsed_rules = parse_peg_grammar(gram_path.read_text())
        except Exception as e:
            raise Exception(f"{gram_file}: {e}") from None
        for rule_name, rule in parsed_rules.items():
            if rule_name in rules:
                raise Exception(f"duplicate grammar rule '{rule_name}' in {gram_file}")
            rules[rule_name] = rule
    return rules


def main():
    arg_parser = argparse.ArgumentParser(description="Generate trampoline-style transformer wrappers.")
    arg_parser.add_argument("--write", action="store_true", help="Write generated files to disk.")
    arg_parser.add_argument(
        "--grammar",
        action="append",
        default=["use.gram"],
        help="Grammar file to process. Defaults to use.gram. Can be specified multiple times.",
    )
    args = arg_parser.parse_args()

    if args.write:
        raise NotImplementedError("Writing trampoline transformer output is not implemented yet")

    grammar_files = sorted(set(args.grammar))
    rules = load_grammar_rules(grammar_files)
    all_grammar_files = sorted(path.name for path in statements_dir.glob("*.gram"))
    all_rules = load_grammar_rules(all_grammar_files)
    config_file = type_dir / "transformer_trampoline.yml"
    rule_config = load_transformer_trampoline_config(config_file, all_rules.keys())

    print("// Trampoline transformer generation preview")
    print(f"// grammar files: {', '.join(grammar_files)}")
    print(f"// rules: {len(rules)}")
    for rule_name in sorted(rules):
        config = rule_config.get(rule_name)
        mode = config.mode.value if config else "generated"
        print(f"// {rule_name}: {mode}")


if __name__ == "__main__":
    main()
