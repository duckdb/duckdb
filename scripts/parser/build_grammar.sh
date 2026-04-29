#!/usr/bin/env bash
set -euo pipefail

# Run grammar inlining with and without argument
GRAMMAR_FILE="scripts/parser/inline_grammar.py"
if [[ ! -f "$GRAMMAR_FILE" ]]; then
  echo "Error: $GRAMMAR_FILE not found"
  deactivate
  exit 1
fi

python "$GRAMMAR_FILE" --grammar-file
python "$GRAMMAR_FILE"

echo "Successfully build PEG grammar files"