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

echo "Successfully built grammar files"

# Generate Internal transformer wrappers for auto-generatable grammar rules
GEN_TRANSFORMER_FILE="scripts/parser/gen_transformer_v2.py"
if [[ ! -f "$GEN_TRANSFORMER_FILE" ]]; then
  echo "Error: $GEN_TRANSFORMER_FILE not found"
  exit 1
fi

python "$GEN_TRANSFORMER_FILE" --write

echo "Successfully generated transformer wrappers"

make format-fix