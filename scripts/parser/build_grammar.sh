#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON:-python3}"

# Run grammar inlining with and without argument
GRAMMAR_FILE="scripts/parser/inline_grammar.py"
if [[ ! -f "$GRAMMAR_FILE" ]]; then
  echo "Error: $GRAMMAR_FILE not found"
  deactivate
  exit 1
fi

"$PYTHON_BIN" "$GRAMMAR_FILE" --grammar-file
"$PYTHON_BIN" "$GRAMMAR_FILE"

echo "Successfully built grammar files"

# Generate Internal transformer wrappers for auto-generatable grammar rules
GEN_TRANSFORMER_FILE="scripts/parser/generate_transformer.py"
if [[ ! -f "$GEN_TRANSFORMER_FILE" ]]; then
  echo "Error: $GEN_TRANSFORMER_FILE not found"
  exit 1
fi

"$PYTHON_BIN" "$GEN_TRANSFORMER_FILE" --write

echo "Successfully generated transformer wrappers"

GEN_TRAMPOLINE_FILE="scripts/parser/generate_transformer_trampoline.py"
if [[ ! -f "$GEN_TRAMPOLINE_FILE" ]]; then
  echo "Error: $GEN_TRAMPOLINE_FILE not found"
  exit 1
fi

"$PYTHON_BIN" "$GEN_TRAMPOLINE_FILE" --write

echo "Successfully generated trampoline transformer wrappers"

make format-parser-grammar
