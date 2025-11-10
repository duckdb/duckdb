#!/usr/bin/env bash
set -euo pipefail
# Print each command before executing (optional, for debug)
# set -x

# Activate virtual environment
if [[ -d ".venv" ]]; then
  source .venv/bin/activate
else
  echo "Error: .venv directory not found"
  exit 1
fi

# Run grammar inlining with and without argument
GRAMMAR_FILE="extension/autocomplete/inline_grammar.py"
if [[ ! -f "$GRAMMAR_FILE" ]]; then
  echo "Error: $GRAMMAR_FILE not found"
  deactivate
  exit 1
fi

python "$GRAMMAR_FILE" --grammar-file
python "$GRAMMAR_FILE"

echo "Successfully build PEG grammar files"

# Deactivate virtual environment
deactivate