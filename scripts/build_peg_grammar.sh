#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

if ! command -v python3 &>/dev/null; then
  echo "Error: python3 not found"
  exit 1
fi

GRAMMAR_FILE="extension/autocomplete/inline_grammar.py"
if [[ ! -f "$GRAMMAR_FILE" ]]; then
  echo "Error: $GRAMMAR_FILE not found"
  exit 1
fi

python3 "$GRAMMAR_FILE" --grammar-file
python3 "$GRAMMAR_FILE"
echo "Successfully built PEG grammar files"

TRANSFORMER_SCRIPT="scripts/generate_peg_transformer.py"
if [[ ! -f "$TRANSFORMER_SCRIPT" ]]; then
  echo "Error: $TRANSFORMER_SCRIPT not found"
  exit 1
fi

echo ""
echo "--- Transformer Coverage Check ---"
python3 "$TRANSFORMER_SCRIPT" -q || true
