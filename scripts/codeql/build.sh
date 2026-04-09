#!/usr/bin/env bash
set -eux -o pipefail

export CODEQL_HOME="$HOME/dev/codeql"

# git clone https://github.com/github/codeql.git

# Clean previous codeql build.
rm -rf build/codeql
mkdir -p build/codeql

# CMAKE_LLVM_PATH=/opt/homebrew/opt/llvm"
# arch -x86_64

codeql database create build/codeql/db-cpp \
  --language=cpp \
  --command="make codeql"
