//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_prompt_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "shell_state.hpp"
namespace duckdb_shell {
string get_prompt(const string &prompt, ShellState &state);
}
