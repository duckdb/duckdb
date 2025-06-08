#pragma once

#include <cctype>
#include <locale>

#define DUCKDB_WRAP_STD

namespace duckdb_wrapped {
namespace std {
using ::std::isspace;
} // namespace std
} // namespace duckdb_wrapped
