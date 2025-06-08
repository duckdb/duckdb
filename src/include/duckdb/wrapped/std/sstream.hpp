#pragma once

#include <sstream>

#define DUCKDB_WRAP_STD

namespace duckdb_wrapped {
namespace std {
using ::std::basic_stringstream;
using ::std::stringstream;
using ::std::wstringstream;
} // namespace std
} // namespace duckdb_wrapped
