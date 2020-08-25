//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct QualifiedName {
	string schema;
	string name;
};

} // namespace duckdb
