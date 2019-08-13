//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/cast_rules.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"

namespace duckdb {
//! Contains a list of rules for casting
class CastRules {
public:
	//! Returns true if the "from" type can be implicitly casted to the "to" type
	static bool ImplicitCast(SQLType from, SQLType to);
};

} // namespace duckdb
