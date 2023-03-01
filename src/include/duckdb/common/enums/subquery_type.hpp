//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/subquery_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subquery Types
//===--------------------------------------------------------------------===//
enum class SubqueryType : uint8_t {
	INVALID = 0,
	SCALAR = 1,     // Regular scalar subquery
	EXISTS = 2,     // EXISTS (SELECT...)
	NOT_EXISTS = 3, // NOT EXISTS(SELECT...)
	ANY = 4,        // x = ANY(SELECT...) OR x IN (SELECT...)
};

} // namespace duckdb
