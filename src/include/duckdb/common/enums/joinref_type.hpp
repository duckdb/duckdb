//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/joinref_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Join Reference Types
//===--------------------------------------------------------------------===//
enum class JoinRefType : uint8_t {
	REGULAR,   // Explicit conditions
	NATURAL,   // Implied conditions
	CROSS,     // No condition
	POSITIONAL // Positional condition
};

} // namespace duckdb
