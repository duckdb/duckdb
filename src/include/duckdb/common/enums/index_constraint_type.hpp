//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/index_constraint_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Index Constraint Types
//===--------------------------------------------------------------------===//
enum class IndexConstraintType : uint8_t {
	NONE = 0,    // index is an index don't built to any constraint
	UNIQUE = 1,  // index is an index built to enforce a UNIQUE constraint
	PRIMARY = 2, // index is an index built to enforce a PRIMARY KEY constraint
	FOREIGN = 3  // index is an index built to enforce a FOREIGN KEY constraint
};

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//
// NOTE: deprecated. Still necessary to read older duckdb files.
enum class DeprecatedIndexType : uint8_t {
	INVALID = 0,    // invalid index type
	ART = 1,        // Adaptive Radix Tree
	EXTENSION = 100 // Extension index
};

} // namespace duckdb
