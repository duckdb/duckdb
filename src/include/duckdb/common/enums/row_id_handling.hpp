//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/row_id_handling.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/windows_undefs.hpp"

namespace duckdb {

//! How a data-modifying operator handles a target row-id that appears more than once in its input (e.g. an
//! UPDATE ... FROM whose join matches a target row via several source rows). Decouples the duplicate-row-id
//! policy from the SQL syntax that produced it.
enum class RowIdHandling : uint8_t {
	//! Row-ids are assumed unique; do not deduplicate (keeps the lock-free path). Used by plain UPDATE.
	ASSUME_UNIQUE = 0,
	//! Deduplicate row-ids, keeping the first occurrence of each. Used by UPDATE ... FROM.
	KEEP_FIRST = 1,
	//! Raise a cardinality error when a row-id repeats. (MERGE enforces this at the outer MERGE operator.)
	ERROR = 2
};

} // namespace duckdb
