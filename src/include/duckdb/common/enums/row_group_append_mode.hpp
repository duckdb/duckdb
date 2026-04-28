//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/row_group_append_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! Controls whether an append creates a new row group or reuses an existing one.
//! Values are ordered so that higher values take priority (ratchet semantics).
enum class RowGroupAppendMode : uint8_t {
	//! Append to the last existing row group if possible (default)
	APPEND_TO_EXISTING = 0,
	//! Suggest creating a new row group — may be ignored for tables with indexes
	SUGGEST_NEW = 1,
	//! Require creating a new row group — cannot be ignored
	REQUIRE_NEW = 2
};

} // namespace duckdb
