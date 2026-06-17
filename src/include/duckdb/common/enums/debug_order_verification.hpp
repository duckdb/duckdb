//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_order_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! How ORDER BY results should be verified (debug setting)
enum class DebugOrderVerification : uint8_t {
	//! No verification (default) - ORDER BY is evaluated normally
	NONE = 0,
	//! Replace ORDER BY x, y, ... with ORDER BY create_sort_key(x, y, ...) to verify the sort key
	//! produces the same ordering as the regular comparison
	CREATE_SORT_KEY = 1,
	//! Cast every ORDER BY expression to VARIANT (ORDER BY x::VARIANT, y::VARIANT, ...) to verify that
	//! the VARIANT comparator produces the same ordering as the regular comparison
	VARIANT = 2
};

} // namespace duckdb
