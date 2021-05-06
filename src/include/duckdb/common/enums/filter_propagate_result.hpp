//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/filter_propagate_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class FilterPropagateResult : uint8_t {
	NO_PRUNING_POSSIBLE = 0,
	FILTER_ALWAYS_TRUE = 1,
	FILTER_ALWAYS_FALSE = 2,
	FILTER_TRUE_OR_NULL = 3,
	FILTER_FALSE_OR_NULL = 4
};

} // namespace duckdb
