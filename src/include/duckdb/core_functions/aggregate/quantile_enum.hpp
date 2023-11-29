//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_enum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class QuantileSerializationType : uint8_t {
	NON_DECIMAL = 0,
	DECIMAL_DISCRETE,
	DECIMAL_DISCRETE_LIST,
	DECIMAL_CONTINUOUS,
	DECIMAL_CONTINUOUS_LIST
};

}
