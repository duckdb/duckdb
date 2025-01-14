//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/quantile_enum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class QuantileSerializationType : uint8_t {
	NON_DECIMAL = 0,
	DECIMAL_DISCRETE,
	DECIMAL_DISCRETE_LIST,
	DECIMAL_CONTINUOUS,
	DECIMAL_CONTINUOUS_LIST
};

}
