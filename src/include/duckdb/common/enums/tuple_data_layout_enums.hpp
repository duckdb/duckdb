//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/tuple_data_layout_enums.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TupleDataValidityType : uint8_t {
	CAN_HAVE_NULL_VALUES = 0,
	CANNOT_HAVE_NULL_VALUES = 1,
};

enum class TupleDataNestednessType : uint8_t {
	TOP_LEVEL_LAYOUT = 0,
	NESTED_STRUCT_LAYOUT = 1,
};

} // namespace duckdb
