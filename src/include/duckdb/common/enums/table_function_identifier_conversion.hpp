//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/table_function_identifier_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TableFunctionIdentifierConversion : uint8_t {
	DEFAULT = 0,
	ENABLE_IMPLICIT_STRING = 1,
	DISABLE_IMPLICIT_STRING = 2
};

} // namespace duckdb
