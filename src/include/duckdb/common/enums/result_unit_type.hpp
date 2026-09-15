//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/result_unit_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! What a piece of query output holds, and what a unit cast is checked against
enum class ResultUnitType : uint8_t {
	//! A DataChunk
	CHUNK
};

} // namespace duckdb
