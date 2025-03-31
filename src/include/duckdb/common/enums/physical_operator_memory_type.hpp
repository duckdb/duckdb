//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_operator_memory_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class PhysicalOperatorMemoryType : uint8_t {
	DESTROY,
	DO_NOT_DESTROY,
};

} // namespace duckdb
