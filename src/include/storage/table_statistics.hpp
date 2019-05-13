//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

struct TableStatistics {
	count_t estimated_cardinality;
};

} // namespace duckdb
