//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct TableStatistics {
	idx_t estimated_cardinality;
};

} // namespace duckdb
