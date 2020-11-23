//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/persistent_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class BaseStatistics;
class PersistentSegment;

class PersistentTableData {
public:
	PersistentTableData(idx_t column_count);
	~PersistentTableData();

	vector<unique_ptr<BaseStatistics>> column_stats;
	vector<vector<unique_ptr<PersistentSegment>>> table_data;
};

} // namespace duckdb
