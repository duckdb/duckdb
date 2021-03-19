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
#include "duckdb/storage/table/segment_tree.hpp"

namespace duckdb {
class BaseStatistics;
class PersistentSegment;

class PersistentColumnData {
public:
	unique_ptr<BaseStatistics> stats;
	vector<unique_ptr<PersistentSegment>> data;
};

class PersistentTableData {
public:
	explicit PersistentTableData(idx_t column_count);
	~PersistentTableData();

	vector<PersistentColumnData> column_data;
	shared_ptr<SegmentTree> versions;
};

} // namespace duckdb
