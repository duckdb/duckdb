#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

PersistentTableData::PersistentTableData(idx_t column_count) {
	column_data.resize(column_count);
}

PersistentTableData::~PersistentTableData() {
}

PersistentColumnData::~PersistentColumnData() {
}

} // namespace duckdb
