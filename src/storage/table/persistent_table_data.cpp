#include "duckdb/storage/table/persistent_table_data.hpp"

namespace duckdb {

PersistentTableData::PersistentTableData(idx_t column_count) : total_rows(0), row_group_count(0) {
}

PersistentTableData::~PersistentTableData() {
}

} // namespace duckdb
