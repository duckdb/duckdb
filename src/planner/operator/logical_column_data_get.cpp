#include "duckdb/planner/operator/logical_column_data_get.hpp"

#include <utility>

#include "duckdb/common/assert.hpp"

namespace duckdb {

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
}

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           ColumnDataCollection &to_scan)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index), collection(to_scan) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
}

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           optionally_owned_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, chunk_types.size());
}

vector<TableIndex> LogicalColumnDataGet::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalColumnDataGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
