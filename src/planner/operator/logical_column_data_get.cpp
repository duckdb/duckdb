#include "duckdb/planner/operator/logical_column_data_get.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types, ColumnDataCollection &to_scan)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index), collection(to_scan) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types,
                                           optionally_owned_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, chunk_types.size());
}

vector<idx_t> LogicalColumnDataGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalColumnDataGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
