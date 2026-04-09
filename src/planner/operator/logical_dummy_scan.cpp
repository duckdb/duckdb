#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/common/projection_index.hpp"

namespace duckdb {
class ClientContext;

LogicalDummyScan::LogicalDummyScan(TableIndex table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN), table_index(table_index) {
}

vector<ColumnBinding> LogicalDummyScan::GetColumnBindings() {
	return {ColumnBinding(table_index, ProjectionIndex(0))};
}

idx_t LogicalDummyScan::EstimateCardinality(ClientContext &context) {
	return 1;
}
vector<TableIndex> LogicalDummyScan::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalDummyScan::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
