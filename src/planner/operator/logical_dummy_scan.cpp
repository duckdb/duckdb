#include "duckdb/planner/operator/logical_dummy_scan.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

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
