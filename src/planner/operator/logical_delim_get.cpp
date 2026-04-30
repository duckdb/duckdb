#include "duckdb/planner/operator/logical_delim_get.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

vector<TableIndex> LogicalDelimGet::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalDelimGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
