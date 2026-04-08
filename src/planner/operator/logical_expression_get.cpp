#include "duckdb/planner/operator/logical_expression_get.hpp"

namespace duckdb {

vector<TableIndex> LogicalExpressionGet::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalExpressionGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
