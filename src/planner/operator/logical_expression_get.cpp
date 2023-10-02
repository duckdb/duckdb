#include "duckdb/planner/operator/logical_expression_get.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

vector<idx_t> LogicalExpressionGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalExpressionGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
