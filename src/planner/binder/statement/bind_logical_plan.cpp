#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include <algorithm>

namespace duckdb {

idx_t GetMaxTableIndex(LogicalOperator &op) {
	idx_t result = 0;
	for (auto &child : op.children) {
		auto max_child_index = GetMaxTableIndex(*child);
		result = MaxValue<idx_t>(result, max_child_index);
	}
	auto indexes = op.GetTableIndex();
	for (auto &index : indexes) {
		result = MaxValue<idx_t>(result, index);
	}
	return result;
}

BoundStatement Binder::Bind(LogicalPlanStatement &stmt) {
	BoundStatement result;
	result.types = stmt.plan->types;
	for (idx_t i = 0; i < result.types.size(); i++) {
		result.names.push_back(StringUtil::Format("col%d", i));
	}
	result.plan = std::move(stmt.plan);

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::ALLOW_STREAMING;
	properties.return_type = StatementReturnType::QUERY_RESULT; // TODO could also be something else

	if (parent) {
		throw InternalException("LogicalPlanStatement should be bound in root binder");
	}
	global_binder_state->bound_tables = GetMaxTableIndex(*result.plan) + 1;
	return result;
}

} // namespace duckdb
