#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(LogicalPlanStatement &stmt) {
	BoundStatement result;
	result.types = stmt.plan->types;
	for (idx_t i = 0; i < result.types.size(); i++) {
		result.names.push_back(StringUtil::Format("col%d", i));
	}
	result.plan = std::move(stmt.plan);
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT; // TODO could also be something else

	return result;
}

} // namespace duckdb
