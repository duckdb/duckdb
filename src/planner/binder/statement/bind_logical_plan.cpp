#include <string>
#include <utility>
#include <vector>

#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

TableIndex GetMaxTableIndex(LogicalOperator &op) {
	TableIndex result(0);
	for (auto &child : op.children) {
		auto max_child_index = GetMaxTableIndex(*child);
		result = MaxValue<TableIndex>(result, max_child_index);
	}
	auto indexes = op.GetTableIndex();
	for (auto &index : indexes) {
		result = MaxValue<TableIndex>(result, index);
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
	global_binder_state->bound_tables = GetMaxTableIndex(*result.plan).index + 1;
	return result;
}

} // namespace duckdb
