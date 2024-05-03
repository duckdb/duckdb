#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

BoundStatement Binder::Bind(CallStatement &stmt) {
	BoundStatement result;

	TableFunctionRef ref;
	ref.function = std::move(stmt.function);

	auto bound_func = Bind(ref);
	auto &bound_table_func = bound_func->Cast<BoundTableFunction>();
	;
	auto &get = bound_table_func.get->Cast<LogicalGet>();
	D_ASSERT(get.returned_types.size() > 0);
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.column_ids.push_back(i);
	}

	result.types = get.returned_types;
	result.names = get.names;
	result.plan = CreatePlan(*bound_func);

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
