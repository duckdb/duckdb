#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {
using namespace std;

BoundStatement Binder::Bind(CallStatement &stmt) {
	BoundStatement result;

	TableFunctionRef ref;
	ref.function = move(stmt.function);

	auto bound_func = Bind(ref);
	auto &bound_table_func = (BoundTableFunction &)*bound_func;
	auto &get = (LogicalGet&) *bound_table_func.get;

	result.types = get.types;
	result.names = get.names;
	result.plan = CreatePlan(*bound_func);
	return result;
}

} // namespace duckdb
