#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

BoundStatement Binder::Bind(CallStatement &stmt) {
	SelectStatement select_statement;
	auto select_node = make_uniq<SelectNode>();
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = std::move(stmt.function);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(table_function);
	select_statement.node = std::move(select_node);

	// Bind as `SELECT * FROM func()` - which already propagates the table function's call_return_type
	// into the statement return type (see Binder::Bind(SelectStatement)) - then force materialization,
	// the extra behavior CALL requires on top of the shared passthrough handling.
	auto result = Bind(select_statement);
	GetStatementProperties().output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	return result;
}

} // namespace duckdb
