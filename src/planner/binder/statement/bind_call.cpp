#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

static optional_ptr<LogicalGet> FindTableFunctionGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		auto get = FindTableFunctionGet(*child);
		if (get) {
			return get;
		}
	}
	return nullptr;
}

BoundStatement Binder::Bind(CallStatement &stmt) {
	SelectStatement select_statement;
	auto select_node = make_uniq<SelectNode>();
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = std::move(stmt.function);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(table_function);
	select_statement.node = std::move(select_node);

	auto result = Bind(select_statement);
	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	// use the return type of the table function (if any) instead of the default query result
	if (result.plan) {
		auto get = FindTableFunctionGet(*result.plan);
		if (get) {
			properties.return_type = get->function.call_return_type;
		}
	}
	return result;
}

} // namespace duckdb
