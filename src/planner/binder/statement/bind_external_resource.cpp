#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/external_resource_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/planner/operator/logical_external_resource.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExternalResourceStatement &stmt) {
	if (stmt.operation == ExternalResourceOperation::SHOW) {
		// SHOW [ALL] EXTERNAL RESOURCES desugars to `SELECT * FROM duckdb_external_resources(all := <all>)`.
		vector<unique_ptr<ParsedExpression>> children;
		if (stmt.all) {
			auto all_expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
			all_expr->SetAlias("discover");
			children.push_back(std::move(all_expr));
		}
		auto table_function = make_uniq<TableFunctionRef>();
		table_function->function = make_uniq<FunctionExpression>("duckdb_external_resources", std::move(children));

		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq<StarExpression>());
		select_node->from_table = std::move(table_function);
		SelectStatement select_statement;
		select_statement.node = std::move(select_node);

		auto show_result = Bind(select_statement);
		GetStatementProperties().output_type = QueryResultOutputType::FORCE_MATERIALIZED;
		return show_result;
	}

	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	TableFunctionBinder binder(*this, context, "External resource", "External resource parameter");
	auto eval = [&](unique_ptr<ParsedExpression> &expr) {
		auto bound = binder.Bind(expr);
		return ExpressionExecutor::EvaluateScalar(context, *bound);
	};

	BoundExternalResource data;
	data.operation = stmt.operation;
	data.name = stmt.name.GetIdentifierName();

	// A durable resource is registered under its name, so every verb needs one.
	if (data.name.empty()) {
		throw BinderException("EXTERNAL RESOURCE requires a name (AS <name>)");
	}

	if (stmt.operation != ExternalResourceOperation::DESTROY) {
		// CREATE / REGISTER: the resource type is a string literal; only the create params need resolving.
		data.type = stmt.type;
		for (auto &entry : stmt.options) {
			data.params[entry.first] = eval(entry.second);
		}
		if (stmt.operation == ExternalResourceOperation::REGISTER) {
			data.handle = eval(stmt.handle);
			if (data.handle.IsNull()) {
				throw BinderException("REGISTER EXTERNAL RESOURCE: the handle must not be NULL");
			}
		}
	}

	result.plan = make_uniq<LogicalExternalResource>(std::move(data));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
