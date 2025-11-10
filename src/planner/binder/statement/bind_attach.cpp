#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

BoundStatement Binder::Bind(AttachStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// bind the options
	TableFunctionBinder option_binder(*this, context, "Attach", "Attach parameter");
	unordered_map<string, Value> kv_options;
	for (auto &entry : stmt.info->parsed_options) {
		auto bound_expr = option_binder.Bind(entry.second);
		auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr);
		if (val.IsNull()) {
			throw BinderException("NULL is not supported as a valid option for ATTACH option \"" + entry.first + "\"");
		}
		stmt.info->options[entry.first] = std::move(val);
	}
	stmt.info->parsed_options.clear();

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ATTACH, std::move(stmt.info));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
