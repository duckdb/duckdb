#include <string>
#include <unordered_map>
#include <utility>

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundStatement Binder::Bind(AttachStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// resolve the path expression if set by the parser
	if (stmt.info->parsed_path) {
		TableFunctionBinder path_binder(*this, context, "Attach", "Attach path");
		auto bound_path = path_binder.Bind(stmt.info->parsed_path);
		auto path_val = ExpressionExecutor::EvaluateScalar(context, *bound_path);
		if (path_val.IsNull()) {
			throw BinderException("ATTACH path expression must not evaluate to NULL");
		}
		stmt.info->path = path_val.ToString();
		stmt.info->parsed_path.reset();
	}

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
