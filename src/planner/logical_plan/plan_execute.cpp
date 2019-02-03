#include "main/client_context.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/parameter_expression.hpp"
#include "parser/statement/execute_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"
#include "planner/operator/logical_execute.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(ExecuteStatement &statement) {
	auto *prep = (PreparedStatementCatalogEntry *)context.prepared_statements->GetEntry(context.ActiveTransaction(),
	                                                                                    statement.name);
	if (!prep || prep->deleted) {
		throw Exception("Could not find prepared statement with that name");
	}
	// set parameters
	if (statement.values.size() != prep->parameter_expression_map.size()) {
		throw Exception("Parameter/argument count mismatch");
	}
	size_t param_idx = 1;
	for (auto &expr : statement.values) {
		auto it = prep->parameter_expression_map.find(param_idx);
		if (it == prep->parameter_expression_map.end() || it->second == nullptr) {
			throw Exception("Could not find parameter with this index");
		}
		ParameterExpression *param_expr = it->second;
		switch (expr->type) {
		case ExpressionType::VALUE_CONSTANT: {
			auto const_expr = (ConstantExpression *)expr.get();
			assert(const_expr);
			auto val = const_expr->value;
			if (param_expr->return_type != val.type) {
				val = val.CastAs(param_expr->return_type);
			}
			param_expr->value = val;
			break;
		}
		default:
			throw Exception("Expression type");
		}

		param_idx++;
	}

	// all set, execute
	auto execute = make_unique<LogicalExecute>(prep);
	root = move(execute);
}
