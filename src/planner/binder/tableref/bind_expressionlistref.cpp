#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace std;

namespace duckdb {

unique_ptr<LogicalOperator> Binder::Bind(ExpressionListRef &expr) {
	// construct the current root as an empty LogicalGet
	// this is done only so there is a root node in case there are subqueries inside the expressions
	auto root = make_unique_base<LogicalOperator, LogicalGet>(0);

	vector<vector<unique_ptr<Expression>>> expressions;
	vector<SQLType> expr_types;
	vector<string> expr_names;
	if (expr.expected_types.size() > 0) {
		expr_types = expr.expected_types;
		expr_names = expr.expected_names;
	}
	// bind value list
	InsertBinder binder(*this, context);
	binder.target_type = SQLType(SQLTypeId::INVALID);
	for (idx_t list_idx = 0; list_idx < expr.values.size(); list_idx++) {
		auto &expression_list = expr.values[list_idx];
		// first we bind all of the expressions
		vector<unique_ptr<Expression>> list;
		if (expr_types.size() == 0) {
			// for the first list, we set the expected types as the types of these expressions
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				SQLType result_type;
				auto expr = binder.Bind(expression_list[val_idx], &result_type);
				expr_types.push_back(result_type);
				expr_names.push_back("col" + to_string(val_idx));
				list.push_back(move(expr));
			}
		} else {
			// for subsequent lists, we apply the expected types we found in the first list
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				binder.target_type = expr_types[val_idx];
				list.push_back(binder.Bind(expression_list[val_idx]));
			}
		}
		// plan any subqueries inside the expression list
		for(auto &expr : list) {
			PlanSubqueries(&expr, &root);
		}
		expressions.push_back(move(list));
	}
	// generate the bind index and add the entry to the bind context
	auto bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(bind_index, expr.alias, expr_names, expr_types);

	// now create a LogicalExpressionGet from the set of expressions
	vector<TypeId> types;
	for (auto &expr : expressions[0]) {
		types.push_back(expr->return_type);
	}
	auto expr_get = make_unique<LogicalExpressionGet>(bind_index, types, move(expressions));
	expr_get->AddChild(move(root));
	return move(expr_get);
}

}
