#include "parser/expression/case_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, uint32_t depth) {
	// first try to bind the children of the case expression
	string error;
	BindChild(expr.check, depth, error);
	BindChild(expr.result_if_true, depth, error);
	BindChild(expr.result_if_false, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	// now resolve the type of the result
	auto check = GetExpression(expr.check);
	auto result_if_true = GetExpression(expr.result_if_true);
	auto result_if_false = GetExpression(expr.result_if_false);
	// add a cast to BOOLEAN in the CHECK condition
	check = AddCastToType(move(check), SQLType(SQLTypeId::BOOLEAN));
	// now obtain the result type of the input types
	auto return_type = MaxSQLType(result_if_true->sql_type, result_if_false->sql_type);
	// add casts (if necessary)
	result_if_true = AddCastToType(move(result_if_true), return_type);
	result_if_false = AddCastToType(move(result_if_false), return_type);
	// now create the bound case expression
	return BindResult(make_unique<BoundCaseExpression>(move(check), move(result_if_true), move(result_if_false)));
}
