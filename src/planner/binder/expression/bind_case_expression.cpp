#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
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
	auto &check = (BoundExpression &)*expr.check;
	auto &result_if_true = (BoundExpression &)*expr.result_if_true;
	auto &result_if_false = (BoundExpression &)*expr.result_if_false;
	// add a cast to BOOLEAN in the CHECK condition
	check.expr = BoundCastExpression::AddCastToType(move(check.expr), check.sql_type, SQLType(SQLTypeId::BOOLEAN));
	// now obtain the result type of the input types
	auto return_type = MaxSQLType(result_if_true.sql_type, result_if_false.sql_type);
	// add casts (if necessary)
	result_if_true.expr =
	    BoundCastExpression::AddCastToType(move(result_if_true.expr), result_if_true.sql_type, return_type);
	result_if_false.expr =
	    BoundCastExpression::AddCastToType(move(result_if_false.expr), result_if_false.sql_type, return_type);
	// now create the bound case expression
	return BindResult(
	    make_unique<BoundCaseExpression>(move(check.expr), move(result_if_true.expr), move(result_if_false.expr)),
	    return_type);
}
