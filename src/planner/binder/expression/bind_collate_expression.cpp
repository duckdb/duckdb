#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(CollateExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	string error = Bind(&expr.child, depth);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto &child = (BoundExpression &)*expr.child;
	if (child.sql_type.id != SQLTypeId::VARCHAR) {
		throw BinderException("collations are only supported for type varchar");
	}
	child.sql_type.collation = expr.collation;
	return BindResult(move(child.expr), child.sql_type);
}
