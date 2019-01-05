#include "optimizer/rule/constant_cast.hpp"

using namespace duckdb;
using namespace std;


ConstantCastRule::ConstantCastRule() {
	auto cast = make_unique<CastExpressionMatcher>();
	cast->child = make_unique<ConstantExpressionMatcher>();
	root = move(cast);
}

unique_ptr<Expression> ConstantCastRule::Apply(vector<Expression*> &bindings, bool &fixed_point) {
	auto cast_expr = (CastExpression*) bindings[0];
	auto const_expr = (ConstantExpression*) bindings[1];
	return make_unique<ConstantExpression>(const_expr->value.CastAs(cast_expr->return_type));
}
