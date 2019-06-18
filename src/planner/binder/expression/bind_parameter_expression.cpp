#include "parser/expression/parameter_expression.hpp"
#include "planner/binder.hpp"
#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, index_t depth) {
	if (!binder.parameters) {
		throw BinderException("Parameter expressions are only allowed in PREPARE statements!");
	}
	auto bound_parameter = make_unique<BoundParameterExpression>(expr.parameter_nr);
	auto sql_type = bound_parameter->sql_type;
	binder.parameters->push_back(bound_parameter.get());
	return BindResult(move(bound_parameter), sql_type);
}
