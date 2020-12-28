#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	auto bound_parameter = make_unique<BoundParameterExpression>(expr.parameter_nr);
	binder.parameters->push_back(bound_parameter.get());
	return BindResult(move(bound_parameter));
}

} // namespace duckdb
