#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	D_ASSERT(expr.parameter_nr > 0);
	auto bound_parameter = make_unique<BoundParameterExpression>(expr.parameter_nr);
	if (!binder.parameters) {
		throw std::runtime_error("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	binder.parameters->push_back(bound_parameter.get());
	if (binder.parameter_types && expr.parameter_nr <= binder.parameter_types->size()) {
		bound_parameter->return_type = (*binder.parameter_types)[expr.parameter_nr - 1];
	}
	return BindResult(move(bound_parameter));
}

} // namespace duckdb
