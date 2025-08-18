#include "duckdb/parser/expression/measure_definition_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

// Creates a new measure type holding the definition expression and returns a constant value of that type
BindResult ExpressionBinder::BindExpression(MeasureDefinitionExpression &expr, idx_t depth) {
	if (!expr.measure_expression) {
		return BindResult(
		    BinderException::Unsupported(expr, "MEASURE_DEFINITION expression must have a measure expression"));
	}
	BindResult result = BindExpression(expr.measure_expression, depth, false);
	if (result.HasError()) {
		return result;
	}
	auto measure_type =
	    LogicalType::MEASURE_TYPE(result.expression->return_type, expr.alias, result.expression->Copy());
	return BindResult(make_uniq<BoundConstantExpression>(Value(measure_type)));
}

} // namespace duckdb
