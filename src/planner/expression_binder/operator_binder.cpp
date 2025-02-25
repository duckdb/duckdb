#include "duckdb/planner/expression_binder/operator_binder.hpp"

#include "duckdb/planner/binder.hpp"

namespace duckdb {

OperatorBinder::OperatorBinder(Binder &binder, ClientContext &context, ExpressionType operator_type)
    : ExpressionBinder(binder, context, true), operator_type(operator_type) {
}

BindResult OperatorBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                         idx_t depth) {
	if (operator_type == ExpressionType::OPERATOR_TRY) {
		throw BinderException("aggregates are not allowed inside the TRY expression");
	}
	return ExpressionBinder::BindAggregate(expr, function, depth);
}

} // namespace duckdb
