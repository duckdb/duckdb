#include "duckdb/planner/expression_binder/try_operator_binder.hpp"

#include "duckdb/planner/binder.hpp"

namespace duckdb {

TryOperatorBinder::TryOperatorBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult TryOperatorBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                            idx_t depth) {
	throw BinderException("aggregates are not allowed inside the TRY expression");
}

} // namespace duckdb
