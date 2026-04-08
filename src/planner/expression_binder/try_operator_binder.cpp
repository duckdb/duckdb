#include "duckdb/planner/expression_binder/try_operator_binder.hpp"

#include <memory>

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class AggregateFunctionCatalogEntry;
class Binder;
class ClientContext;
class ColumnRefExpression;
class FunctionExpression;
class ParsedExpression;

TryOperatorBinder::TryOperatorBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult TryOperatorBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                            idx_t depth) {
	throw BinderException("aggregates are not allowed inside the TRY expression");
}

bool TryOperatorBinder::TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression,
                                                 BindResult &result, unique_ptr<ParsedExpression> &expr_ptr) {
	if (!stored_binder) {
		return false;
	}
	return stored_binder->TryResolveAliasReference(colref, depth, root_expression, result, expr_ptr);
}

bool TryOperatorBinder::DoesColumnAliasExist(const ColumnRefExpression &colref) {
	if (!stored_binder) {
		return false;
	}
	return stored_binder->DoesColumnAliasExist(colref);
}

} // namespace duckdb
