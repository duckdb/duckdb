#include <memory>
#include <utility>

#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CollateExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	auto error = Bind(expr.child, depth);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	auto &child = BoundExpression::GetExpression(*expr.child);
	if (child->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (child->return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException(child->query_location, "collations are only supported for type varchar");
	}
	// Validate the collation, but don't use it
	auto collation_test = make_uniq_base<Expression, BoundConstantExpression>(Value(child->return_type));
	auto collation_type = LogicalType::VARCHAR_COLLATION(expr.collation);
	PushCollation(context, collation_test, collation_type);
	child->return_type = collation_type;
	return BindResult(std::move(child));
}

} // namespace duckdb
