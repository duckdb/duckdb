#include <utility>
#include <vector>

#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ConjunctionExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	ErrorData error;
	for (idx_t i = 0; i < expr.children.size(); i++) {
		BindChild(expr.children[i], depth, error);
	}
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// the children have been successfully resolved
	// cast the input types to boolean (if necessary)
	// and construct the bound conjunction expression
	auto result = make_uniq<BoundConjunctionExpression>(expr.GetExpressionType());
	for (auto &child_expr : expr.children) {
		auto &child = BoundExpression::GetExpression(*child_expr);
		result->children.push_back(BoundCastExpression::AddCastToType(context, std::move(child), LogicalType::BOOLEAN));
	}
	// now create the bound conjunction expression
	return BindResult(std::move(result));
}

} // namespace duckdb
