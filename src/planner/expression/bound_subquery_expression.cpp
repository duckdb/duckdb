#include "duckdb/planner/expression/bound_subquery_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundSubqueryExpression::BoundSubqueryExpression(LogicalType return_type)
    : Expression(ExpressionType::SUBQUERY, ExpressionClass::BOUND_SUBQUERY, std::move(return_type)) {
}

string BoundSubqueryExpression::ToString() const {
	return "SUBQUERY";
}

bool BoundSubqueryExpression::Equals(const BaseExpression &other_p) const {
	// equality between bound subqueries not implemented currently
	return false;
}

unique_ptr<Expression> BoundSubqueryExpression::Copy() const {
	throw SerializationException("Cannot copy BoundSubqueryExpression");
}

bool BoundSubqueryExpression::PropagatesNullValues() const {
	// TODO this can be optimized further by checking the actual subquery node
	return false;
}

bool BoundSubqueryExpression::IsVolatile() const {
	if (Expression::IsVolatile()) {
		return true;
	}
	return subquery.plan && subquery.plan->HasVolatileExpressions();
}

} // namespace duckdb
