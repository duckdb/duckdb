#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::ANY:
		return "(" + child->ToString() + " " + ExpressionTypeToOperator(comparison_type) + " ANY(" +
		       subquery->ToString() + "))";
	case SubqueryType::EXISTS:
		return "EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::SCALAR:
		return "(" + subquery->ToString() + ")";
	default:
		throw InternalException("Unrecognized type for subquery");
	}
}

} // namespace duckdb
