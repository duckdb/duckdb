#include "duckdb/planner/expression/bound_subquery_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
using namespace std;

BoundSubqueryExpression::BoundSubqueryExpression(TypeId return_type, SQLType sql_type)
    : Expression(ExpressionType::SUBQUERY, ExpressionClass::BOUND_SUBQUERY, return_type, move(sql_type)) {
}

string BoundSubqueryExpression::ToString() const {
	return "SUBQUERY";
}

bool BoundSubqueryExpression::Equals(const BaseExpression *other_) const {
	// equality between bound subqueries not implemented currently
	return false;
}

unique_ptr<Expression> BoundSubqueryExpression::Copy() {
	throw SerializationException("Cannot copy BoundSubqueryExpression");
}

} // namespace duckdb
