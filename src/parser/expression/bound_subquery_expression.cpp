#include "parser/expression/bound_subquery_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> BoundSubqueryExpression::Copy() const {
	throw SerializationException("Cannot copy BoundSubqueryExpression");
}

void BoundSubqueryExpression::Serialize(Serializer &serializer) {
	throw SerializationException("Cannot serialize BoundSubqueryExpression");
}

bool BoundSubqueryExpression::Equals(const Expression *other_) const {
	// equality between bound subqueries not implemented currently
	return false;
}
