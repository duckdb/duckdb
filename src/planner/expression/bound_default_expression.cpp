#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void BoundDefaultExpression::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(ExpressionTypeToString(type));
}

} // namespace duckdb
