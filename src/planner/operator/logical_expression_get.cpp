#include "duckdb/planner/operator/logical_expression_get.hpp"

namespace duckdb {

void LogicalExpressionGet::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
