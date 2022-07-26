#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                     FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}
} // namespace duckdb
