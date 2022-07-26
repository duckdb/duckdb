#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

void LogicalSetOperation::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalSetOperation::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                             FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}
} // namespace duckdb
