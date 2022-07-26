#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

void LogicalUpdate::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalUpdate::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
