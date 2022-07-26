#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                      FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
