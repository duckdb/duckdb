#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

void LogicalDummyScan::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalDummyScan::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                          FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
