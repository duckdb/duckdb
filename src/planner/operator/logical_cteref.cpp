#include "duckdb/planner/operator/logical_cteref.hpp"

namespace duckdb {

void LogicalCTERef::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalCTERef::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
