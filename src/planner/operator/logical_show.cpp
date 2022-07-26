#include "duckdb/planner/operator/logical_show.hpp"

namespace duckdb {

void LogicalShow::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalShow::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                     FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}
} // namespace duckdb
