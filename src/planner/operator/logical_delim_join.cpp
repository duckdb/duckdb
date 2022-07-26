#include "duckdb/planner/operator/logical_delim_join.hpp"

namespace duckdb {

void LogicalDelimJoin::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalDelimJoin::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                          FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
