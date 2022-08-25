#include "duckdb/planner/operator/logical_execute.hpp"

namespace duckdb {

void LogicalExecute::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalExecute::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}
} // namespace duckdb
