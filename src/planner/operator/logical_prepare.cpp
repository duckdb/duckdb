#include "duckdb/planner/operator/logical_prepare.hpp"

namespace duckdb {

void LogicalPrepare::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalPrepare::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}

} // namespace duckdb
