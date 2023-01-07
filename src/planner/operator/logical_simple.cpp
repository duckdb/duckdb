#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

void LogicalSimple::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalSimple::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}

idx_t LogicalSimple::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
