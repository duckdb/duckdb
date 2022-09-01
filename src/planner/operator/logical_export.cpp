#include "duckdb/planner/operator/logical_export.hpp"

namespace duckdb {

void LogicalExport::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalExport::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}

} // namespace duckdb
