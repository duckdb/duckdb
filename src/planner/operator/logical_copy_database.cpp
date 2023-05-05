#include "duckdb/planner/operator/logical_copy_database.hpp"

namespace duckdb {

void LogicalCopyDatabase::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalCopyDatabase::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}

} // namespace duckdb
