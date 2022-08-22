#include "duckdb/planner/operator/logical_pragma.hpp"

namespace duckdb {

void LogicalPragma::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalPragma::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(state.type));
}

} // namespace duckdb
