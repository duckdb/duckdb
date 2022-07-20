#include "duckdb/planner/operator/logical_create_index.hpp"

namespace duckdb {

void LogicalCreateIndex::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
