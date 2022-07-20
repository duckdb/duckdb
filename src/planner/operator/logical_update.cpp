#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

void LogicalUpdate::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
