#include "duckdb/planner/operator/logical_set.hpp"

namespace duckdb {

void LogicalSet::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
