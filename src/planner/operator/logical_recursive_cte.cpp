#include "duckdb/planner/operator/logical_recursive_cte.hpp"

namespace duckdb {

void LogicalRecursiveCTE::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
