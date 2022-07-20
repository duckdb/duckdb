#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
