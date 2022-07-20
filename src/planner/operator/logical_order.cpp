#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
