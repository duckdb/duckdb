#include "duckdb/planner/operator/logical_limit_percent.hpp"

namespace duckdb {

void LogicalLimitPercent::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
