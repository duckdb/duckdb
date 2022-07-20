#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

void LogicalExplain::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
