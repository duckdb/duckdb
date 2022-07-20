#include "duckdb/planner/operator/logical_prepare.hpp"

namespace duckdb {

void LogicalPrepare::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
