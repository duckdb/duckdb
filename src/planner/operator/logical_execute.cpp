#include "duckdb/planner/operator/logical_execute.hpp"

namespace duckdb {

void LogicalExecute::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
