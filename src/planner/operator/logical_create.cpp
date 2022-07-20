#include "duckdb/planner/operator/logical_create.hpp"

namespace duckdb {

void LogicalCreate::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
