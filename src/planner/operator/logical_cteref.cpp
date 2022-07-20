#include "duckdb/planner/operator/logical_cteref.hpp"

namespace duckdb {

void LogicalCTERef::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
