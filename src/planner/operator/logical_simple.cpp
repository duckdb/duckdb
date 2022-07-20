#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

void LogicalSimple::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
