#include "duckdb/planner/operator/logical_delim_join.hpp"

namespace duckdb {

void LogicalDelimJoin::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
