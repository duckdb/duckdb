#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {

void LogicalDelimGet::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
