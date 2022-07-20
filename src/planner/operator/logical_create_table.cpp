#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

void LogicalCreateTable::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
