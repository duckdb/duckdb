#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

void LogicalInsert::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
