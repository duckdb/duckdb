#include "duckdb/planner/operator/logical_delete.hpp"

namespace duckdb {

void LogicalDelete::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
