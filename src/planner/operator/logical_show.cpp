#include "duckdb/planner/operator/logical_show.hpp"

namespace duckdb {

void LogicalShow::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
