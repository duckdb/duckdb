#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

void LogicalDummyScan::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
