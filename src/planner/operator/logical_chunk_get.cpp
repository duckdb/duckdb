#include "duckdb/planner/operator/logical_chunk_get.hpp"

namespace duckdb {

void LogicalChunkGet::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
