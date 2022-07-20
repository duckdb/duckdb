#include "duckdb/planner/operator/logical_pragma.hpp"

namespace duckdb {

void LogicalPragma::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
