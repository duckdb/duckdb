#include "duckdb/planner/operator/logical_export.hpp"

namespace duckdb {

void LogicalExport::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalExport::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
