#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

void LogicalCopyToFile::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                           FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
