#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

void LogicalSetOperation::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(column_count);
}

unique_ptr<LogicalOperator> LogicalSetOperation::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                             FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto column_count = reader.ReadRequired<idx_t>();
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	return unique_ptr<LogicalSetOperation>(new LogicalSetOperation(table_index, column_count, type));
}
} // namespace duckdb
