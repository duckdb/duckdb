#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalSetOperation::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(column_count);
}

unique_ptr<LogicalOperator> LogicalSetOperation::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto column_count = reader.ReadRequired<idx_t>();
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	return unique_ptr<LogicalSetOperation>(new LogicalSetOperation(table_index, column_count, state.type));
}

vector<idx_t> LogicalSetOperation::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
