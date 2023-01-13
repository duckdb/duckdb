#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalRecursiveCTE::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(column_count);
	writer.WriteField(union_all);
}

unique_ptr<LogicalOperator> LogicalRecursiveCTE::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto column_count = reader.ReadRequired<idx_t>();
	auto union_all = reader.ReadRequired<bool>();
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	return unique_ptr<LogicalRecursiveCTE>(new LogicalRecursiveCTE(table_index, column_count, union_all, state.type));
}

vector<idx_t> LogicalRecursiveCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
