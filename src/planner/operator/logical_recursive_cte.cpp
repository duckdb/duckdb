#include "duckdb/planner/operator/logical_recursive_cte.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void LogicalRecursiveCTE::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(column_count);
	writer.WriteField(union_all);
}

unique_ptr<LogicalOperator> LogicalRecursiveCTE::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto result = unique_ptr<LogicalRecursiveCTE>(new LogicalRecursiveCTE());
	result->table_index = reader.ReadRequired<idx_t>();
	result->column_count = reader.ReadRequired<idx_t>();
	result->union_all = reader.ReadRequired<bool>();
	return std::move(result);
}

vector<idx_t> LogicalRecursiveCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalRecursiveCTE::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
