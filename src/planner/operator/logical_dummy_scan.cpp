#include "duckdb/planner/operator/logical_dummy_scan.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void LogicalDummyScan::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalDummyScan::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	return make_uniq<LogicalDummyScan>(table_index);
}

vector<idx_t> LogicalDummyScan::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalDummyScan::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
