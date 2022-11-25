#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {

void LogicalDelimGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(chunk_types);
}

unique_ptr<LogicalOperator> LogicalDelimGet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	return make_unique<LogicalDelimGet>(table_index, chunk_types);
}

vector<idx_t> LogicalDelimGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
