#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalCTERef::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(cte_index);
	writer.WriteRegularSerializableList(chunk_types);
	writer.WriteList<string>(bound_columns);
}

unique_ptr<LogicalOperator> LogicalCTERef::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto cte_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto bound_columns = reader.ReadRequiredList<string>();
	return make_unique<LogicalCTERef>(table_index, cte_index, chunk_types, bound_columns);
}

vector<idx_t> LogicalCTERef::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
