#include "duckdb/planner/operator/logical_cteref.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void LogicalCTERef::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteField(cte_index);
	writer.WriteRegularSerializableList(chunk_types);
	writer.WriteList<string>(bound_columns);
	writer.WriteField(materialized_cte);
}

unique_ptr<LogicalOperator> LogicalCTERef::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto cte_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto bound_columns = reader.ReadRequiredList<string>();
	auto materialized_cte = reader.ReadField<CTEMaterialize>(CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return make_uniq<LogicalCTERef>(table_index, cte_index, chunk_types, bound_columns, materialized_cte);
}

vector<idx_t> LogicalCTERef::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalCTERef::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
