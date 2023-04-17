#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalCTE::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalCTE::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	return unique_ptr<LogicalCTE>(new LogicalCTE(table_index));
}

vector<idx_t> LogicalCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
