#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalMaterializedCTE::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalMaterializedCTE::Deserialize(LogicalDeserializationState &state,
                                                                FieldReader &reader) {
	auto result = unique_ptr<LogicalMaterializedCTE>(new LogicalMaterializedCTE());
	result->table_index = reader.ReadRequired<idx_t>();
	return std::move(result);
}

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
