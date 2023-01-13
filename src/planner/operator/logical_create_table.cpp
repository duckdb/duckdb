#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

void LogicalCreateTable::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*info);
}

unique_ptr<LogicalOperator> LogicalCreateTable::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto info = reader.ReadRequiredSerializable<BoundCreateTableInfo>(state.gstate);
	auto schema = info->schema;
	return make_unique<LogicalCreateTable>(schema, std::move(info));
}

idx_t LogicalCreateTable::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
