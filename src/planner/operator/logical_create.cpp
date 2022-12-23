#include "duckdb/planner/operator/logical_create.hpp"

namespace duckdb {

void LogicalCreate::Serialize(FieldWriter &writer) const {
	info->Serialize(writer.GetSerializer());
}

unique_ptr<LogicalOperator> LogicalCreate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = CreateInfo::Deserialize(reader.GetSource());

	auto schema_catalog_entry = Catalog::GetSchema(context, INVALID_CATALOG, info->schema, true);
	return make_unique<LogicalCreate>(state.type, move(info), schema_catalog_entry);
}

idx_t LogicalCreate::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
