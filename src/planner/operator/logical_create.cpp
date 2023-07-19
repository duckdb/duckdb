#include "duckdb/planner/operator/logical_create.hpp"

namespace duckdb {

LogicalCreate::LogicalCreate(LogicalOperatorType type, unique_ptr<CreateInfo> info,
                             optional_ptr<SchemaCatalogEntry> schema)
    : LogicalOperator(type), schema(schema), info(std::move(info)) {
}

LogicalCreate::LogicalCreate(LogicalOperatorType type, ClientContext &context, unique_ptr<CreateInfo> info_p)
    : LogicalOperator(type), info(std::move(info_p)) {
	this->schema = Catalog::GetSchema(context, info->catalog, info->schema, OnEntryNotFound::RETURN_NULL);
}

void LogicalCreate::Serialize(FieldWriter &writer) const {
	info->Serialize(writer.GetSerializer());
}

unique_ptr<LogicalOperator> LogicalCreate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = CreateInfo::Deserialize(reader.GetSource());

	auto schema_catalog_entry = Catalog::GetSchema(context, info->catalog, info->schema, OnEntryNotFound::RETURN_NULL);
	return make_uniq<LogicalCreate>(state.type, std::move(info), schema_catalog_entry);
}

idx_t LogicalCreate::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalCreate::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
