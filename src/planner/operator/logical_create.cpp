#include "duckdb/planner/operator/logical_create.hpp"

#include <memory>
#include <utility>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class SchemaCatalogEntry;

LogicalCreate::LogicalCreate(LogicalOperatorType type, unique_ptr<CreateInfo> info,
                             optional_ptr<SchemaCatalogEntry> schema)
    : LogicalOperator(type), schema(schema), info(std::move(info)) {
}

LogicalCreate::LogicalCreate(LogicalOperatorType type, ClientContext &context, unique_ptr<CreateInfo> info_p)
    : LogicalOperator(type), info(std::move(info_p)) {
	this->schema = Catalog::GetSchema(context, info->catalog, info->schema, OnEntryNotFound::RETURN_NULL);
}

idx_t LogicalCreate::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalCreate::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
