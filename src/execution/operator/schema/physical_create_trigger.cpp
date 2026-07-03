#include "duckdb/execution/operator/schema/physical_create_trigger.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

SourceResultType PhysicalCreateTrigger::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->GetQualifiedName().Catalog());
	// The trigger inherits the catalog/schema of its base table, so reuse them to look up the table. The base table
	// reference preserves the name exactly as written, which may be a two-part `catalog.table` reference that would
	// otherwise be misread as `schema.table` here.
	auto &table = Catalog::GetEntry<TableCatalogEntry>(
	    context.client, QualifiedName(info->GetQualifiedName().Catalog(), info->GetQualifiedName().Schema(),
	                                  info->base_table->GetQualifiedName().Name()));
	auto transaction = catalog.GetCatalogTransaction(context.client);
	table.CreateTrigger(transaction, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
