#include "duckdb/execution/operator/schema/physical_create_trigger.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

SourceResultType PhysicalCreateTrigger::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->GetQualifiedName().Catalog());
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context.client, QualifiedName(info->GetQualifiedName().Catalog(),
	                                                                                 info->GetQualifiedName().Schema(),
	                                                                                 info->base_table->Table()));
	auto transaction = catalog.GetCatalogTransaction(context.client);
	table.CreateTrigger(transaction, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
