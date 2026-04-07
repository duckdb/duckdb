#include "duckdb/execution/operator/schema/physical_create_trigger.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

namespace duckdb {

SourceResultType PhysicalCreateTrigger::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	auto &table =
	    Catalog::GetEntry<TableCatalogEntry>(context.client, info->catalog, info->schema, info->base_table->table_name);
	auto &duck_table = table.Cast<DuckTableEntry>();
	auto transaction = catalog.GetCatalogTransaction(context.client);
	duck_table.CreateTrigger(transaction, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
