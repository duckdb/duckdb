#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAlter::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	// Edge case where we are modifying the metadata of a database
	if (info->GetCatalogType() == CatalogType::DATABASE_ENTRY) {
		D_ASSERT(info->type == AlterType::SET_COMMENT);
		auto &db_manager = DatabaseManager::Get(context.client);
		db_manager.AlterDatabase(context.client, *info);
		return SourceResultType::FINISHED;
	}

	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.Alter(context.client, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
