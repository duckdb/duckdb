#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAlter::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	if (info->type == AlterType::ALTER_DATABASE) {
		auto &db_info = info->Cast<AlterDatabaseInfo>();
		auto &db_manager = DatabaseManager::Get(context.client);

		switch (db_info.alter_database_type) {
		case AlterDatabaseType::RENAME_DATABASE: {
			auto &rename_info = db_info.Cast<RenameDatabaseInfo>();
			db_manager.RenameDatabase(context.client, db_info.catalog, rename_info.new_name, db_info.if_not_found);
			break;
		}
		default:
			throw InternalException("Unsupported ALTER DATABASE operation");
		}
	} else {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.Alter(context.client, *info);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
