#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {


BoundStatement Binder::Bind(CopyDatabaseStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	auto &from_database = Catalog::GetCatalog(context, stmt.from_database);
	auto &to_database = Catalog::GetCatalog(context, stmt.to_database);

	auto from_schemas = from_database.GetSchemas(context);

	ExportEntries entries;
	PhysicalExport::ExtractEntries(context, from_schemas, entries);

	auto info = make_uniq<CopyDatabaseInfo>(from_database, to_database);

	// get a list of all schemas to copy over
	for(auto &schema_ref : from_schemas) {
		auto &schema = schema_ref.get().Cast<SchemaCatalogEntry>();
		auto create_info = schema.GetInfo();
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		info->schemas.push_back(std::move(create_info));
	}
	// get a list of all tables to copy over
	for(auto &table_ref : entries.tables) {
		auto &table = table_ref.get().Cast<TableCatalogEntry>();
		auto create_info = table.GetInfo();
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->tables.push_back(std::move(create_info));
	}

	auto copy_database = make_uniq<LogicalCopyDatabase>(std::move(info));

	result.plan = std::move(copy_database);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
