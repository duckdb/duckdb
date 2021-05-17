#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(ExportStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.enable_copy) {
		throw Exception("COPY TO is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// lookup the format in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function->function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	// gather a list of all the tables
	vector<TableCatalogEntry *> tables;
	Catalog::GetCatalog(context).schemas->Scan(context, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
			if (entry->type == CatalogType::TABLE_ENTRY) {
				tables.push_back((TableCatalogEntry *)entry);
			}
		});
	});

	// now generate the COPY statements for each of the tables
	auto &fs = FileSystem::GetFileSystem(context);
	unique_ptr<LogicalOperator> child_operator;
	for (auto &table : tables) {
		auto info = make_unique<CopyInfo>();
		// we copy the options supplied to the EXPORT
		info->format = stmt.info->format;
		info->options = stmt.info->options;
		// set up the file name for the COPY TO
		if (table->schema->name == DEFAULT_SCHEMA) {
			info->file_path = fs.JoinPath(stmt.info->file_path,
			                              StringUtil::Format("%s.%s", table->name, copy_function->function.extension));
		} else {
			info->file_path =
			    fs.JoinPath(stmt.info->file_path, StringUtil::Format("%s.%s.%s", table->schema->name, table->name,
			                                                         copy_function->function.extension));
		}
		info->is_from = false;
		info->schema = table->schema->name;
		info->table = table->name;

		// generate the copy statement and bind it
		CopyStatement copy_stmt;
		copy_stmt.info = move(info);

		auto copy_binder = Binder::CreateBinder(context);
		auto bound_statement = copy_binder->Bind(copy_stmt);
		if (child_operator) {
			// use UNION ALL to combine the individual copy statements into a single node
			auto copy_union =
			    make_unique<LogicalSetOperation>(GenerateTableIndex(), 1, move(child_operator),
			                                     move(bound_statement.plan), LogicalOperatorType::LOGICAL_UNION);
			child_operator = move(copy_union);
		} else {
			child_operator = move(bound_statement.plan);
		}
	}

	// try to create the directory, if it doesn't exist yet
	// a bit hacky to do it here, but we need to create the directory BEFORE the copy statements run
	if (!fs.DirectoryExists(stmt.info->file_path)) {
		fs.CreateDirectory(stmt.info->file_path);
	}

	// create the export node
	auto export_node = make_unique<LogicalExport>(copy_function->function, move(stmt.info));

	if (child_operator) {
		export_node->children.push_back(move(child_operator));
	}

	result.plan = move(export_node);
	return result;
}

} // namespace duckdb
