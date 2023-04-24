#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/parser/parsed_data/exported_table_data.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/string_util.hpp"
#include <algorithm>

namespace duckdb {

//! Sanitizes a string to have only low case chars and underscores
string SanitizeExportIdentifier(const string &str) {
	// Copy the original string to result
	string result(str);

	for (idx_t i = 0; i < str.length(); ++i) {
		auto c = str[i];
		if (c >= 'a' && c <= 'z') {
			// If it is lower case just continue
			continue;
		}

		if (c >= 'A' && c <= 'Z') {
			// To lowercase
			result[i] = tolower(c);
		} else {
			// Substitute to underscore
			result[i] = '_';
		}
	}

	return result;
}

bool IsExistMainKeyTable(string &table_name, vector<reference<TableCatalogEntry>> &unordered) {
	for (idx_t i = 0; i < unordered.size(); i++) {
		if (unordered[i].get().name == table_name) {
			return true;
		}
	}
	return false;
}

void ScanForeignKeyTable(vector<reference<TableCatalogEntry>> &ordered, vector<reference<TableCatalogEntry>> &unordered,
                         bool move_only_pk_table) {
	for (auto i = unordered.begin(); i != unordered.end();) {
		auto table_entry = *i;
		bool move_to_ordered = true;
		auto &constraints = table_entry.get().GetConstraints();
		for (idx_t j = 0; j < constraints.size(); j++) {
			auto &cond = constraints[j];
			if (cond->type == ConstraintType::FOREIGN_KEY) {
				auto &fk = cond->Cast<ForeignKeyConstraint>();
				if ((move_only_pk_table && fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) ||
				    (!move_only_pk_table && fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE &&
				     IsExistMainKeyTable(fk.info.table, unordered))) {
					move_to_ordered = false;
					break;
				}
			}
		}
		if (move_to_ordered) {
			ordered.push_back(table_entry);
			i = unordered.erase(i);
		} else {
			i++;
		}
	}
}

void ReorderTableEntries(vector<reference<TableCatalogEntry>> &tables) {
	vector<reference<TableCatalogEntry>> ordered;
	vector<reference<TableCatalogEntry>> unordered = tables;
	ScanForeignKeyTable(ordered, unordered, true);
	while (!unordered.empty()) {
		ScanForeignKeyTable(ordered, unordered, false);
	}
	tables = ordered;
}

string CreateFileName(const string &id_suffix, TableCatalogEntry &table, const string &extension) {
	auto name = SanitizeExportIdentifier(table.name);
	if (table.schema->name == DEFAULT_SCHEMA) {
		return StringUtil::Format("%s%s.%s", name, id_suffix, extension);
	}
	auto schema = SanitizeExportIdentifier(table.schema->name);
	return StringUtil::Format("%s_%s%s.%s", schema, name, id_suffix, extension);
}

BoundStatement Binder::Bind(ExportStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY TO is disabled through configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// lookup the format in the catalog
	auto copy_function =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function->function.copy_to_bind && !copy_function->function.plan) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	// gather a list of all the tables
	string catalog = stmt.database.empty() ? INVALID_CATALOG : stmt.database;
	vector<reference<TableCatalogEntry>> tables;
	auto schemas = Catalog::GetSchemas(context, catalog);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
			if (entry->type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry->Cast<TableCatalogEntry>());
			}
		});
	}

	// reorder tables because of foreign key constraint
	ReorderTableEntries(tables);

	// now generate the COPY statements for each of the tables
	auto &fs = FileSystem::GetFileSystem(context);
	unique_ptr<LogicalOperator> child_operator;

	BoundExportData exported_tables;

	unordered_set<string> table_name_index;
	for (auto &t : tables) {
		auto &table = t.get();
		auto info = make_uniq<CopyInfo>();
		// we copy the options supplied to the EXPORT
		info->format = stmt.info->format;
		info->options = stmt.info->options;
		// set up the file name for the COPY TO

		idx_t id = 0;
		while (true) {
			string id_suffix = id == 0 ? string() : "_" + to_string(id);
			auto name = CreateFileName(id_suffix, table, copy_function->function.extension);
			auto directory = stmt.info->file_path;
			auto full_path = fs.JoinPath(directory, name);
			info->file_path = full_path;
			auto insert_result = table_name_index.insert(info->file_path);
			if (insert_result.second == true) {
				// this name was not yet taken: take it
				break;
			}
			id++;
		}
		info->is_from = false;
		info->catalog = catalog;
		info->schema = table.schema->name;
		info->table = table.name;

		// We can not export generated columns
		for (auto &col : table.GetColumns().Physical()) {
			info->select_list.push_back(col.GetName());
		}

		ExportedTableData exported_data;
		exported_data.database_name = catalog;
		exported_data.table_name = info->table;
		exported_data.schema_name = info->schema;

		exported_data.file_path = info->file_path;

		ExportedTableInfo table_info(table, std::move(exported_data));
		exported_tables.data.push_back(table_info);
		id++;

		// generate the copy statement and bind it
		CopyStatement copy_stmt;
		copy_stmt.info = std::move(info);

		auto copy_binder = Binder::CreateBinder(context, this);
		auto bound_statement = copy_binder->Bind(copy_stmt);
		if (child_operator) {
			// use UNION ALL to combine the individual copy statements into a single node
			auto copy_union =
			    make_uniq<LogicalSetOperation>(GenerateTableIndex(), 1, std::move(child_operator),
			                                   std::move(bound_statement.plan), LogicalOperatorType::LOGICAL_UNION);
			child_operator = std::move(copy_union);
		} else {
			child_operator = std::move(bound_statement.plan);
		}
	}

	// try to create the directory, if it doesn't exist yet
	// a bit hacky to do it here, but we need to create the directory BEFORE the copy statements run
	if (!fs.DirectoryExists(stmt.info->file_path)) {
		fs.CreateDirectory(stmt.info->file_path);
	}

	// create the export node
	auto export_node = make_uniq<LogicalExport>(copy_function->function, std::move(stmt.info), exported_tables);

	if (child_operator) {
		export_node->children.push_back(std::move(child_operator));
	}

	result.plan = std::move(export_node);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
