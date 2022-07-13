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

bool IsExistMainKeyTable(string &table_name, vector<TableCatalogEntry *> &unordered) {
	for (idx_t i = 0; i < unordered.size(); i++) {
		if (unordered[i]->name == table_name) {
			return true;
		}
	}
	return false;
}

void ScanForeignKeyTable(vector<TableCatalogEntry *> &ordered, vector<TableCatalogEntry *> &unordered,
                         bool move_only_pk_table) {
	for (auto i = unordered.begin(); i != unordered.end();) {
		auto table_entry = *i;
		bool move_to_ordered = true;
		for (idx_t j = 0; j < table_entry->constraints.size(); j++) {
			auto &cond = table_entry->constraints[j];
			if (cond->type == ConstraintType::FOREIGN_KEY) {
				auto &fk = (ForeignKeyConstraint &)*cond;
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

void ReorderTableEntries(vector<TableCatalogEntry *> &tables) {
	vector<TableCatalogEntry *> ordered;
	vector<TableCatalogEntry *> unordered = tables;
	ScanForeignKeyTable(ordered, unordered, true);
	while (!unordered.empty()) {
		ScanForeignKeyTable(ordered, unordered, false);
	}
	tables = ordered;
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
	auto &catalog = Catalog::GetCatalog(context);
	auto copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function->function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	// gather a list of all the tables
	vector<TableCatalogEntry *> tables;
	auto schemas = catalog.schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
			if (entry->type == CatalogType::TABLE_ENTRY) {
				tables.push_back((TableCatalogEntry *)entry);
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
	for (auto &table : tables) {
		auto info = make_unique<CopyInfo>();
		// we copy the options supplied to the EXPORT
		info->format = stmt.info->format;
		info->options = stmt.info->options;
		// set up the file name for the COPY TO

		auto exported_data = ExportedTableData();
		idx_t id = 0;
		while (true) {
			string id_suffix = id == 0 ? string() : "_" + to_string(id);
			if (table->schema->name == DEFAULT_SCHEMA) {
				info->file_path = fs.JoinPath(stmt.info->file_path,
				                              StringUtil::Format("%s%s.%s", SanitizeExportIdentifier(table->name),
				                                                 id_suffix, copy_function->function.extension));
			} else {
				info->file_path =
				    fs.JoinPath(stmt.info->file_path,
				                StringUtil::Format("%s_%s%s.%s", SanitizeExportIdentifier(table->schema->name),
				                                   SanitizeExportIdentifier(table->name), id_suffix,
				                                   copy_function->function.extension));
			}
			if (table_name_index.find(info->file_path) == table_name_index.end()) {
				// this name was not yet taken: take it
				table_name_index.insert(info->file_path);
				break;
			}
			id++;
		}
		info->is_from = false;
		info->schema = table->schema->name;
		info->table = table->name;

		exported_data.table_name = info->table;
		exported_data.schema_name = info->schema;
		exported_data.file_path = info->file_path;

		ExportedTableInfo table_info;
		table_info.entry = table;
		table_info.table_data = exported_data;
		exported_tables.data.push_back(table_info);
		id++;

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
	auto export_node = make_unique<LogicalExport>(copy_function->function, move(stmt.info), exported_tables);

	if (child_operator) {
		export_node->children.push_back(move(child_operator));
	}

	result.plan = move(export_node);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
