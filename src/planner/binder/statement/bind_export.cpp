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
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

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

bool ReferencedTableIsOrdered(string &referenced_table, catalog_entry_vector_t &ordered) {
	for (auto &entry : ordered) {
		auto &table_entry = entry.get().Cast<TableCatalogEntry>();
		if (StringUtil::CIEquals(table_entry.name, referenced_table)) {
			// The referenced table is already ordered
			return true;
		}
	}
	return false;
}

void ScanForeignKeyTable(catalog_entry_vector_t &ordered, catalog_entry_vector_t &unordered,
                         bool move_primary_keys_only) {
	catalog_entry_vector_t remaining;

	for (auto &entry : unordered) {
		auto &table_entry = entry.get().Cast<TableCatalogEntry>();
		bool move_to_ordered = true;
		auto &constraints = table_entry.GetConstraints();

		for (auto &cond : constraints) {
			if (cond->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = cond->Cast<ForeignKeyConstraint>();
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}

			if (move_primary_keys_only) {
				// This table references a table, don't move it yet
				move_to_ordered = false;
				break;
			} else if (!ReferencedTableIsOrdered(fk.info.table, ordered)) {
				// The table that it references isn't ordered yet
				move_to_ordered = false;
				break;
			}
		}

		if (move_to_ordered) {
			ordered.push_back(table_entry);
		} else {
			remaining.push_back(table_entry);
		}
	}
	unordered = remaining;
}

void ReorderTableEntries(catalog_entry_vector_t &tables) {
	catalog_entry_vector_t ordered;
	catalog_entry_vector_t unordered = tables;
	// First only move the tables that don't have any dependencies
	ScanForeignKeyTable(ordered, unordered, true);
	while (!unordered.empty()) {
		// Now we will start moving tables that have foreign key constraints
		// if the tables they reference are already moved
		ScanForeignKeyTable(ordered, unordered, false);
	}
	tables = ordered;
}

string CreateFileName(const string &id_suffix, TableCatalogEntry &table, const string &extension) {
	auto name = SanitizeExportIdentifier(table.name);
	if (table.schema.name == DEFAULT_SCHEMA) {
		return StringUtil::Format("%s%s.%s", name, id_suffix, extension);
	}
	auto schema = SanitizeExportIdentifier(table.schema.name);
	return StringUtil::Format("%s_%s%s.%s", schema, name, id_suffix, extension);
}

static bool IsSupported(CopyTypeSupport support_level) {
	// For export purposes we don't want to lose information, so we only accept fully supported types
	return support_level == CopyTypeSupport::SUPPORTED;
}

static LogicalType AlterLogicalType(const LogicalType &original, copy_supports_type_t type_check) {
	D_ASSERT(type_check);
	auto id = original.id();
	switch (id) {
	case LogicalTypeId::LIST: {
		auto child = AlterLogicalType(ListType::GetChildType(original), type_check);
		return LogicalType::LIST(child);
	}
	case LogicalTypeId::STRUCT: {
		auto &original_children = StructType::GetChildTypes(original);
		child_list_t<LogicalType> new_children;
		for (auto &child : original_children) {
			auto &child_name = child.first;
			auto &child_type = child.second;

			LogicalType new_type;
			if (!IsSupported(type_check(child_type))) {
				new_type = AlterLogicalType(child_type, type_check);
			} else {
				new_type = child_type;
			}
			new_children.push_back(std::make_pair(child_name, new_type));
		}
		return LogicalType::STRUCT(std::move(new_children));
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(original);
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < member_count; i++) {
			auto &child_name = UnionType::GetMemberName(original, i);
			auto &child_type = UnionType::GetMemberType(original, i);

			LogicalType new_type;
			if (!IsSupported(type_check(child_type))) {
				new_type = AlterLogicalType(child_type, type_check);
			} else {
				new_type = child_type;
			}

			new_children.push_back(std::make_pair(child_name, new_type));
		}
		return LogicalType::UNION(std::move(new_children));
	}
	case LogicalTypeId::MAP: {
		auto &key_type = MapType::KeyType(original);
		auto &value_type = MapType::ValueType(original);

		LogicalType new_key_type;
		LogicalType new_value_type;
		if (!IsSupported(type_check(key_type))) {
			new_key_type = AlterLogicalType(key_type, type_check);
		} else {
			new_key_type = key_type;
		}

		if (!IsSupported(type_check(value_type))) {
			new_value_type = AlterLogicalType(value_type, type_check);
		} else {
			new_value_type = value_type;
		}
		return LogicalType::MAP(new_key_type, new_value_type);
	}
	default: {
		D_ASSERT(!IsSupported(type_check(original)));
		return LogicalType::VARCHAR;
	}
	}
}

static bool NeedsCast(LogicalType &type, copy_supports_type_t type_check) {
	if (!type_check) {
		return false;
	}
	if (IsSupported(type_check(type))) {
		// The type is supported in it's entirety, no cast is required
		return false;
	}
	// Change the type to something that is supported
	type = AlterLogicalType(type, type_check);
	return true;
}

static unique_ptr<QueryNode> CreateSelectStatement(CopyStatement &stmt, child_list_t<LogicalType> &select_list,
                                                   copy_supports_type_t type_check) {
	auto ref = make_uniq<BaseTableRef>();
	ref->catalog_name = stmt.info->catalog;
	ref->schema_name = stmt.info->schema;
	ref->table_name = stmt.info->table;

	auto statement = make_uniq<SelectNode>();
	statement->from_table = std::move(ref);

	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &col : select_list) {
		auto &name = col.first;
		auto &type = col.second;

		auto expression = make_uniq_base<ParsedExpression, ColumnRefExpression>(name);
		if (NeedsCast(type, type_check)) {
			// Add a cast to a type supported by the copy function
			expression = make_uniq_base<ParsedExpression, CastExpression>(type, std::move(expression));
		}
		expressions.push_back(std::move(expression));
	}

	statement->select_list = std::move(expressions);
	return std::move(statement);
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
	auto &copy_function =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function.function.copy_to_bind && !copy_function.function.plan) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	// gather a list of all the tables
	string catalog = stmt.database.empty() ? INVALID_CATALOG : stmt.database;
	catalog_entry_vector_t tables;
	auto schemas = Catalog::GetSchemas(context, catalog);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry.Cast<TableCatalogEntry>());
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
		auto &table = t.get().Cast<TableCatalogEntry>();
		auto info = make_uniq<CopyInfo>();
		// we copy the options supplied to the EXPORT
		info->format = stmt.info->format;
		info->options = stmt.info->options;
		// set up the file name for the COPY TO

		idx_t id = 0;
		while (true) {
			string id_suffix = id == 0 ? string() : "_" + to_string(id);
			auto name = CreateFileName(id_suffix, table, copy_function.function.extension);
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
		info->schema = table.schema.name;
		info->table = table.name;

		// We can not export generated columns
		child_list_t<LogicalType> select_list;

		for (auto &col : table.GetColumns().Physical()) {
			select_list.push_back(std::make_pair(col.Name(), col.Type()));
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
		copy_stmt.select_statement =
		    CreateSelectStatement(copy_stmt, select_list, copy_function.function.supports_type);

		auto copy_binder = Binder::CreateBinder(context, this);
		auto bound_statement = copy_binder->Bind(copy_stmt);
		auto plan = std::move(bound_statement.plan);

		if (child_operator) {
			// use UNION ALL to combine the individual copy statements into a single node
			auto copy_union = make_uniq<LogicalSetOperation>(GenerateTableIndex(), 1, std::move(child_operator),
			                                                 std::move(plan), LogicalOperatorType::LOGICAL_UNION);
			child_operator = std::move(copy_union);
		} else {
			child_operator = std::move(plan);
		}
	}

	// try to create the directory, if it doesn't exist yet
	// a bit hacky to do it here, but we need to create the directory BEFORE the copy statements run
	if (!fs.DirectoryExists(stmt.info->file_path)) {
		fs.CreateDirectory(stmt.info->file_path);
	}

	// create the export node
	auto export_node = make_uniq<LogicalExport>(copy_function.function, std::move(stmt.info), exported_tables);

	if (child_operator) {
		export_node->children.push_back(std::move(child_operator));
	}

	result.plan = std::move(export_node);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
