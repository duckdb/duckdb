#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/prepared_statement_catalog_entry.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"
#include "parser/constraints/list.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "planner/constraints/bound_unique_constraint.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/parsed_data/bound_create_table_info.hpp"
#include "storage/storage_manager.hpp"
#include "planner/binder.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : CatalogEntry(CatalogType::TABLE, catalog, info->base->table), schema(schema), storage(inherited_storage),
      columns(move(info->base->columns)), constraints(move(info->base->constraints)),
      bound_constraints(move(info->bound_constraints)),
      name_map(info->name_map) {
	// add the "rowid" alias, if there is no rowid column specified in the table
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	if (!storage) {
		// create the physical storage
		storage = make_shared<DataTable>(catalog->storage, schema->name, name, GetTypes());
		// create the unique indexes for the UNIQUE and PRIMARY KEY constraints
		for (auto &constraint : bound_constraints) {
			if (constraint->type == ConstraintType::UNIQUE) {
				vector<TypeId> types;
				auto &unique = (BoundUniqueConstraint &)*constraint;
				// fetch the types from the columns
				for (auto key : unique.keys) {
					assert(key < columns.size());
					types.push_back(GetInternalType(columns[key].type));
				}
				// initialize the index with the parsed data
				storage->unique_indexes.push_back(
				    make_unique<UniqueIndex>(*storage, types, unique.keys, !unique.is_primary_key));
			}
		}
	}
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

unique_ptr<CatalogEntry> TableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	if (info->type != AlterType::ALTER_TABLE) {
		throw CatalogException("Can only modify table with ALTER TABLE statement");
	}
	auto table_info = (AlterTableInfo *)info;
	switch (table_info->alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto rename_info = (RenameColumnInfo *)table_info;
		auto create_info = make_unique<CreateTableInfo>(schema->name, name);
		bool found = false;
		for (index_t i = 0; i < columns.size(); i++) {
			ColumnDefinition copy(columns[i].name, columns[i].type);
			copy.oid = columns[i].oid;
			copy.default_value = columns[i].default_value ? columns[i].default_value->Copy() : nullptr;

			create_info->columns.push_back(move(copy));
			if (rename_info->name == columns[i].name) {
				assert(!found);
				create_info->columns[i].name = rename_info->new_name;
				found = true;
			}
		}
		if (!found) {
			throw CatalogException("Table does not have a column with name \"%s\"", rename_info->name.c_str());
		}
		create_info->constraints.resize(constraints.size());
		for (index_t i = 0; i < constraints.size(); i++) {
			create_info->constraints[i] = constraints[i]->Copy();
		}
		Binder binder(context);
		auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
		return make_unique<TableCatalogEntry>(catalog, schema, bound_create_info.get(), storage);
	}
	default:
		throw CatalogException("Unrecognized alter table type!");
	}
}

ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) {
	auto entry = name_map.find(name);
	if (entry == name_map.end() || entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Column with name %s does not exist!", name.c_str());
	}
	return columns[entry->second];
}

ColumnStatistics &TableCatalogEntry::GetStatistics(column_t oid) {
	return storage->GetStatistics(oid);
}

vector<TypeId> TableCatalogEntry::GetTypes() {
	vector<TypeId> types;
	for (auto &it : columns) {
		types.push_back(GetInternalType(it.type));
	}
	return types;
}

vector<TypeId> TableCatalogEntry::GetTypes(const vector<column_t> &column_ids) {
	vector<TypeId> result;
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			result.push_back(TypeId::BIGINT);
		} else {
			result.push_back(GetInternalType(columns[index].type));
		}
	}
	return result;
}

void TableCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	assert(columns.size() <= std::numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column.name);
		column.type.Serialize(serializer);
		serializer.WriteOptional(column.default_value);
	}
	assert(constraints.size() <= std::numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)constraints.size());
	for (auto &constraint : constraints) {
		constraint->Serialize(serializer);
	}
}

unique_ptr<CreateTableInfo> TableCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTableInfo>();

	info->schema = source.Read<string>();
	info->table = source.Read<string>();
	auto column_count = source.Read<uint32_t>();

	for (uint32_t i = 0; i < column_count; i++) {
		auto column_name = source.Read<string>();
		auto column_type = SQLType::Deserialize(source);
		auto default_value = source.ReadOptional<ParsedExpression>();
		info->columns.push_back(ColumnDefinition(column_name, column_type, move(default_value)));
	}
	auto constraint_count = source.Read<uint32_t>();

	for (uint32_t i = 0; i < constraint_count; i++) {
		auto constraint = Constraint::Deserialize(source);
		info->constraints.push_back(move(constraint));
	}
	return info;
}
