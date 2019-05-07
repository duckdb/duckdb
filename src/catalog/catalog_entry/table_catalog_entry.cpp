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
#include "parser/parsed_data/create_table_info.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "storage/storage_manager.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

void TableCatalogEntry::Initialize(CreateTableInfo *info) {
	for (auto &entry : info->columns) {
		if (ColumnExists(entry.name)) {
			throw CatalogException("Column with name %s already exists!", entry.name.c_str());
		}

		column_t oid = columns.size();
		name_map[entry.name] = oid;
		entry.oid = oid;
		// default to using NULL value as default
		bound_defaults.push_back(make_unique<BoundConstantExpression>(Value(GetInternalType(entry.type))));
		columns.push_back(move(entry));
	}
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	assert(bound_defaults.size() == columns.size());
	for (uint64_t i = 0; i < info->bound_defaults.size(); i++) {
		auto &bound_default = info->bound_defaults[i];
		if (bound_default) {
			// explicit default: use the users' expression
			bound_defaults[i] = move(bound_default);
		}
	}
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInfo *info)
    : CatalogEntry(CatalogType::TABLE, catalog, info->table), schema(schema) {
	Initialize(info);
	storage = make_shared<DataTable>(catalog->storage, schema->name, name, GetTypes());
	// resolve the constraints
	// we have to parse the DUMMY constraints and initialize the indices
	bool has_primary_key = false;
	for (auto &constraint : info->constraints) {
		if (constraint->type == ConstraintType::DUMMY) {
			// have to resolve columns
			auto c = (ParsedConstraint *)constraint.get();
			vector<TypeId> types;
			vector<uint64_t> keys;
			if (c->index != (uint64_t)-1) {
				// column referenced by key is given by index
				types.push_back(GetInternalType(columns[c->index].type));
				keys.push_back(c->index);
			} else {
				// have to resolve names
				for (auto &keyname : c->columns) {
					auto entry = name_map.find(keyname);
					if (entry == name_map.end()) {
						throw ParserException("column \"%s\" named in key does not exist", keyname.c_str());
					}
					if (find(keys.begin(), keys.end(), entry->second) != keys.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname.c_str());
					}
					types.push_back(GetInternalType(columns[entry->second].type));
					keys.push_back(entry->second);
				}
			}

			bool allow_null = true;
			if (c->ctype == ConstraintType::PRIMARY_KEY) {
				if (has_primary_key) {
					throw ParserException("table \"%s\" has more than one primary key", name.c_str());
				}
				has_primary_key = true;
				// PRIMARY KEY constraints do not allow NULLs
				allow_null = false;
			} else {
				assert(c->ctype == ConstraintType::UNIQUE);
			}
			// initialize the index with the parsed data
			storage->unique_indexes.push_back(make_unique<UniqueIndex>(*storage, types, keys, allow_null));
		}
		constraints.push_back(move(constraint));
	}
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInfo *info,
                                     shared_ptr<DataTable> storage)
    : CatalogEntry(CatalogType::TABLE, catalog, info->table), schema(schema), storage(storage) {
	Initialize(info);
	for (auto &constraint : info->constraints) {
		assert(constraint->type != ConstraintType::DUMMY);
		constraints.push_back(move(constraint));
	}
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

unique_ptr<CatalogEntry> TableCatalogEntry::AlterEntry(AlterInfo *info) {
	if (info->type != AlterType::ALTER_TABLE) {
		throw CatalogException("Can only modify table with ALTER TABLE statement");
	}
	auto table_info = (AlterTableInfo *)info;
	switch (table_info->alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto rename_info = (RenameColumnInfo *)table_info;
		CreateTableInfo create_info;
		create_info.schema = schema->name;
		create_info.table = name;
		bool found = false;
		for (uint64_t i = 0; i < columns.size(); i++) {
			ColumnDefinition copy(columns[i].name, columns[i].type);
			copy.oid = columns[i].oid;
			copy.default_value = columns[i].default_value ? columns[i].default_value->Copy() : nullptr;

			create_info.columns.push_back(move(copy));
			if (rename_info->name == columns[i].name) {
				assert(!found);
				create_info.columns[i].name = rename_info->new_name;
				found = true;
			}
		}
		if (!found) {
			throw CatalogException("Table does not have a column with name \"%s\"", rename_info->name.c_str());
		}
		create_info.constraints.resize(constraints.size());
		for (uint64_t i = 0; i < constraints.size(); i++) {
			create_info.constraints[i] = constraints[i]->Copy();
		}
		return make_unique<TableCatalogEntry>(catalog, schema, &create_info, storage);
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
	serializer.Write<uint32_t>((uint32_t)columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column.name);
		column.type.Serialize(serializer);
		serializer.WriteOptional(column.default_value);
	}
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
