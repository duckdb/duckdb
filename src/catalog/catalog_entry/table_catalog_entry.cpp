#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "catalog/catalog.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "parser/constraints/list.hpp"
#include "storage/storage_manager.hpp"
// TODO this pulls in all that stuff. Do we want that?
#include "main/connection.hpp"
#include "main/database.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

void TableCatalogEntry::Initialize(CreateTableInformation *info) {
	for (auto entry : info->columns) {
		if (ColumnExists(entry.name)) {
			throw CatalogException("Column with name %s already exists!", entry.name.c_str());
		}

		column_t oid = columns.size();
		name_map[entry.name] = oid;
		entry.oid = oid;
		columns.push_back(entry);
	}
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInformation *info)
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
			vector<size_t> keys;
			if (c->index != (size_t)-1) {
				// column referenced by key is given by index
				types.push_back(columns[c->index].type);
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
					types.push_back(columns[entry->second].type);
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

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInformation *info,
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

unique_ptr<CatalogEntry> TableCatalogEntry::AlterEntry(AlterInformation *info) {
	if (info->type != AlterType::ALTER_TABLE) {
		throw CatalogException("Can only modify table with ALTER TABLE statement");
	}
	auto table_info = (AlterTableInformation *)info;
	switch (table_info->alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto rename_info = (RenameColumnInformation *)table_info;
		CreateTableInformation create_info;
		create_info.schema = schema->name;
		create_info.table = name;
		bool found = false;
		for (size_t i = 0; i < columns.size(); i++) {
			create_info.columns.push_back(columns[i]);
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
		for (size_t i = 0; i < constraints.size(); i++) {
			create_info.constraints[i] = constraints[i]->Copy();
		}
		return make_unique<TableCatalogEntry>(catalog, schema, &create_info, storage);
	}
	default:
		throw CatalogException("Unrecognized alter table type!");
	}
}

ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) {
	if (!ColumnExists(name)) {
		throw CatalogException("Column with name %s does not exist!", name.c_str());
	}
	return columns[name_map[name]];
}

ColumnStatistics &TableCatalogEntry::GetStatistics(column_t oid) {
	return storage->GetStatistics(oid);
}

vector<TypeId> TableCatalogEntry::GetTypes() {
	vector<TypeId> types;
	for (auto &it : columns) {
		types.push_back(it.type);
	}
	return types;
}

vector<TypeId> TableCatalogEntry::GetTypes(const vector<column_t> &column_ids) {
	vector<TypeId> result;
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			result.push_back(TypeId::POINTER);
		} else {
			result.push_back(columns[index].type);
		}
	}
	return result;
}

void TableCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	serializer.Write<uint32_t>(columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column.name);
		serializer.Write<int>((int)column.type);
	}
	serializer.Write<uint32_t>(constraints.size());
	for (auto &constraint : constraints) {
		constraint->Serialize(serializer);
	}
}

unique_ptr<CreateTableInformation> TableCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTableInformation>();

	info->schema = source.Read<string>();
	info->table = source.Read<string>();
	auto column_count = source.Read<uint32_t>();

	for (size_t i = 0; i < column_count; i++) {
		auto column_name = source.Read<string>();
		auto column_type = (TypeId)source.Read<int>();
		info->columns.push_back(ColumnDefinition(column_name, column_type, false));
	}
	auto constraint_count = source.Read<uint32_t>();

	for (size_t i = 0; i < constraint_count; i++) {
		auto constraint = Constraint::Deserialize(source);
		info->constraints.push_back(move(constraint));
	}
	return info;
}

bool TableCatalogEntry::HasDependents(Transaction &transaction) {
	bool found_table = false;

	catalog->storage.GetDatabase().connection_manager.Scan([&](DuckDBConnection *conn) {
		auto &prep_catalog = conn->context.prepared_statements;
		prep_catalog->Scan(transaction, [&](CatalogEntry *entry) {
			assert(entry->type == CatalogType::PREPARED_STATEMENT);
			auto prep_stmt = (PreparedStatementCatalogEntry *)entry;
			// TODO add HasTable() call to prep_stmt or so
			if (prep_stmt->tables.find(this) != prep_stmt->tables.end()) {
				found_table = true;
			}
		});
	});

	if (found_table) {
		return true;
	}
	return false;
}

void TableCatalogEntry::DropDependents(Transaction &transaction) {
}
