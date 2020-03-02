#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : CatalogEntry(CatalogType::TABLE, catalog, info->base->table), schema(schema), storage(inherited_storage),
      columns(move(info->base->columns)), constraints(move(info->base->constraints)),
      bound_constraints(move(info->bound_constraints)), name_map(info->name_map) {
	// add the "rowid" alias, if there is no rowid column specified in the table
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	if (!storage) {
		// create the physical storage
		storage = make_shared<DataTable>(catalog->storage, schema->name, name, GetTypes(), move(info->data));

		// create the unique indexes for the UNIQUE and PRIMARY KEY constraints
		for (index_t i = 0; i < bound_constraints.size(); i++) {
			auto &constraint = bound_constraints[i];
			if (constraint->type == ConstraintType::UNIQUE) {
				// unique constraint: create a unique index
				auto &unique = (BoundUniqueConstraint &)*constraint;
				// fetch types and create expressions for the index from the columns
				vector<column_t> column_ids;
				vector<unique_ptr<Expression>> unbound_expressions;
				vector<unique_ptr<Expression>> bound_expressions;
				index_t key_nr = 0;
				for (auto &key : unique.keys) {
					TypeId column_type = GetInternalType(columns[key].type);
					assert(key < columns.size());

					unbound_expressions.push_back(
					    make_unique<BoundColumnRefExpression>(column_type, ColumnBinding(0, column_ids.size())));
					bound_expressions.push_back(make_unique<BoundReferenceExpression>(column_type, key_nr++));
					column_ids.push_back(key);
				}
				// create an adaptive radix tree around the expressions
				auto art = make_unique<ART>(*storage, column_ids, move(unbound_expressions), true);

				if (unique.is_primary_key) {
					// if this is a primary key index, also create a NOT NULL constraint for each of the columns
					for (auto &column_index : unique.keys) {
						bound_constraints.push_back(make_unique<BoundNotNullConstraint>(column_index));
					}
				}
				storage->AddIndex(move(art), bound_expressions);
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
	if (constraints.size() > 0) {
		throw CatalogException("Cannot modify a table with constraints");
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
		assert(constraints.size() == 0);
		// create_info->constraints.resize(constraints.size());
		// for (index_t i = 0; i < constraints.size(); i++) {
		// 	create_info->constraints[i] = constraints[i]->Copy();
		// }
		Binder binder(context);
		auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
		return make_unique<TableCatalogEntry>(catalog, schema, bound_create_info.get(), storage);
	}
	case AlterTableType::RENAME_TABLE: {
        auto rename_info = (RenameTableInfo *)table_info;
        auto create_info = make_unique<CreateTableInfo>(schema->name, rename_info->new_table_name);
//        create_info->table = rename_info->new_table_name;

        for (index_t i = 0; i < columns.size(); i++) {
            ColumnDefinition copy(columns[i].name, columns[i].type);
            copy.oid = columns[i].oid;
            copy.default_value = columns[i].default_value ? columns[i].default_value->Copy() : nullptr;
            create_info->columns.push_back(move(copy));
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
			result.push_back(TypeId::INT64);
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

unique_ptr<CatalogEntry> TableCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
    for (index_t i = 0; i < columns.size(); i++) {
        ColumnDefinition copy(columns[i].name, columns[i].type);
        copy.oid = columns[i].oid;
        copy.default_value = columns[i].default_value ? columns[i].default_value->Copy() : nullptr;
        create_info->columns.push_back(move(copy));
    }

	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, bound_create_info.get(), storage);
}
