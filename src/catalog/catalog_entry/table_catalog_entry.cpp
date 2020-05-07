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
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder/alter_binder.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : StandardEntry(CatalogType::TABLE, schema, catalog, info->Base().table), storage(inherited_storage),
      columns(move(info->Base().columns)), constraints(move(info->Base().constraints)),
      bound_constraints(move(info->bound_constraints)), name_map(info->name_map) {
	this->temporary = info->Base().temporary;
	// add the "rowid" alias, if there is no rowid column specified in the table
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	if (!storage) {
		// create the physical storage
		storage = make_shared<DataTable>(catalog->storage, schema->name, name, GetTypes(), move(info->data));

		// create the unique indexes for the UNIQUE and PRIMARY KEY constraints
		for (idx_t i = 0; i < bound_constraints.size(); i++) {
			auto &constraint = bound_constraints[i];
			if (constraint->type == ConstraintType::UNIQUE) {
				// unique constraint: create a unique index
				auto &unique = (BoundUniqueConstraint &)*constraint;
				// fetch types and create expressions for the index from the columns
				vector<column_t> column_ids;
				vector<unique_ptr<Expression>> unbound_expressions;
				vector<unique_ptr<Expression>> bound_expressions;
				idx_t key_nr = 0;
				for (auto &key : unique.keys) {
					TypeId column_type = GetInternalType(columns[key].type);
					assert(key < columns.size());

					unbound_expressions.push_back(
					    make_unique<BoundColumnRefExpression>(column_type, ColumnBinding(0, column_ids.size())));
					bound_expressions.push_back(make_unique<BoundReferenceExpression>(column_type, key_nr++));
					column_ids.push_back(key);
				}
				// create an adaptive radix tree around the expressions
				auto art = make_unique<ART>(column_ids, move(unbound_expressions), true);
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
	auto table_info = (AlterTableInfo *)info;
	switch (table_info->alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto rename_info = (RenameColumnInfo *)table_info;
		return RenameColumn(context, *rename_info);
	}
	case AlterTableType::RENAME_TABLE: {
		auto rename_info = (RenameTableInfo *)table_info;
		auto copied_table = Copy(context);
		copied_table->name = rename_info->new_table_name;
		return copied_table;
	}
	case AlterTableType::ADD_COLUMN: {
		auto add_info = (AddColumnInfo *)table_info;
		return AddColumn(context, *add_info);
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto remove_info = (RemoveColumnInfo *)table_info;
		return RemoveColumn(context, *remove_info);
	}
	case AlterTableType::SET_DEFAULT: {
		auto set_default_info = (SetDefaultInfo *)table_info;
		return SetDefault(context, *set_default_info);
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto change_type_info = (ChangeColumnTypeInfo *)table_info;
		return ChangeColumnType(context, *change_type_info);
	}
	default:
		throw InternalException("Unrecognized alter table type!");
	}
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.column_name == info.name) {
			colref.column_name = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

unique_ptr<CatalogEntry> TableCatalogEntry::RenameColumn(ClientContext &context, RenameColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	bool found = false;
	for (idx_t i = 0; i < columns.size(); i++) {
		ColumnDefinition copy = columns[i].Copy();

		create_info->columns.push_back(move(copy));
		if (info.name == columns[i].name) {
			assert(!found);
			create_info->columns[i].name = info.new_name;
			found = true;
		}
	}
	if (!found) {
		throw CatalogException("Table does not have a column with name \"%s\"", info.name.c_str());
	}
	for (idx_t c_idx = 0; c_idx < constraints.size(); c_idx++) {
		auto copy = constraints[c_idx]->Copy();
		switch (copy->type) {
		case ConstraintType::NOT_NULL:
			// NOT NULL constraint: no adjustments necessary
			break;
		case ConstraintType::CHECK: {
			// CHECK constraint: need to rename column references that refer to the renamed column
			auto &check = (CheckConstraint &)*copy;
			RenameExpression(*check.expression, info);
			break;
		}
		case ConstraintType::UNIQUE: {
			// UNIQUE constraint: possibly need to rename columns
			auto &unique = (UniqueConstraint &)*copy;
			for (idx_t i = 0; i < unique.columns.size(); i++) {
				if (unique.columns[i] == info.name) {
					unique.columns[i] = info.new_name;
				}
			}
			break;
		}
		default:
			throw CatalogException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(copy));
	}
	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::AddColumn(ClientContext &context, AddColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}
	info.new_column.oid = columns.size();
	create_info->columns.push_back(info.new_column.Copy());

	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	auto new_storage =
	    make_shared<DataTable>(context, *storage, info.new_column, bound_create_info->bound_defaults.back().get());
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::RemoveColumn(ClientContext &context, RemoveColumnInfo &info) {
	idx_t removed_index = INVALID_INDEX;
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (columns[i].name == info.removed_column) {
			assert(removed_index == INVALID_INDEX);
			removed_index = i;
			continue;
		}
		create_info->columns.push_back(columns[i].Copy());
	}
	if (removed_index == INVALID_INDEX) {
		if (!info.if_exists) {
			throw CatalogException("Table does not have a column with name \"%s\"", info.removed_column.c_str());
		}
		return nullptr;
	}
	if (create_info->columns.size() == 0) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	// handle constraints for the new table
	assert(constraints.size() == bound_constraints.size());
	for (idx_t constr_idx = 0; constr_idx < constraints.size(); constr_idx++) {
		auto &constraint = constraints[constr_idx];
		auto &bound_constraint = bound_constraints[constr_idx];
		switch (bound_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null_constraint = (BoundNotNullConstraint &)*bound_constraint;
			if (not_null_constraint.index != removed_index) {
				// the constraint is not about this column: we need to copy it
				// we might need to shift the index back by one though, to account for the removed column
				idx_t new_index = not_null_constraint.index;
				if (not_null_constraint.index > removed_index) {
					new_index -= 1;
				}
				create_info->constraints.push_back(make_unique<NotNullConstraint>(new_index));
			}
			break;
		}
		case ConstraintType::CHECK: {
			// CHECK constraint
			auto &bound_check = (BoundCheckConstraint &)*bound_constraint;
			// check if the removed column is part of the check constraint
			if (bound_check.bound_columns.find(removed_index) != bound_check.bound_columns.end()) {
				if (bound_check.bound_columns.size() > 1) {
					// CHECK constraint that concerns mult
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a CHECK constraint that depends on it",
					    info.removed_column.c_str());
				} else {
					// CHECK constraint that ONLY concerns this column, strip the constraint
				}
			} else {
				// check constraint does not concern the removed column: simply re-add it
				create_info->constraints.push_back(constraint->Copy());
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto copy = constraint->Copy();
			auto &unique = (UniqueConstraint &) *copy;
			if (unique.index != INVALID_INDEX) {
				if (unique.index == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column.c_str());
				} else if (unique.index > removed_index) {
					unique.index--;
				}
			}
			create_info->constraints.push_back(move(copy));
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
	}

	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	auto new_storage = make_shared<DataTable>(context, *storage, removed_index);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetDefault(ClientContext &context, SetDefaultInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	bool found = false;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (info.column_name == copy.name) {
			// set the default value of this column
			copy.default_value = info.expression ? info.expression->Copy() : nullptr;
			found = true;
		}
		create_info->columns.push_back(move(copy));
	}
	if (!found) {
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", info.table.c_str(),
		                      info.column_name.c_str());
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	idx_t change_idx = INVALID_INDEX;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (info.column_name == copy.name) {
			// set the default value of this column
			change_idx = i;
			copy.type = info.target_type;
		}
		create_info->columns.push_back(move(copy));
	}
	if (change_idx == INVALID_INDEX) {
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", info.table.c_str(),
		                      info.column_name.c_str());
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		switch (constraint->type) {
		case ConstraintType::CHECK: {
			auto &bound_check = (BoundCheckConstraint &)*bound_constraints[i];
			if (bound_check.bound_columns.find(change_idx) != bound_check.bound_columns.end()) {
				throw BinderException("Cannot change the type of a column that has a CHECK constraint specified");
			}
			break;
		}
		case ConstraintType::NOT_NULL:
			break;
		case ConstraintType::UNIQUE: {
			auto &bound_unique = (BoundUniqueConstraint &)*bound_constraints[i];
			if (bound_unique.keys.find(change_idx) != bound_unique.keys.end()) {
				throw BinderException(
				    "Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
			}
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(constraint));
	}

	Binder binder(context);
	// bind the specified expression
	vector<column_t> bound_columns;
	AlterBinder expr_binder(binder, context, name, columns, bound_columns, info.target_type);
	auto expression = info.expression->Copy();
	auto bound_expression = expr_binder.Bind(expression);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	if (bound_columns.size() == 0) {
		bound_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto new_storage =
	    make_shared<DataTable>(context, *storage, change_idx, info.target_type, move(bound_columns), *bound_expression);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
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
		column.Serialize(serializer);
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
		auto column = ColumnDefinition::Deserialize(source);
		info->columns.push_back(move(column));
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
	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	Binder binder(context);
	auto bound_create_info = binder.BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

void TableCatalogEntry::SetAsRoot() {
	storage->SetAsRoot();
}
