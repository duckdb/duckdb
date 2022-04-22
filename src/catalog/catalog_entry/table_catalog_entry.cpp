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
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder/alter_binder.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

idx_t TableCatalogEntry::GetColumnIndex(string &column_name, bool if_exists) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		// entry not found: try lower-casing the name
		entry = name_map.find(StringUtil::Lower(column_name));
		if (entry == name_map.end()) {
			if (if_exists) {
				return DConstants::INVALID_INDEX;
			}
			throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, column_name);
		}
	}
	column_name = columns[entry->second].name;
	return idx_t(entry->second);
}

void AddDataTableIndex(DataTable *storage, vector<ColumnDefinition> &columns, vector<idx_t> &keys,
                       IndexConstraintType constraint_type) {
	// fetch types and create expressions for the index from the columns
	vector<column_t> column_ids;
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<Expression>> bound_expressions;
	idx_t key_nr = 0;
	for (auto &key : keys) {
		D_ASSERT(key < columns.size());

		unbound_expressions.push_back(make_unique<BoundColumnRefExpression>(columns[key].name, columns[key].type,
		                                                                    ColumnBinding(0, column_ids.size())));

		bound_expressions.push_back(make_unique<BoundReferenceExpression>(columns[key].type, key_nr++));
		column_ids.push_back(key);
	}
	// create an adaptive radix tree around the expressions
	auto art = make_unique<ART>(column_ids, move(unbound_expressions), constraint_type);
	storage->AddIndex(move(art), bound_expressions);
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : StandardEntry(CatalogType::TABLE_ENTRY, schema, catalog, info->Base().table), storage(move(inherited_storage)),
      columns(move(info->Base().columns)), constraints(move(info->Base().constraints)),
      bound_constraints(move(info->bound_constraints)) {
	this->temporary = info->Base().temporary;
	// add lower case aliases
	for (idx_t i = 0; i < columns.size(); i++) {
		D_ASSERT(name_map.find(columns[i].name) == name_map.end());
		name_map[columns[i].name] = i;
	}
	// add the "rowid" alias, if there is no rowid column specified in the table
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	if (!storage) {
		// create the physical storage
		vector<ColumnDefinition> colum_def_copy;
		for (auto &col_def : columns) {
			colum_def_copy.push_back(col_def.Copy());
		}
		storage = make_shared<DataTable>(catalog->db, schema->name, name, move(colum_def_copy), move(info->data));

		// create the unique indexes for the UNIQUE and PRIMARY KEY and FOREIGN KEY constraints
		for (idx_t i = 0; i < bound_constraints.size(); i++) {
			auto &constraint = bound_constraints[i];
			if (constraint->type == ConstraintType::UNIQUE) {
				// unique constraint: create a unique index
				auto &unique = (BoundUniqueConstraint &)*constraint;
				IndexConstraintType constraint_type = IndexConstraintType::UNIQUE;
				if (unique.is_primary_key) {
					constraint_type = IndexConstraintType::PRIMARY;
				}
				AddDataTableIndex(storage.get(), columns, unique.keys, constraint_type);
			} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
				// foreign key constraint: create a foreign key index
				auto &bfk = (BoundForeignKeyConstraint &)*constraint;
				if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
				    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
					AddDataTableIndex(storage.get(), columns, bfk.info.fk_keys, IndexConstraintType::FOREIGN);
				}
			}
		}
	}
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

unique_ptr<CatalogEntry> TableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	D_ASSERT(!internal);
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
	case AlterTableType::FOREIGN_KEY_CONSTRAINT: {
		auto foreign_key_constraint_info = (AlterForeignKeyInfo *)table_info;
		return SetForeignKeyConstraint(context, *foreign_key_constraint_info);
	}
	default:
		throw InternalException("Unrecognized alter table type!");
	}
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.column_names[0] == info.old_name) {
			colref.column_names[0] = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

unique_ptr<CatalogEntry> TableCatalogEntry::RenameColumn(ClientContext &context, RenameColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	idx_t rename_idx = GetColumnIndex(info.old_name);
	for (idx_t i = 0; i < columns.size(); i++) {
		ColumnDefinition copy = columns[i].Copy();

		create_info->columns.push_back(move(copy));
		if (rename_idx == i) {
			create_info->columns[i].name = info.new_name;
		}
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
				if (unique.columns[i] == info.old_name) {
					unique.columns[i] = info.new_name;
				}
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			// FOREIGN KEY constraint: possibly need to rename columns
			auto &fk = (ForeignKeyConstraint &)*copy;
			vector<string> columns = fk.pk_columns;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				columns = fk.fk_columns;
			} else if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < fk.fk_columns.size(); i++) {
					columns.push_back(fk.fk_columns[i]);
				}
			}
			for (idx_t i = 0; i < columns.size(); i++) {
				if (columns[i] == info.old_name) {
					throw CatalogException(
					    "Cannot rename column \"%s\" because this is involved in the foreign key constraint",
					    info.old_name);
				}
			}
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(copy));
	}
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::AddColumn(ClientContext &context, AddColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}
	Binder::BindLogicalType(context, info.new_column.type, schema->name);
	info.new_column.oid = columns.size();
	create_info->columns.push_back(info.new_column.Copy());

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	auto new_storage =
	    make_shared<DataTable>(context, *storage, info.new_column, bound_create_info->bound_defaults.back().get());
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::RemoveColumn(ClientContext &context, RemoveColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	idx_t removed_index = GetColumnIndex(info.removed_column, info.if_exists);
	if (removed_index == DConstants::INVALID_INDEX) {
		return nullptr;
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (removed_index != i) {
			create_info->columns.push_back(columns[i].Copy());
		}
	}
	if (create_info->columns.empty()) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	// handle constraints for the new table
	D_ASSERT(constraints.size() == bound_constraints.size());
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
					    info.removed_column);
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
			auto &unique = (UniqueConstraint &)*copy;
			if (unique.index != DConstants::INVALID_INDEX) {
				if (unique.index == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column);
				} else if (unique.index > removed_index) {
					unique.index--;
				}
			}
			create_info->constraints.push_back(move(copy));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto copy = constraint->Copy();
			auto &fk = (ForeignKeyConstraint &)*copy;
			vector<string> columns = fk.pk_columns;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				columns = fk.fk_columns;
			} else if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < fk.fk_columns.size(); i++) {
					columns.push_back(fk.fk_columns[i]);
				}
			}
			for (idx_t i = 0; i < columns.size(); i++) {
				if (columns[i] == info.removed_column) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a FOREIGN KEY constraint that depends on it",
					    info.removed_column);
				}
			}
			create_info->constraints.push_back(move(copy));
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	auto new_storage = make_shared<DataTable>(context, *storage, removed_index);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetDefault(ClientContext &context, SetDefaultInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	idx_t default_idx = GetColumnIndex(info.column_name);
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (default_idx == i) {
			// set the default value of this column
			copy.default_value = info.expression ? info.expression->Copy() : nullptr;
		}
		create_info->columns.push_back(move(copy));
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info) {
	if (info.target_type.id() == LogicalTypeId::USER) {
		auto &user_type_name = UserType::GetTypeName(info.target_type);
		auto user_type_catalog = (TypeCatalogEntry *)context.db->GetCatalog().GetEntry(
		    context, CatalogType::TYPE_ENTRY, schema->name, user_type_name, true);
		if (!user_type_catalog) {
			throw NotImplementedException("DataType %s not supported yet...\n", user_type_name);
		}
		info.target_type = user_type_catalog->user_type;
	}
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	idx_t change_idx = GetColumnIndex(info.column_name);
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (change_idx == i) {
			// set the default value of this column
			copy.type = info.target_type;
		}
		create_info->columns.push_back(move(copy));
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
			if (bound_unique.key_set.find(change_idx) != bound_unique.key_set.end()) {
				throw BinderException(
				    "Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = (BoundForeignKeyConstraint &)*bound_constraints[i];
			unordered_set<idx_t> key_set = bfk.pk_key_set;
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				key_set = bfk.fk_key_set;
			} else if (bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < bfk.info.fk_keys.size(); i++) {
					key_set.insert(bfk.info.fk_keys[i]);
				}
			}
			if (key_set.find(change_idx) != key_set.end()) {
				throw BinderException("Cannot change the type of a column that has a FOREIGN KEY constraint specified");
			}
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	// bind the specified expression
	vector<column_t> bound_columns;
	AlterBinder expr_binder(*binder, context, *this, bound_columns, info.target_type);
	auto expression = info.expression->Copy();
	auto bound_expression = expr_binder.Bind(expression);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	if (bound_columns.empty()) {
		bound_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto new_storage =
	    make_shared<DataTable>(context, *storage, change_idx, info.target_type, move(bound_columns), *bound_expression);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);

	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			ForeignKeyConstraint &fk = (ForeignKeyConstraint &)*constraint;
			if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && fk.info.table == info.fk_table) {
				continue;
			}
		}
		create_info->constraints.push_back(move(constraint));
	}
	if (info.type == AlterForeignKeyType::AFT_ADD) {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
		fk_info.schema = info.schema;
		fk_info.table = info.fk_table;
		fk_info.pk_keys = info.pk_keys;
		fk_info.fk_keys = info.fk_keys;
		create_info->constraints.push_back(
		    make_unique<ForeignKeyConstraint>(info.pk_columns, info.fk_columns, move(fk_info)));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));

	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) {
	auto entry = name_map.find(name);
	if (entry == name_map.end() || entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Column with name %s does not exist!", name);
	}
	return columns[entry->second];
}

vector<LogicalType> TableCatalogEntry::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : columns) {
		types.push_back(it.type);
	}
	return types;
}

void TableCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);

	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteRegularSerializableList(columns);
	writer.WriteSerializableList(constraints);
	writer.Finalize();
}

unique_ptr<CreateTableInfo> TableCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTableInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->table = reader.ReadRequired<string>();
	info->columns = reader.ReadRequiredSerializableList<ColumnDefinition, ColumnDefinition>();
	info->constraints = reader.ReadRequiredSerializableList<Constraint>();
	reader.Finalize();

	return info;
}

string TableCatalogEntry::ToSQL() {
	std::stringstream ss;

	ss << "CREATE TABLE ";

	if (schema->name != DEFAULT_SCHEMA) {
		ss << KeywordHelper::WriteOptionallyQuoted(schema->name) << ".";
	}

	ss << KeywordHelper::WriteOptionallyQuoted(name) << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key columns
	unordered_set<idx_t> not_null_columns;
	unordered_set<idx_t> unique_columns;
	unordered_set<idx_t> pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = (NotNullConstraint &)*constraint;
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = (UniqueConstraint &)*constraint;
			vector<string> constraint_columns = pk.columns;
			if (pk.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = (ForeignKeyConstraint &)*constraint;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			ss << ", ";
		}
		auto &column = columns[i];
		ss << KeywordHelper::WriteOptionallyQuoted(column.name) << " ";
		ss << column.type.ToString();
		bool not_null = not_null_columns.find(column.oid) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.oid) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.name) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.oid) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.default_value) {
			ss << " DEFAULT(" << column.default_value->ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ");";
	return ss.str();
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

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

void TableCatalogEntry::SetAsRoot() {
	storage->SetAsRoot();
}

void TableCatalogEntry::CommitAlter(AlterInfo &info) {
	D_ASSERT(info.type == AlterType::ALTER_TABLE);
	auto &alter_table = (AlterTableInfo &)info;
	string column_name;
	switch (alter_table.alter_table_type) {
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = (RemoveColumnInfo &)alter_table;
		column_name = remove_info.removed_column;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_info = (ChangeColumnTypeInfo &)alter_table;
		column_name = change_info.column_name;
		break;
	}
	default:
		break;
	}
	if (column_name.empty()) {
		return;
	}
	idx_t removed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (columns[i].name == column_name) {
			removed_index = i;
			break;
		}
	}
	D_ASSERT(removed_index != DConstants::INVALID_INDEX);
	storage->CommitDropColumn(removed_index);
}

void TableCatalogEntry::CommitDrop() {
	storage->CommitDropTable();
}

} // namespace duckdb
