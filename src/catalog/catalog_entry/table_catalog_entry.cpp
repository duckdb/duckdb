#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/alter_binder.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/index_map.hpp"

#include <sstream>

namespace duckdb {

bool TableCatalogEntry::HasGeneratedColumns() const {
	return columns.LogicalColumnCount() != columns.PhysicalColumnCount();
}

const string &TableCatalogEntry::GetColumnName(column_t index) {
	return columns.GetColumn(LogicalIndex(index)).Name();
}

LogicalIndex TableCatalogEntry::GetColumnIndexLogical(string &column_name, bool if_exists) {
	auto entry = columns.GetColumnIndex(column_name);
	if (entry.index == DConstants::INVALID_INDEX) {
		if (if_exists) {
			return entry;
		}
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, column_name);
	}
	return entry;
}

column_t TableCatalogEntry::GetColumnIndex(string &column_name, bool if_exists) {
	return GetColumnIndexLogical(column_name, if_exists).index;
}

void AddDataTableIndex(DataTable *storage, const ColumnList &columns, const vector<PhysicalIndex> &keys,
                       IndexConstraintType constraint_type, BlockPointer *index_block = nullptr) {
	// fetch types and create expressions for the index from the columns
	vector<column_t> column_ids;
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<Expression>> bound_expressions;
	idx_t key_nr = 0;
	for (auto &physical_key : keys) {
		auto &column = columns.GetColumn(physical_key);
		D_ASSERT(!column.Generated());
		unbound_expressions.push_back(
		    make_unique<BoundColumnRefExpression>(column.Name(), column.Type(), ColumnBinding(0, column_ids.size())));

		bound_expressions.push_back(make_unique<BoundReferenceExpression>(column.Type(), key_nr++));
		column_ids.push_back(column.StorageOid());
	}
	unique_ptr<ART> art;
	// create an adaptive radix tree around the expressions
	if (index_block) {
		art = make_unique<ART>(column_ids, TableIOManager::Get(*storage), move(unbound_expressions), constraint_type,
		                       storage->db, index_block->block_id, index_block->offset);
	} else {
		art = make_unique<ART>(column_ids, TableIOManager::Get(*storage), move(unbound_expressions), constraint_type,
		                       storage->db);
		if (!storage->IsRoot()) {
			throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
		}
	}
	storage->info->indexes.AddIndex(move(art));
}

void AddDataTableIndex(DataTable *storage, const ColumnList &columns, vector<LogicalIndex> &keys,
                       IndexConstraintType constraint_type, BlockPointer *index_block = nullptr) {
	vector<PhysicalIndex> new_keys;
	for (auto &logical_key : keys) {
		new_keys.push_back(columns.LogicalToPhysical(logical_key));
	}
	AddDataTableIndex(storage, columns, new_keys, constraint_type, index_block);
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : StandardEntry(CatalogType::TABLE_ENTRY, schema, catalog, info->Base().table), storage(move(inherited_storage)),
      columns(move(info->Base().columns)), constraints(move(info->Base().constraints)),
      bound_constraints(move(info->bound_constraints)),
      column_dependency_manager(move(info->column_dependency_manager)) {
	this->temporary = info->Base().temporary;
	if (!storage) {
		// create the physical storage
		vector<ColumnDefinition> storage_columns;
		for (auto &col_def : columns.Physical()) {
			storage_columns.push_back(col_def.Copy());
		}
		storage =
		    make_shared<DataTable>(catalog->db, StorageManager::GetStorageManager(catalog->db).GetTableIOManager(info),
		                           schema->name, name, move(storage_columns), move(info->data));

		// create the unique indexes for the UNIQUE and PRIMARY KEY and FOREIGN KEY constraints
		idx_t indexes_idx = 0;
		for (idx_t i = 0; i < bound_constraints.size(); i++) {
			auto &constraint = bound_constraints[i];
			if (constraint->type == ConstraintType::UNIQUE) {
				// unique constraint: create a unique index
				auto &unique = (BoundUniqueConstraint &)*constraint;
				IndexConstraintType constraint_type = IndexConstraintType::UNIQUE;
				if (unique.is_primary_key) {
					constraint_type = IndexConstraintType::PRIMARY;
				}
				if (info->indexes.empty()) {
					AddDataTableIndex(storage.get(), columns, unique.keys, constraint_type);
				} else {
					AddDataTableIndex(storage.get(), columns, unique.keys, constraint_type,
					                  &info->indexes[indexes_idx++]);
				}
			} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
				// foreign key constraint: create a foreign key index
				auto &bfk = (BoundForeignKeyConstraint &)*constraint;
				if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
				    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
					if (info->indexes.empty()) {
						AddDataTableIndex(storage.get(), columns, bfk.info.fk_keys, IndexConstraintType::FOREIGN);
					} else {
						AddDataTableIndex(storage.get(), columns, bfk.info.fk_keys, IndexConstraintType::FOREIGN,
						                  &info->indexes[indexes_idx++]);
					}
				}
			}
		}
	}
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return columns.ColumnExists(name);
}

idx_t TableCatalogEntry::StandardColumnCount() const {
	return columns.PhysicalColumnCount();
}

unique_ptr<BaseStatistics> TableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	auto &column = columns.GetColumn(LogicalIndex(column_id));
	if (column.Generated()) {
		return nullptr;
	}
	return storage->GetStatistics(context, column.StorageOid());
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
		storage->info->table = rename_info->new_table_name;
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
		if (foreign_key_constraint_info->type == AlterForeignKeyType::AFT_ADD) {
			return AddForeignKeyConstraint(context, *foreign_key_constraint_info);
		} else {
			return DropForeignKeyConstraint(context, *foreign_key_constraint_info);
		}
	}
	case AlterTableType::SET_NOT_NULL: {
		auto set_not_null_info = (SetNotNullInfo *)table_info;
		return SetNotNull(context, *set_not_null_info);
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto drop_not_null_info = (DropNotNullInfo *)table_info;
		return DropNotNull(context, *drop_not_null_info);
	}
	default:
		throw InternalException("Unrecognized alter table type!");
	}
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.column_names.back() == info.old_name) {
			colref.column_names.back() = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

unique_ptr<CatalogEntry> TableCatalogEntry::RenameColumn(ClientContext &context, RenameColumnInfo &info) {
	auto rename_idx = GetColumnIndexLogical(info.old_name);
	if (rename_idx.index == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Cannot rename rowid column");
	}
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (rename_idx == col.Logical()) {
			copy.SetName(info.new_name);
		}
		if (col.Generated() && column_dependency_manager.IsDependencyOf(col.Logical(), rename_idx)) {
			RenameExpression(copy.GeneratedExpressionMutable(), info);
		}
		create_info->columns.AddColumn(move(copy));
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
	auto col_name = info.new_column.GetName();

	// We're checking for the opposite condition (ADD COLUMN IF _NOT_ EXISTS ...).
	if (info.if_column_not_exists && GetColumnIndex(col_name, true) != DConstants::INVALID_INDEX) {
		return nullptr;
	}

	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	for (auto &col : columns.Logical()) {
		create_info->columns.AddColumn(col.Copy());
	}
	for (auto &constraint : constraints) {
		create_info->constraints.push_back(constraint->Copy());
	}
	Binder::BindLogicalType(context, info.new_column.TypeMutable(), schema->name);
	auto col = info.new_column.Copy();

	create_info->columns.AddColumn(move(col));

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	auto new_storage =
	    make_shared<DataTable>(context, *storage, info.new_column, bound_create_info->bound_defaults.back().get());
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::RemoveColumn(ClientContext &context, RemoveColumnInfo &info) {
	auto removed_index = GetColumnIndexLogical(info.removed_column, info.if_column_exists);
	if (removed_index.index == DConstants::INVALID_INDEX) {
		if (!info.if_column_exists) {
			throw CatalogException("Cannot drop column: rowid column cannot be dropped");
		}
		return nullptr;
	}

	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	logical_index_set_t removed_columns;
	if (column_dependency_manager.HasDependents(removed_index)) {
		removed_columns = column_dependency_manager.GetDependents(removed_index);
	}
	if (!removed_columns.empty() && !info.cascade) {
		throw CatalogException("Cannot drop column: column is a dependency of 1 or more generated column(s)");
	}
	for (auto &col : columns.Logical()) {
		if (col.Logical() == removed_index || removed_columns.count(col.Logical())) {
			continue;
		}
		create_info->columns.AddColumn(col.Copy());
	}
	if (create_info->columns.empty()) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	auto adjusted_indices = column_dependency_manager.RemoveColumn(removed_index, columns.LogicalColumnCount());
	// handle constraints for the new table
	D_ASSERT(constraints.size() == bound_constraints.size());
	for (idx_t constr_idx = 0; constr_idx < constraints.size(); constr_idx++) {
		auto &constraint = constraints[constr_idx];
		auto &bound_constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null_constraint = (BoundNotNullConstraint &)*bound_constraint;
			auto not_null_index = columns.PhysicalToLogical(not_null_constraint.index);
			if (not_null_index != removed_index) {
				// the constraint is not about this column: we need to copy it
				// we might need to shift the index back by one though, to account for the removed column
				auto new_index = adjusted_indices[not_null_index.index];
				create_info->constraints.push_back(make_unique<NotNullConstraint>(new_index));
			}
			break;
		}
		case ConstraintType::CHECK: {
			// CHECK constraint
			auto &bound_check = (BoundCheckConstraint &)*bound_constraint;
			// check if the removed column is part of the check constraint
			auto physical_index = columns.LogicalToPhysical(removed_index);
			if (bound_check.bound_columns.find(physical_index) != bound_check.bound_columns.end()) {
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
			if (unique.index.index != DConstants::INVALID_INDEX) {
				if (unique.index == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column);
				}
				unique.index = adjusted_indices[unique.index.index];
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
	if (columns.GetColumn(LogicalIndex(removed_index)).Generated()) {
		return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
		                                      storage);
	}
	auto new_storage =
	    make_shared<DataTable>(context, *storage, columns.LogicalToPhysical(LogicalIndex(removed_index)).index);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetDefault(ClientContext &context, SetDefaultInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	auto default_idx = GetColumnIndex(info.column_name);
	if (default_idx == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Cannot SET DEFAULT for rowid column");
	}

	// Copy all the columns, changing the value of the one that was specified by 'column_name'
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (default_idx == col.Oid()) {
			// set the default value of this column
			if (copy.Generated()) {
				throw BinderException("Cannot SET DEFAULT for generated column \"%s\"", col.Name());
			}
			copy.SetDefaultValue(info.expression ? info.expression->Copy() : nullptr);
		}
		create_info->columns.AddColumn(move(copy));
	}
	// Copy all the constraints
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetNotNull(ClientContext &context, SetNotNullInfo &info) {

	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->columns = columns.Copy();

	auto not_null_idx = GetColumnIndexLogical(info.column_name);
	if (columns.GetColumn(LogicalIndex(not_null_idx)).Generated()) {
		throw BinderException("Unsupported constraint for generated column!");
	}
	bool has_not_null = false;
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = (NotNullConstraint &)*constraint;
			if (not_null.index == not_null_idx) {
				has_not_null = true;
			}
		}
		create_info->constraints.push_back(move(constraint));
	}
	if (!has_not_null) {
		create_info->constraints.push_back(make_unique<NotNullConstraint>(not_null_idx));
	}
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));

	// Early return
	if (has_not_null) {
		return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
		                                      storage);
	}

	// Return with new storage info. Note that we need the bound column index here.
	auto new_storage = make_shared<DataTable>(
	    context, *storage, make_unique<BoundNotNullConstraint>(columns.LogicalToPhysical(LogicalIndex(not_null_idx))));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::DropNotNull(ClientContext &context, DropNotNullInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->columns = columns.Copy();

	auto not_null_idx = GetColumnIndexLogical(info.column_name);
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		// Skip/drop not_null
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = (NotNullConstraint &)*constraint;
			if (not_null.index == not_null_idx) {
				continue;
			}
		}
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info) {
	if (info.target_type.id() == LogicalTypeId::USER) {
		auto &catalog = Catalog::GetCatalog(context);
		info.target_type = catalog.GetType(context, schema->name, UserType::GetTypeName(info.target_type));
	}
	auto change_idx = GetColumnIndexLogical(info.column_name);
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (change_idx == col.Logical()) {
			// set the type of this column
			if (copy.Generated()) {
				throw NotImplementedException("Changing types of generated columns is not supported yet");
			}
			copy.SetType(info.target_type);
		}
		// TODO: check if the generated_expression breaks, only delete it if it does
		if (copy.Generated() && column_dependency_manager.IsDependencyOf(col.Logical(), change_idx)) {
			throw BinderException(
			    "This column is referenced by the generated column \"%s\", so its type can not be changed",
			    copy.Name());
		}
		create_info->columns.AddColumn(move(copy));
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		switch (constraint->type) {
		case ConstraintType::CHECK: {
			auto &bound_check = (BoundCheckConstraint &)*bound_constraints[i];
			auto physical_index = columns.LogicalToPhysical(change_idx);
			if (bound_check.bound_columns.find(physical_index) != bound_check.bound_columns.end()) {
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
			auto key_set = bfk.pk_key_set;
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				key_set = bfk.fk_key_set;
			} else if (bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < bfk.info.fk_keys.size(); i++) {
					key_set.insert(bfk.info.fk_keys[i]);
				}
			}
			if (key_set.find(columns.LogicalToPhysical(change_idx)) != key_set.end()) {
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
	vector<column_t> storage_oids;
	if (bound_columns.empty()) {
		storage_oids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	// transform to storage_oid
	else {
		for (idx_t i = 0; i < bound_columns.size(); i++) {
			storage_oids.push_back(columns.LogicalToPhysical(LogicalIndex(bound_columns[i])).index);
		}
	}

	auto new_storage =
	    make_shared<DataTable>(context, *storage, columns.LogicalToPhysical(LogicalIndex(change_idx)).index,
	                           info.target_type, move(storage_oids), *bound_expression);
	auto result =
	    make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), new_storage);
	return move(result);
}

unique_ptr<CatalogEntry> TableCatalogEntry::AddForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info) {
	D_ASSERT(info.type == AlterForeignKeyType::AFT_ADD);
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	create_info->columns = columns.Copy();
	for (idx_t i = 0; i < constraints.size(); i++) {
		create_info->constraints.push_back(constraints[i]->Copy());
	}
	ForeignKeyInfo fk_info;
	fk_info.type = ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
	fk_info.schema = info.schema;
	fk_info.table = info.fk_table;
	fk_info.pk_keys = info.pk_keys;
	fk_info.fk_keys = info.fk_keys;
	create_info->constraints.push_back(
	    make_unique<ForeignKeyConstraint>(info.pk_columns, info.fk_columns, move(fk_info)));

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));

	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::DropForeignKeyConstraint(ClientContext &context,
                                                                     AlterForeignKeyInfo &info) {
	D_ASSERT(info.type == AlterForeignKeyType::AFT_DELETE);
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	create_info->columns = columns.Copy();
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

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));

	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) {
	return columns.GetColumnMutable(name);
}

vector<LogicalType> TableCatalogEntry::GetTypes() {
	vector<LogicalType> types;
	for (auto &col : columns.Physical()) {
		types.push_back(col.Type());
	}
	return types;
}

void TableCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);

	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	columns.Serialize(writer);
	writer.WriteSerializableList(constraints);
	writer.Finalize();
}

unique_ptr<CreateTableInfo> TableCatalogEntry::Deserialize(Deserializer &source, ClientContext &context) {
	auto info = make_unique<CreateTableInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->table = reader.ReadRequired<string>();
	info->columns = ColumnList::Deserialize(reader);
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
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = (NotNullConstraint &)*constraint;
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = (UniqueConstraint &)*constraint;
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
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

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteOptionallyQuoted(column.Name()) << " ";
		ss << column.Type().ToString();
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
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
		if (column.DefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue()->ToString() << ")";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
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
	create_info->columns = columns.Copy();

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
	storage->info->table = name;
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
	for (auto &col : columns.Logical()) {
		if (col.Name() == column_name) {
			// No need to alter storage, removed column is generated column
			if (col.Generated()) {
				return;
			}
			removed_index = col.Oid();
			break;
		}
	}
	D_ASSERT(removed_index != DConstants::INVALID_INDEX);
	storage->CommitDropColumn(columns.LogicalToPhysical(LogicalIndex(removed_index)).index);
}

void TableCatalogEntry::CommitDrop() {
	storage->CommitDropTable();
}

} // namespace duckdb
