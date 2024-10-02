#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/index_map.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/alter_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

void AddDataTableIndex(DataTable &storage, const ColumnList &columns, const vector<PhysicalIndex> &keys,
                       IndexConstraintType constraint_type, const IndexStorageInfo &info) {

	// fetch types and create expressions for the index from the columns
	vector<column_t> column_ids;
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<Expression>> bound_expressions;
	idx_t key_nr = 0;
	column_ids.reserve(keys.size());
	for (auto &physical_key : keys) {
		auto &column = columns.GetColumn(physical_key);
		D_ASSERT(!column.Generated());
		unbound_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(column.Name(), column.Type(), ColumnBinding(0, column_ids.size())));

		bound_expressions.push_back(make_uniq<BoundReferenceExpression>(column.Type(), key_nr++));
		column_ids.push_back(column.StorageOid());
	}
	// create an adaptive radix tree around the expressions
	auto art = make_uniq<ART>(info.name, constraint_type, column_ids, TableIOManager::Get(storage),
	                          std::move(unbound_expressions), storage.db, nullptr, info);
	if (!info.IsValid() && !info.name.empty() && !storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}
	storage.AddIndex(std::move(art));
}

void AddDataTableIndex(DataTable &storage, const ColumnList &columns, vector<LogicalIndex> &keys,
                       IndexConstraintType constraint_type, const IndexStorageInfo &info) {
	vector<PhysicalIndex> new_keys;
	new_keys.reserve(keys.size());
	for (auto &logical_key : keys) {
		new_keys.push_back(columns.LogicalToPhysical(logical_key));
	}
	AddDataTableIndex(storage, columns, new_keys, constraint_type, info);
}

IndexStorageInfo GetIndexInfo(const IndexConstraintType &constraint_type, const bool v1_0_0_storage,
                              unique_ptr<CreateInfo> &create_info, const idx_t identifier) {

	auto &create_table_info = create_info->Cast<CreateTableInfo>();
	auto constraint_name = EnumUtil::ToString(constraint_type) + "_";
	auto name = constraint_name + create_table_info.table + "_" + to_string(identifier);
	IndexStorageInfo info(name);
	if (!v1_0_0_storage) {
		info.options.emplace("v1_0_0_storage", v1_0_0_storage);
	}
	return info;
}

vector<PhysicalIndex> GetUniqueConstraintKeys(const ColumnList &columns, const UniqueConstraint &constraint) {
	vector<PhysicalIndex> indexes;
	if (constraint.HasIndex()) {
		indexes.push_back(columns.LogicalToPhysical(constraint.GetIndex()));
	} else {
		for (auto &keyname : constraint.GetColumnNames()) {
			indexes.push_back(columns.GetColumn(keyname).Physical());
		}
	}
	return indexes;
}

DuckTableEntry::DuckTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
                               shared_ptr<DataTable> inherited_storage)
    : TableCatalogEntry(catalog, schema, info.Base()), storage(std::move(inherited_storage)),
      column_dependency_manager(std::move(info.column_dependency_manager)) {

	if (storage) {
		if (!info.indexes.empty()) {
			storage->SetIndexStorageInfo(std::move(info.indexes));
		}
		return;
	}

	// create the physical storage
	vector<ColumnDefinition> storage_columns;
	for (auto &col_def : columns.Physical()) {
		storage_columns.push_back(col_def.Copy());
	}
	storage = make_shared_ptr<DataTable>(catalog.GetAttached(), StorageManager::Get(catalog).GetTableIOManager(&info),
	                                     schema.name, name, std::move(storage_columns), std::move(info.data));

	// create the unique indexes for the UNIQUE and PRIMARY KEY and FOREIGN KEY constraints
	idx_t indexes_idx = 0;
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto &constraint = constraints[i];
		if (constraint->type == ConstraintType::UNIQUE) {
			// unique constraint: create a unique index
			auto &unique = constraint->Cast<UniqueConstraint>();
			IndexConstraintType constraint_type = IndexConstraintType::UNIQUE;
			if (unique.is_primary_key) {
				constraint_type = IndexConstraintType::PRIMARY;
			}
			auto unique_keys = GetUniqueConstraintKeys(columns, unique);
			if (info.indexes.empty()) {
				auto index_storage_info = GetIndexInfo(constraint_type, false, info.base, i);
				AddDataTableIndex(*storage, columns, unique_keys, constraint_type, index_storage_info);
				continue;
			}

			// We read the index from an old storage version applying a dummy name.
			if (info.indexes[indexes_idx].name.empty()) {
				auto name_info = GetIndexInfo(constraint_type, true, info.base, i);
				info.indexes[indexes_idx].name = name_info.name;
			}

			// now add the index
			AddDataTableIndex(*storage, columns, unique_keys, constraint_type, info.indexes[indexes_idx++]);
			continue;
		}

		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			// foreign key constraint: create a foreign key index
			auto &bfk = constraint->Cast<ForeignKeyConstraint>();
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {

				if (info.indexes.empty()) {
					auto constraint_type = IndexConstraintType::FOREIGN;
					auto index_storage_info = GetIndexInfo(constraint_type, false, info.base, i);
					AddDataTableIndex(*storage, columns, bfk.info.fk_keys, constraint_type, index_storage_info);
					continue;
				}

				// We read the index from an old storage version applying a dummy name.
				if (info.indexes[indexes_idx].name.empty()) {
					auto name_info = GetIndexInfo(IndexConstraintType::FOREIGN, true, info.base, i);
					info.indexes[indexes_idx].name = name_info.name;
				}

				// now add the index
				AddDataTableIndex(*storage, columns, bfk.info.fk_keys, IndexConstraintType::FOREIGN,
				                  info.indexes[indexes_idx++]);
			}
		}
	}

	if (!info.indexes.empty()) {
		storage->SetIndexStorageInfo(std::move(info.indexes));
	}
}

unique_ptr<BaseStatistics> DuckTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	auto &column = columns.GetColumn(LogicalIndex(column_id));
	if (column.Generated()) {
		return nullptr;
	}
	return storage->GetStatistics(context, column.StorageOid());
}

unique_ptr<CatalogEntry> DuckTableEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	if (transaction.HasContext()) {
		return AlterEntry(transaction.GetContext(), info);
	}
	if (info.type != AlterType::ALTER_TABLE) {
		return CatalogEntry::AlterEntry(transaction, info);
	}

	auto &table_info = info.Cast<AlterTableInfo>();
	if (table_info.alter_table_type != AlterTableType::FOREIGN_KEY_CONSTRAINT) {
		return CatalogEntry::AlterEntry(transaction, info);
	}

	auto &foreign_key_constraint_info = table_info.Cast<AlterForeignKeyInfo>();
	if (foreign_key_constraint_info.type != AlterForeignKeyType::AFT_ADD) {
		return CatalogEntry::AlterEntry(transaction, info);
	}

	// We add foreign key constraints without a client context during checkpoint loading.
	return AddForeignKeyConstraint(nullptr, foreign_key_constraint_info);
}

unique_ptr<CatalogEntry> DuckTableEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	D_ASSERT(!internal);

	// Column comments have a special alter type
	if (info.type == AlterType::SET_COLUMN_COMMENT) {
		auto &comment_on_column_info = info.Cast<SetColumnCommentInfo>();
		return SetColumnComment(context, comment_on_column_info);
	}

	if (info.type != AlterType::ALTER_TABLE) {
		throw CatalogException("Can only modify table with ALTER TABLE statement");
	}
	auto &table_info = info.Cast<AlterTableInfo>();
	switch (table_info.alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = table_info.Cast<RenameColumnInfo>();
		return RenameColumn(context, rename_info);
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename_info = table_info.Cast<RenameTableInfo>();
		auto copied_table = Copy(context);
		copied_table->name = rename_info.new_table_name;
		storage->SetTableName(rename_info.new_table_name);
		return copied_table;
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = table_info.Cast<AddColumnInfo>();
		return AddColumn(context, add_info);
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = table_info.Cast<RemoveColumnInfo>();
		return RemoveColumn(context, remove_info);
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_default_info = table_info.Cast<SetDefaultInfo>();
		return SetDefault(context, set_default_info);
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_type_info = table_info.Cast<ChangeColumnTypeInfo>();
		return ChangeColumnType(context, change_type_info);
	}
	case AlterTableType::FOREIGN_KEY_CONSTRAINT: {
		auto &foreign_key_constraint_info = table_info.Cast<AlterForeignKeyInfo>();
		if (foreign_key_constraint_info.type == AlterForeignKeyType::AFT_ADD) {
			return AddForeignKeyConstraint(context, foreign_key_constraint_info);
		} else {
			return DropForeignKeyConstraint(context, foreign_key_constraint_info);
		}
	}
	case AlterTableType::SET_NOT_NULL: {
		auto &set_not_null_info = table_info.Cast<SetNotNullInfo>();
		return SetNotNull(context, set_not_null_info);
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_not_null_info = table_info.Cast<DropNotNullInfo>();
		return DropNotNull(context, drop_not_null_info);
	}
	default:
		throw InternalException("Unrecognized alter table type!");
	}
}

void DuckTableEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
	D_ASSERT(!internal);
	D_ASSERT(info.type == AlterType::ALTER_TABLE);
	auto &table_info = info.Cast<AlterTableInfo>();
	switch (table_info.alter_table_type) {
	case AlterTableType::RENAME_TABLE: {
		storage->SetTableName(this->name);
		break;
	default:
		break;
	}
	}
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (colref.column_names.back() == info.old_name) {
			colref.column_names.back() = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

unique_ptr<CatalogEntry> DuckTableEntry::RenameColumn(ClientContext &context, RenameColumnInfo &info) {
	auto rename_idx = GetColumnIndex(info.old_name);
	if (rename_idx.index == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Cannot rename rowid column");
	}
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (rename_idx == col.Logical()) {
			copy.SetName(info.new_name);
		}
		if (col.Generated() && column_dependency_manager.IsDependencyOf(col.Logical(), rename_idx)) {
			RenameExpression(copy.GeneratedExpressionMutable(), info);
		}
		create_info->columns.AddColumn(std::move(copy));
	}
	for (idx_t c_idx = 0; c_idx < constraints.size(); c_idx++) {
		auto copy = constraints[c_idx]->Copy();
		switch (copy->type) {
		case ConstraintType::NOT_NULL:
			// NOT NULL constraint: no adjustments necessary
			break;
		case ConstraintType::CHECK: {
			// CHECK constraint: need to rename column references that refer to the renamed column
			auto &check = copy->Cast<CheckConstraint>();
			RenameExpression(*check.expression, info);
			break;
		}
		case ConstraintType::UNIQUE: {
			// UNIQUE constraint: possibly need to rename columns
			auto &unique = copy->Cast<UniqueConstraint>();
			for (auto &column_name : unique.GetColumnNamesMutable()) {
				if (column_name == info.old_name) {
					column_name = info.new_name;
				}
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			// FOREIGN KEY constraint: possibly need to rename columns
			auto &fk = copy->Cast<ForeignKeyConstraint>();
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
		create_info->constraints.push_back(std::move(copy));
	}
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::AddColumn(ClientContext &context, AddColumnInfo &info) {
	auto col_name = info.new_column.GetName();

	// We're checking for the opposite condition (ADD COLUMN IF _NOT_ EXISTS ...).
	if (info.if_column_not_exists && ColumnExists(col_name)) {
		return nullptr;
	}

	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;

	for (auto &col : columns.Logical()) {
		create_info->columns.AddColumn(col.Copy());
	}
	for (auto &constraint : constraints) {
		create_info->constraints.push_back(constraint->Copy());
	}
	auto binder = Binder::CreateBinder(context);
	binder->BindLogicalType(info.new_column.TypeMutable(), &catalog, schema.name);
	info.new_column.SetOid(columns.LogicalColumnCount());
	info.new_column.SetStorageOid(columns.PhysicalColumnCount());
	auto col = info.new_column.Copy();

	create_info->columns.AddColumn(std::move(col));

	vector<unique_ptr<Expression>> bound_defaults;
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema, bound_defaults);
	auto new_storage = make_shared_ptr<DataTable>(context, *storage, info.new_column, *bound_defaults.back());
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, new_storage);
}

void DuckTableEntry::UpdateConstraintsOnColumnDrop(const LogicalIndex &removed_index,
                                                   const vector<LogicalIndex> &adjusted_indices,
                                                   const RemoveColumnInfo &info, CreateTableInfo &create_info,
                                                   const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                                   bool is_generated) {
	// handle constraints for the new table
	D_ASSERT(constraints.size() == bound_constraints.size());
	for (idx_t constr_idx = 0; constr_idx < constraints.size(); constr_idx++) {
		auto &constraint = constraints[constr_idx];
		auto &bound_constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null_constraint = bound_constraint->Cast<BoundNotNullConstraint>();
			auto not_null_index = columns.PhysicalToLogical(not_null_constraint.index);
			if (not_null_index != removed_index) {
				// the constraint is not about this column: we need to copy it
				// we might need to shift the index back by one though, to account for the removed column
				auto new_index = adjusted_indices[not_null_index.index];
				create_info.constraints.push_back(make_uniq<NotNullConstraint>(new_index));
			}
			break;
		}
		case ConstraintType::CHECK: {
			// Generated columns can not be part of an index
			// CHECK constraint
			auto &bound_check = bound_constraint->Cast<BoundCheckConstraint>();
			// check if the removed column is part of the check constraint
			if (is_generated) {
				// generated columns can not be referenced by constraints, we can just add the constraint back
				create_info.constraints.push_back(constraint->Copy());
				break;
			}
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
				create_info.constraints.push_back(constraint->Copy());
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto copy = constraint->Copy();
			auto &unique = copy->Cast<UniqueConstraint>();
			if (unique.HasIndex()) {
				if (unique.GetIndex() == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column);
				}
				unique.SetIndex(adjusted_indices[unique.GetIndex().index]);
			}
			create_info.constraints.push_back(std::move(copy));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto copy = constraint->Copy();
			auto &fk = copy->Cast<ForeignKeyConstraint>();
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
			create_info.constraints.push_back(std::move(copy));
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
	}
}

unique_ptr<CatalogEntry> DuckTableEntry::RemoveColumn(ClientContext &context, RemoveColumnInfo &info) {
	auto removed_index = GetColumnIndex(info.removed_column, info.if_column_exists);
	if (!removed_index.IsValid()) {
		if (!info.if_column_exists) {
			throw CatalogException("Cannot drop column: rowid column cannot be dropped");
		}
		return nullptr;
	}

	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;

	logical_index_set_t removed_columns;
	if (column_dependency_manager.HasDependents(removed_index)) {
		removed_columns = column_dependency_manager.GetDependents(removed_index);
	}
	if (!removed_columns.empty() && !info.cascade) {
		throw CatalogException("Cannot drop column: column is a dependency of 1 or more generated column(s)");
	}
	bool dropped_column_is_generated = false;
	for (auto &col : columns.Logical()) {
		if (col.Logical() == removed_index || removed_columns.count(col.Logical())) {
			if (col.Generated()) {
				dropped_column_is_generated = true;
			}
			continue;
		}
		create_info->columns.AddColumn(col.Copy());
	}
	if (create_info->columns.empty()) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	auto adjusted_indices = column_dependency_manager.RemoveColumn(removed_index, columns.LogicalColumnCount());

	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(constraints, name, columns);

	UpdateConstraintsOnColumnDrop(removed_index, adjusted_indices, info, *create_info, bound_constraints,
	                              dropped_column_is_generated);

	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	if (columns.GetColumn(LogicalIndex(removed_index)).Generated()) {
		return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
	}
	auto new_storage =
	    make_shared_ptr<DataTable>(context, *storage, columns.LogicalToPhysical(LogicalIndex(removed_index)).index);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, new_storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::SetDefault(ClientContext &context, SetDefaultInfo &info) {
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;
	create_info->tags = tags;
	auto default_idx = GetColumnIndex(info.column_name);
	if (default_idx.index == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Cannot SET DEFAULT for rowid column");
	}

	// Copy all the columns, changing the value of the one that was specified by 'column_name'
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (default_idx == col.Logical()) {
			// set the default value of this column
			if (copy.Generated()) {
				throw BinderException("Cannot SET DEFAULT for generated column \"%s\"", col.Name());
			}
			copy.SetDefaultValue(info.expression ? info.expression->Copy() : nullptr);
		}
		create_info->columns.AddColumn(std::move(copy));
	}
	// Copy all the constraints
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(std::move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::SetNotNull(ClientContext &context, SetNotNullInfo &info) {

	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;
	create_info->tags = tags;
	create_info->columns = columns.Copy();

	auto not_null_idx = GetColumnIndex(info.column_name);
	if (columns.GetColumn(LogicalIndex(not_null_idx)).Generated()) {
		throw BinderException("Unsupported constraint for generated column!");
	}
	bool has_not_null = false;
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			if (not_null.index == not_null_idx) {
				has_not_null = true;
			}
		}
		create_info->constraints.push_back(std::move(constraint));
	}
	if (!has_not_null) {
		create_info->constraints.push_back(make_uniq<NotNullConstraint>(not_null_idx));
	}
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);

	// Early return
	if (has_not_null) {
		return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
	}

	// Return with new storage info. Note that we need the bound column index here.
	auto new_storage = make_shared_ptr<DataTable>(
	    context, *storage, make_uniq<BoundNotNullConstraint>(columns.LogicalToPhysical(LogicalIndex(not_null_idx))));
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, new_storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::DropNotNull(ClientContext &context, DropNotNullInfo &info) {
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;
	create_info->tags = tags;
	create_info->columns = columns.Copy();

	auto not_null_idx = GetColumnIndex(info.column_name);
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		// Skip/drop not_null
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			if (not_null.index == not_null_idx) {
				continue;
			}
		}
		create_info->constraints.push_back(std::move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info) {
	auto binder = Binder::CreateBinder(context);
	binder->BindLogicalType(info.target_type, &catalog, schema.name);
	auto change_idx = GetColumnIndex(info.column_name);
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;

	auto bound_constraints = binder->BindConstraints(constraints, name, columns);
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
		create_info->columns.AddColumn(std::move(copy));
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		switch (constraint->type) {
		case ConstraintType::CHECK: {
			auto &bound_check = bound_constraints[i]->Cast<BoundCheckConstraint>();
			auto physical_index = columns.LogicalToPhysical(change_idx);
			if (bound_check.bound_columns.find(physical_index) != bound_check.bound_columns.end()) {
				throw BinderException("Cannot change the type of a column that has a CHECK constraint specified");
			}
			break;
		}
		case ConstraintType::NOT_NULL:
			break;
		case ConstraintType::UNIQUE: {
			auto &bound_unique = bound_constraints[i]->Cast<BoundUniqueConstraint>();
			if (bound_unique.key_set.find(change_idx) != bound_unique.key_set.end()) {
				throw BinderException(
				    "Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = bound_constraints[i]->Cast<BoundForeignKeyConstraint>();
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
		create_info->constraints.push_back(std::move(constraint));
	}

	// bind the specified expression
	vector<LogicalIndex> bound_columns;
	AlterBinder expr_binder(*binder, context, *this, bound_columns, info.target_type);
	auto expression = info.expression->Copy();
	auto bound_expression = expr_binder.Bind(expression);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	vector<column_t> storage_oids;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		storage_oids.push_back(columns.LogicalToPhysical(bound_columns[i]).index);
	}
	if (storage_oids.empty()) {
		storage_oids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto new_storage =
	    make_shared_ptr<DataTable>(context, *storage, columns.LogicalToPhysical(LogicalIndex(change_idx)).index,
	                               info.target_type, std::move(storage_oids), *bound_expression);
	auto result = make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, new_storage);
	return std::move(result);
}

unique_ptr<CatalogEntry> DuckTableEntry::SetColumnComment(ClientContext &context, SetColumnCommentInfo &info) {
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;
	create_info->tags = tags;
	auto default_idx = GetColumnIndex(info.column_name);
	if (default_idx.index == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Cannot SET DEFAULT for rowid column");
	}

	// Copy all the columns, changing the value of the one that was specified by 'column_name'
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (default_idx == col.Logical()) {
			copy.SetComment(info.comment_value);
		}
		create_info->columns.AddColumn(std::move(copy));
	}
	// Copy all the constraints
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(std::move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::AddForeignKeyConstraint(optional_ptr<ClientContext> context,
                                                                 AlterForeignKeyInfo &info) {
	D_ASSERT(info.type == AlterForeignKeyType::AFT_ADD);
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;

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
	    make_uniq<ForeignKeyConstraint>(info.pk_columns, info.fk_columns, std::move(fk_info)));

	unique_ptr<BoundCreateTableInfo> bound_create_info;
	if (context) {
		auto binder = Binder::CreateBinder(*context);
		bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	} else {
		bound_create_info = Binder::BindCreateTableCheckpoint(std::move(create_info), schema);
	}
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::DropForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info) {
	D_ASSERT(info.type == AlterForeignKeyType::AFT_DELETE);
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->temporary = temporary;
	create_info->comment = comment;
	create_info->tags = tags;

	create_info->columns = columns.Copy();
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			ForeignKeyConstraint &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && fk.info.table == info.fk_table) {
				continue;
			}
		}
		create_info->constraints.push_back(std::move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

unique_ptr<CatalogEntry> DuckTableEntry::Copy(ClientContext &context) const {
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;
	create_info->tags = tags;
	create_info->columns = columns.Copy();

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(std::move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

void DuckTableEntry::SetAsRoot() {
	storage->SetAsRoot();
	storage->SetTableName(name);
}

void DuckTableEntry::CommitAlter(string &column_name) {
	D_ASSERT(!column_name.empty());
	optional_idx removed_index;
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
	storage->CommitDropColumn(columns.LogicalToPhysical(LogicalIndex(removed_index.GetIndex())).index);
}

void DuckTableEntry::CommitDrop() {
	storage->CommitDropTable();
}

DataTable &DuckTableEntry::GetStorage() {
	return *storage;
}

TableFunction DuckTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = make_uniq<TableScanBindData>(*this);
	return TableScanFunction::GetFunction();
}

vector<ColumnSegmentInfo> DuckTableEntry::GetColumnSegmentInfo() {
	return storage->GetColumnSegmentInfo();
}

TableStorageInfo DuckTableEntry::GetStorageInfo(ClientContext &context) {
	return storage->GetStorageInfo();
}

} // namespace duckdb
