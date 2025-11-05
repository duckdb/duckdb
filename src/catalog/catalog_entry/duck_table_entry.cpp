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
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

IndexStorageInfo GetIndexInfo(const IndexConstraintType type, const bool v1_0_0_storage, unique_ptr<CreateInfo> &info,
                              const idx_t id) {
	auto &table_info = info->Cast<CreateTableInfo>();
	auto constraint_name = EnumUtil::ToString(type) + "_";
	auto name = constraint_name + table_info.table + "_" + to_string(id);
	IndexStorageInfo index_info(name);
	if (!v1_0_0_storage) {
		index_info.options.emplace("v1_0_0_storage", v1_0_0_storage);
	}
	return index_info;
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
	vector<ColumnDefinition> column_defs;
	for (auto &col_def : columns.Physical()) {
		if (TypeVisitor::Contains(col_def.Type(), LogicalTypeId::VARIANT)) {
			throw NotImplementedException("A table cannot be created from a VARIANT column yet");
		}
		column_defs.push_back(col_def.Copy());
	}
	storage = make_shared_ptr<DataTable>(catalog.GetAttached(), StorageManager::Get(catalog).GetTableIOManager(&info),
	                                     schema.name, name, std::move(column_defs), std::move(info.data));

	// Create the unique indexes for the UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints.
	idx_t indexes_idx = 0;
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto &constraint = constraints[i];
		if (constraint->type == ConstraintType::UNIQUE) {
			// UNIQUE constraint: Create a unique index.
			auto &unique = constraint->Cast<UniqueConstraint>();
			IndexConstraintType constraint_type = IndexConstraintType::UNIQUE;
			if (unique.is_primary_key) {
				constraint_type = IndexConstraintType::PRIMARY;
			}

			auto column_indexes = unique.GetLogicalIndexes(columns);
			if (info.indexes.empty()) {
				auto index_info = GetIndexInfo(constraint_type, false, info.base, i);
				storage->AddIndex(columns, column_indexes, constraint_type, index_info);
				continue;
			}

			// We read the index from an old storage version applying a dummy name.
			if (info.indexes[indexes_idx].name.empty()) {
				auto name_info = GetIndexInfo(constraint_type, true, info.base, i);
				info.indexes[indexes_idx].name = name_info.name;
			}

			// Now we can add the index.
			storage->AddIndex(columns, column_indexes, constraint_type, info.indexes[indexes_idx++]);
			continue;
		}

		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			// Create a FOREIGN KEY index.
			auto &bfk = constraint->Cast<ForeignKeyConstraint>();
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				vector<LogicalIndex> column_indexes;
				for (const auto &physical_index : bfk.info.fk_keys) {
					auto &col = columns.GetColumn(physical_index);
					column_indexes.push_back(col.Logical());
				}

				if (info.indexes.empty()) {
					auto constraint_type = IndexConstraintType::FOREIGN;
					auto index_info = GetIndexInfo(constraint_type, false, info.base, i);
					storage->AddIndex(columns, column_indexes, constraint_type, index_info);
					continue;
				}

				// We read the index from an old storage version applying a dummy name.
				if (info.indexes[indexes_idx].name.empty()) {
					auto name_info = GetIndexInfo(IndexConstraintType::FOREIGN, true, info.base, i);
					info.indexes[indexes_idx].name = name_info.name;
				}

				// Now we can add the index.
				storage->AddIndex(columns, column_indexes, IndexConstraintType::FOREIGN, info.indexes[indexes_idx++]);
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

unique_ptr<BlockingSample> DuckTableEntry::GetSample() {
	return storage->GetSample();
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
	return AddForeignKeyConstraint(foreign_key_constraint_info);
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
	case AlterTableType::RENAME_FIELD: {
		auto &rename_info = table_info.Cast<RenameFieldInfo>();
		return RenameField(context, rename_info);
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
	case AlterTableType::ADD_FIELD: {
		auto &add_info = table_info.Cast<AddFieldInfo>();
		return AddField(context, add_info);
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = table_info.Cast<RemoveColumnInfo>();
		return RemoveColumn(context, remove_info);
	}
	case AlterTableType::REMOVE_FIELD: {
		auto &remove_info = table_info.Cast<RemoveFieldInfo>();
		return RemoveField(context, remove_info);
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
			return AddForeignKeyConstraint(foreign_key_constraint_info);
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
	case AlterTableType::ADD_CONSTRAINT: {
		auto &add_constraint_info = table_info.Cast<AddConstraintInfo>();
		return AddConstraint(context, add_constraint_info);
	}
	case AlterTableType::SET_PARTITIONED_BY:
		throw NotImplementedException("SET PARTITIONED BY is not supported for DuckDB tables");
	case AlterTableType::SET_SORTED_BY:
		throw NotImplementedException("SET SORTED BY is not supported for DuckDB tables");
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

static void RenameExpression(ParsedExpression &root_expr, RenameColumnInfo &info) {
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(root_expr, [&](ColumnRefExpression &colref) {
		if (colref.column_names.back() == info.old_name) {
			colref.column_names.back() = info.new_name;
		}
	});
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

struct StructMappingInfo {
	LogicalType new_type;
	unique_ptr<ParsedExpression> default_value;
	ErrorData error;
};

unique_ptr<ParsedExpression> PackExpression(unique_ptr<ParsedExpression> expr, string name) {
	expr->SetAlias(std::move(name));
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(expr));
	auto res = make_uniq<FunctionExpression>("struct_pack", std::move(children));
	return std::move(res);
}

static child_list_t<LogicalType> GetChildList(const LogicalType &type) {
	child_list_t<LogicalType> child_types;
	switch (type.id()) {
	case LogicalTypeId::LIST: {
		child_types.emplace_back("list", ListType::GetChildType(type));
		break;
	}
	case LogicalTypeId::MAP: {
		child_types.emplace_back("key", MapType::KeyType(type));
		child_types.emplace_back("value", MapType::ValueType(type));
		break;
	}
	case LogicalTypeId::STRUCT: {
		child_types = StructType::GetChildTypes(type);
		break;
	}
	default:
		throw BinderException("Can't ConstructMapping for type '%s'", type.ToString());
	}
	return child_types;
}

static LogicalType ConstructNewType(const LogicalType &original_type, child_list_t<LogicalType> new_child_types) {
	switch (original_type.id()) {
	case LogicalTypeId::STRUCT: {
		return LogicalType::STRUCT(std::move(new_child_types));
	}
	case LogicalTypeId::LIST: {
		D_ASSERT(new_child_types.size() == 1);
		return LogicalType::LIST(new_child_types[0].second);
	}
	case LogicalTypeId::MAP: {
		D_ASSERT(new_child_types.size() == 2);
		return LogicalType::MAP(new_child_types[0].second, new_child_types[1].second);
	}
	default:
		throw BinderException("Type '%s' not supported for ADD COLUMN", original_type.ToString());
	}
}

Value ConstructMapping(const string &name, const LogicalType &type) {
	if (!type.IsNested()) {
		return Value(name);
	}
	child_list_t<Value> child_mapping;
	auto child_types = GetChildList(type);
	for (auto &entry : child_types) {
		auto mapping_value = ConstructMapping(entry.first, entry.second);
		if (entry.second.IsNested()) {
			child_list_t<Value> child_values;
			child_values.emplace_back(string(), Value(entry.first));
			child_values.emplace_back(string(), std::move(mapping_value));
			mapping_value = Value::STRUCT(std::move(child_values));
		}
		child_mapping.emplace_back(entry.first, std::move(mapping_value));
	}
	return Value::STRUCT(std::move(child_mapping));
}

StructMappingInfo AddFieldToStruct(const LogicalType &type, const vector<string> &column_path,
                                   const ColumnDefinition &new_field, idx_t depth = 0) {
	if (!type.IsNested()) {
		throw BinderException("Column '%s' is not a nested type, ADD COLUMN can only be used on nested types",
		                      column_path[depth]);
	}

	StructMappingInfo result;
	auto child_list = GetChildList(type);
	auto &current_component = column_path[depth];
	bool last_entry = depth + 1 == column_path.size();

	if (last_entry) {
		if (type.id() != LogicalTypeId::STRUCT) {
			throw BinderException("Column %s is not a struct - ALTER TABLE can only add fields to structs",
			                      current_component);
		}
		// root path - we are adding at this level
		// check if a field with this name already exists
		for (auto &entry : child_list) {
			if (StringUtil::CIEquals(entry.first, new_field.Name())) {
				// already exists!
				result.error = ErrorData(CatalogException("Duplicate field \"%s\" - field already exists in struct %s",
				                                          new_field.Name(), current_component));
				return result;
			}
		}
		// add the new type
		child_list.emplace_back(new_field.Name(), new_field.Type());
		result.new_type = LogicalType::STRUCT(std::move(child_list));
		// set the default value
		unique_ptr<ParsedExpression> default_value;
		if (new_field.HasDefaultValue()) {
			default_value = new_field.DefaultValue().Copy();
		} else {
			default_value = make_uniq<ConstantExpression>(Value(new_field.Type()));
		}
		result.default_value = PackExpression(std::move(default_value), new_field.Name());
		return result;
	}

	// not the root path - we need to recurse
	auto &next_component = column_path[depth + 1];
	bool found = false;
	for (auto &entry : child_list) {
		if ((type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(next_component, "element")) ||
		    StringUtil::CIEquals(entry.first, next_component)) {
			// found the entry - recurse
			auto child_res = AddFieldToStruct(entry.second, column_path, new_field, depth + 1);
			if (child_res.error.HasError()) {
				return child_res;
			}
			entry.second = std::move(child_res.new_type);
			if (type.id() == LogicalTypeId::LIST) {
				result.default_value = PackExpression(std::move(child_res.default_value), "list");
			} else {
				result.default_value = PackExpression(std::move(child_res.default_value), entry.first);
			}
			found = true;
			break;
		}
	}
	if (!found) {
		throw BinderException("Sub-field %s does not exist in column %s", next_component, column_path[depth]);
	}
	result.new_type = ConstructNewType(type, std::move(child_list));
	return result;
}

unique_ptr<CatalogEntry> DuckTableEntry::AddField(ClientContext &context, AddFieldInfo &info) {
	// follow the path
	auto &col = GetColumn(info.column_path[0]);
	auto res = AddFieldToStruct(col.Type(), info.column_path, info.new_field);
	if (res.error.HasError()) {
		if (!info.if_field_not_exists) {
			res.error.Throw();
		}
		return nullptr;
	}

	// construct the struct remapping expression
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(info.column_path[0]));
	children.push_back(make_uniq<ConstantExpression>(Value(res.new_type)));
	children.push_back(make_uniq<ConstantExpression>(ConstructMapping(col.Name(), col.Type())));
	D_ASSERT(res.default_value);
	children.push_back(std::move(res.default_value));

	auto function = make_uniq<FunctionExpression>("remap_struct", std::move(children));

	ChangeColumnTypeInfo change_column_type(info.GetAlterEntryData(), info.column_path[0], std::move(res.new_type),
	                                        std::move(function));
	return ChangeColumnType(context, change_column_type);
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
				// Single-column UNIQUE constraint
				if (unique.GetIndex() == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column);
				}
				unique.SetIndex(adjusted_indices[unique.GetIndex().index]);
			} else {
				// Multi-column UNIQUE constraint - check if any column matches the one being dropped
				for (const auto &col_name : unique.GetColumnNames()) {
					if (col_name == info.removed_column) {
						// Build constraint string for error message: UNIQUE(col1, col2, ...)
						auto constraint_str = "UNIQUE(" + StringUtil::Join(unique.GetColumnNames(), ", ") + ")";
						throw CatalogException(
						    "Cannot drop column \"%s\" because it is referenced in unique constraint %s",
						    info.removed_column, constraint_str);
					}
				}
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

struct DroppedFieldMapping {
	Value mapping;
	LogicalType new_type;
	ErrorData error;
};

DroppedFieldMapping DropFieldFromStruct(const LogicalType &type, const vector<string> &column_path, idx_t depth) {
	if (!type.IsNested()) {
		throw CatalogException("Cannot drop field from column \"%s\" - not a nested type", column_path[0]);
	}
	auto &dropped_entry = column_path[depth];
	bool last_entry = depth + 1 == column_path.size();
	bool found = false;
	DroppedFieldMapping result;
	child_list_t<Value> child_mapping;
	child_list_t<LogicalType> new_type_children;
	auto child_types = GetChildList(type);

	for (auto &entry : child_types) {
		Value mapping_value;
		LogicalType type_value;
		if ((type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(dropped_entry, "element")) ||
		    StringUtil::CIEquals(entry.first, dropped_entry)) {
			// this is the entry we are dropping
			found = true;
			if (last_entry) {
				if (type.id() != LogicalTypeId::STRUCT) {
					throw CatalogException("Cannot drop field '%s' from column '%s' - it's not a struct",
					                       column_path.back(), column_path.front());
				}
				// we are dropping this entry in its entirety - just skip
				if (child_types.size() == 1) {
					throw CatalogException("Cannot drop field %s from column %s - it is the last field of the struct",
					                       column_path.back(), column_path.front());
				}
				continue;
			} else {
				// we are dropping a field in this entry - recurse
				auto child_result = DropFieldFromStruct(entry.second, column_path, depth + 1);
				if (child_result.error.HasError()) {
					// bubble up error
					return child_result;
				}
				mapping_value = std::move(child_result.mapping);
				type_value = std::move(child_result.new_type);
			}
		} else {
			// we are not adjusting this entry - copy the type and create a straightforward mapping
			mapping_value = ConstructMapping(entry.first, entry.second);
			type_value = entry.second;
		}

		if (entry.second.IsNested()) {
			child_list_t<Value> child_values;
			child_values.emplace_back(string(), Value(entry.first));
			child_values.emplace_back(string(), std::move(mapping_value));
			mapping_value = Value::STRUCT(std::move(child_values));
		}
		child_mapping.emplace_back(entry.first, std::move(mapping_value));
		new_type_children.emplace_back(entry.first, type_value);
	}

	if (!found) {
		result.error = ErrorData(CatalogException("Cannot drop field \"%s\" - it does not exist", dropped_entry));
	} else {
		result.mapping = Value::STRUCT(std::move(child_mapping));
		result.new_type = ConstructNewType(type, std::move(new_type_children));
	}
	return result;
}

unique_ptr<CatalogEntry> DuckTableEntry::RemoveField(ClientContext &context, RemoveFieldInfo &info) {
	if (!ColumnExists(info.column_path[0])) {
		if (!info.if_column_exists) {
			throw CatalogException("Cannot drop field from column \"%s\" - it does not exist", info.column_path[0]);
		}
		return nullptr;
	}
	// follow the path
	auto &col = GetColumn(info.column_path[0]);
	auto res = DropFieldFromStruct(col.Type(), info.column_path, 1);
	if (res.error.HasError()) {
		if (!info.if_column_exists) {
			res.error.Throw();
		}
		return nullptr;
	}

	// construct the struct remapping expression
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(info.column_path[0]));
	children.push_back(make_uniq<ConstantExpression>(Value(res.new_type)));
	children.push_back(make_uniq<ConstantExpression>(std::move(res.mapping)));
	children.push_back(make_uniq<ConstantExpression>(Value()));

	auto function = make_uniq<FunctionExpression>("remap_struct", std::move(children));

	ChangeColumnTypeInfo change_column_type(info.GetAlterEntryData(), info.column_path[0], std::move(res.new_type),
	                                        std::move(function));
	return ChangeColumnType(context, change_column_type);
}

DroppedFieldMapping RenameFieldFromStruct(const LogicalType &type, const vector<string> &column_path,
                                          const string &new_name, idx_t depth) {
	if (!type.IsNested()) {
		throw CatalogException("Cannot rename field from column \"%s\" - not a nested type", column_path[0]);
	}
	auto &rename_entry = column_path[depth];
	bool last_entry = depth + 1 == column_path.size();
	bool found = false;
	DroppedFieldMapping result;
	child_list_t<Value> child_mapping;
	child_list_t<LogicalType> new_type_children;
	auto child_types = GetChildList(type);
	for (auto &entry : child_types) {
		auto field_name = entry.first;
		Value mapping_value;
		LogicalType type_value;
		if ((type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(rename_entry, "element")) ||
		    StringUtil::CIEquals(field_name, rename_entry)) {
			// this is the entry we are renaming
			found = true;
			if (last_entry) {
				if (type.id() != LogicalTypeId::STRUCT) {
					throw CatalogException(
					    "Cannot rename field '%s' from column '%s' - can only rename fields inside a struct",
					    column_path.back(), column_path.front());
				}
				// we are renaming this entry
				for (auto &sub_entry : child_types) {
					if (StringUtil::CIEquals(new_name, sub_entry.first)) {
						throw CatalogException(
						    "Cannot rename field %s from column %s to %s - a field with this name already exists",
						    column_path.back(), column_path.front(), new_name);
					}
				}
				field_name = new_name;
				mapping_value = ConstructMapping(entry.first, entry.second);
				type_value = entry.second;
			} else {
				// we are renaming a field in this entry - recurse
				auto child_result = RenameFieldFromStruct(entry.second, column_path, new_name, depth + 1);
				if (child_result.error.HasError()) {
					// bubble up error
					return child_result;
				}
				mapping_value = std::move(child_result.mapping);
				type_value = std::move(child_result.new_type);
			}
		} else {
			// we are not adjusting this entry - copy the type and create a straightforward mapping
			mapping_value = ConstructMapping(entry.first, entry.second);
			type_value = entry.second;
		}
		if (entry.second.IsNested()) {
			child_list_t<Value> child_values;
			child_values.emplace_back(string(), Value(entry.first));
			child_values.emplace_back(string(), std::move(mapping_value));
			mapping_value = Value::STRUCT(std::move(child_values));
		}
		child_mapping.emplace_back(field_name, std::move(mapping_value));
		new_type_children.emplace_back(field_name, type_value);
	}
	if (!found) {
		result.error = ErrorData(CatalogException("Cannot rename field \"%s\" - it does not exist", rename_entry));
	} else {
		result.mapping = Value::STRUCT(std::move(child_mapping));
		result.new_type = ConstructNewType(type, std::move(new_type_children));
	}
	return result;
}

unique_ptr<CatalogEntry> DuckTableEntry::RenameField(ClientContext &context, RenameFieldInfo &info) {
	if (!ColumnExists(info.column_path[0])) {
		throw CatalogException("Cannot rename field from column \"%s\" - it does not exist", info.column_path[0]);
	}

	// follow the path
	auto &col = GetColumn(info.column_path[0]);
	auto res = RenameFieldFromStruct(col.Type(), info.column_path, info.new_name, 1);
	if (res.error.HasError()) {
		res.error.Throw();
	}

	// construct the struct remapping expression
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(info.column_path[0]));
	children.push_back(make_uniq<ConstantExpression>(Value(res.new_type)));
	children.push_back(make_uniq<ConstantExpression>(std::move(res.mapping)));
	children.push_back(make_uniq<ConstantExpression>(Value()));

	auto function = make_uniq<FunctionExpression>("remap_struct", std::move(children));
	ChangeColumnTypeInfo change_column_type(info.GetAlterEntryData(), info.column_path[0], std::move(res.new_type),
	                                        std::move(function));
	return ChangeColumnType(context, change_column_type);
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
	auto physical_columns = columns.LogicalToPhysical(LogicalIndex(not_null_idx));
	auto bound_constraint = make_uniq<BoundNotNullConstraint>(physical_columns);
	auto new_storage = make_shared_ptr<DataTable>(context, *storage, *bound_constraint);
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

	// Bind the USING expression.
	vector<LogicalIndex> bound_columns;
	AlterBinder expr_binder(*binder, context, *this, bound_columns, info.target_type);
	auto expression = info.expression->Copy();
	auto bound_expression = expr_binder.Bind(expression);

	// Infer the target_type from the USING expression, if not set explicitly.
	if (info.target_type == LogicalType::UNKNOWN) {
		info.target_type = bound_expression->return_type;
	}

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

	for (idx_t constr_idx = 0; constr_idx < constraints.size(); constr_idx++) {
		auto constraint = constraints[constr_idx]->Copy();
		switch (constraint->type) {
		case ConstraintType::CHECK: {
			auto &bound_check = bound_constraints[constr_idx]->Cast<BoundCheckConstraint>();
			auto physical_index = columns.LogicalToPhysical(change_idx);
			if (bound_check.bound_columns.find(physical_index) != bound_check.bound_columns.end()) {
				throw BinderException("Cannot change the type of a column that has a CHECK constraint specified");
			}
			break;
		}
		case ConstraintType::NOT_NULL:
			break;
		case ConstraintType::UNIQUE: {
			auto &bound_unique = bound_constraints[constr_idx]->Cast<BoundUniqueConstraint>();
			auto physical_index = columns.LogicalToPhysical(change_idx);
			if (bound_unique.key_set.find(physical_index) != bound_unique.key_set.end()) {
				throw BinderException(
				    "Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = bound_constraints[constr_idx]->Cast<BoundForeignKeyConstraint>();
			auto key_set = bfk.pk_key_set;
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				key_set = bfk.fk_key_set;
			} else if (bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				key_set.insert(bfk.info.fk_keys.begin(), bfk.info.fk_keys.end());
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

	auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);

	vector<StorageIndex> storage_oids;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		storage_oids.emplace_back(columns.LogicalToPhysical(bound_columns[i]).index);
	}
	if (storage_oids.empty()) {
		storage_oids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
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

unique_ptr<CatalogEntry> DuckTableEntry::AddForeignKeyConstraint(AlterForeignKeyInfo &info) {
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
	bound_create_info = Binder::BindCreateTableCheckpoint(std::move(create_info), schema);
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

void DuckTableEntry::Rollback(CatalogEntry &prev_entry) {
	if (prev_entry.type != CatalogType::TABLE_ENTRY) {
		return;
	}

	// Rolls back any physical index creation.
	// FIXME: Currently only works for PKs.
	// FIXME: Should be changed to work for any index-based constraint.

	auto &table = Cast<DuckTableEntry>();
	auto &prev_table = prev_entry.Cast<DuckTableEntry>();
	auto &prev_info = prev_table.GetStorage().GetDataTableInfo();
	auto &prev_indexes = prev_info->GetIndexes();

	// Find all index-based constraints that exist in rollback_table, but not in table.
	// Then, remove them.

	unordered_set<string> names;
	for (const auto &constraint : prev_table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		const auto &unique = constraint->Cast<UniqueConstraint>();
		if (unique.is_primary_key) {
			auto index_name = unique.GetName(prev_table.name);
			names.insert(index_name);
		}
	}

	for (const auto &constraint : GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		const auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		auto index_name = unique.GetName(table.name);
		if (names.find(index_name) == names.end()) {
			prev_indexes.RemoveIndex(index_name);
		}
	}
}

void DuckTableEntry::OnDrop() {
	storage->SetAsDropped();
}

unique_ptr<CatalogEntry> DuckTableEntry::AddConstraint(ClientContext &context, AddConstraintInfo &info) {
	auto create_info = make_uniq<CreateTableInfo>(schema, name);
	create_info->comment = comment;

	// Copy all columns and constraints to the modified table.
	create_info->columns = columns.Copy();
	for (const auto &constraint : constraints) {
		create_info->constraints.push_back(constraint->Copy());
	}

	if (info.constraint->type == ConstraintType::UNIQUE) {
		const auto &unique = info.constraint->Cast<UniqueConstraint>();
		const auto existing_pk = GetPrimaryKey();

		if (unique.is_primary_key && existing_pk) {
			auto existing_name = existing_pk->ToString();
			throw CatalogException("table \"%s\" can have only one primary key: %s", name, existing_name);
		}
		create_info->constraints.push_back(info.constraint->Copy());

	} else {
		throw InternalException("unsupported constraint type in ALTER TABLE statement");
	}

	// We create a physical table with a new constraint and a new unique index.
	const auto binder = Binder::CreateBinder(context);
	const auto bound_constraint = binder->BindConstraint(*info.constraint, create_info->table, create_info->columns);
	const auto bound_create_info = binder->BindCreateTableInfo(std::move(create_info), schema);

	auto new_storage = make_shared_ptr<DataTable>(context, *storage, *bound_constraint);
	auto new_entry = make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, new_storage);
	return std::move(new_entry);
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
	auto bound_create_info = binder->BindCreateTableCheckpoint(std::move(create_info), schema);
	return make_uniq<DuckTableEntry>(catalog, schema, *bound_create_info, storage);
}

void DuckTableEntry::SetAsRoot() {
	storage->SetAsMainTable();
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

	auto logical_column_index = LogicalIndex(removed_index.GetIndex());
	auto column_index = columns.LogicalToPhysical(logical_column_index).index;
	storage->CommitDropColumn(column_index);
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

vector<ColumnSegmentInfo> DuckTableEntry::GetColumnSegmentInfo(const QueryContext &context) {
	return storage->GetColumnSegmentInfo(context);
}

TableStorageInfo DuckTableEntry::GetStorageInfo(ClientContext &context) {
	return storage->GetStorageInfo();
}

} // namespace duckdb
