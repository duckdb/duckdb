#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/common/index_map.hpp"

#include <algorithm>

namespace duckdb {

static void CreateColumnDependencyManager(BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;
	for (auto &col : base.columns.Logical()) {
		if (!col.Generated()) {
			continue;
		}
		info.column_dependency_manager.AddGeneratedColumn(col, base.columns);
	}
}

static void BindCheckConstraint(Binder &binder, BoundCreateTableInfo &info, const unique_ptr<Constraint> &cond) {
	auto &base = (CreateTableInfo &)*info.base;

	auto bound_constraint = make_unique<BoundCheckConstraint>();
	// check constraint: bind the expression
	CheckBinder check_binder(binder, binder.context, base.table, base.columns, bound_constraint->bound_columns);
	auto &check = (CheckConstraint &)*cond;
	// create a copy of the unbound expression because the binding destroys the constraint
	auto unbound_expression = check.expression->Copy();
	// now bind the constraint and create a new BoundCheckConstraint
	bound_constraint->expression = check_binder.Bind(check.expression);
	info.bound_constraints.push_back(move(bound_constraint));
	// move the unbound constraint back into the original check expression
	check.expression = move(unbound_expression);
}

static void BindConstraints(Binder &binder, BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;

	bool has_primary_key = false;
	logical_index_set_t not_null_columns;
	vector<LogicalIndex> primary_keys;
	for (idx_t i = 0; i < base.constraints.size(); i++) {
		auto &cond = base.constraints[i];
		switch (cond->type) {
		case ConstraintType::CHECK: {
			BindCheckConstraint(binder, info, cond);
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &not_null = (NotNullConstraint &)*cond;
			auto &col = base.columns.GetColumn(LogicalIndex(not_null.index));
			info.bound_constraints.push_back(make_unique<BoundNotNullConstraint>(PhysicalIndex(col.StorageOid())));
			not_null_columns.insert(not_null.index);
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = (UniqueConstraint &)*cond;
			// have to resolve columns of the unique constraint
			vector<LogicalIndex> keys;
			logical_index_set_t key_set;
			if (unique.index.index != DConstants::INVALID_INDEX) {
				D_ASSERT(unique.index.index < base.columns.LogicalColumnCount());
				// unique constraint is given by single index
				unique.columns.push_back(base.columns.GetColumn(unique.index).Name());
				keys.push_back(unique.index);
				key_set.insert(unique.index);
			} else {
				// unique constraint is given by list of names
				// have to resolve names
				D_ASSERT(!unique.columns.empty());
				for (auto &keyname : unique.columns) {
					if (!base.columns.ColumnExists(keyname)) {
						throw ParserException("column \"%s\" named in key does not exist", keyname);
					}
					auto &column = base.columns.GetColumn(keyname);
					auto column_index = column.Logical();
					if (key_set.find(column_index) != key_set.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname);
					}
					keys.push_back(column_index);
					key_set.insert(column_index);
				}
			}

			if (unique.is_primary_key) {
				// we can only have one primary key per table
				if (has_primary_key) {
					throw ParserException("table \"%s\" has more than one primary key", base.table);
				}
				has_primary_key = true;
				primary_keys = keys;
			}
			info.bound_constraints.push_back(
			    make_unique<BoundUniqueConstraint>(move(keys), move(key_set), unique.is_primary_key));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &fk = (ForeignKeyConstraint &)*cond;
			D_ASSERT((fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE);
			unordered_set<idx_t> fk_key_set, pk_key_set;
			for (idx_t i = 0; i < fk.info.pk_keys.size(); i++) {
				pk_key_set.insert(fk.info.pk_keys[i]);
			}
			for (idx_t i = 0; i < fk.info.fk_keys.size(); i++) {
				fk_key_set.insert(fk.info.fk_keys[i]);
			}
			info.bound_constraints.push_back(make_unique<BoundForeignKeyConstraint>(fk.info, pk_key_set, fk_key_set));
			break;
		}
		default:
			throw NotImplementedException("unrecognized constraint type in bind");
		}
	}
	if (has_primary_key) {
		// if there is a primary key index, also create a NOT NULL constraint for each of the columns
		for (auto &column_index : primary_keys) {
			if (not_null_columns.count(column_index)) {
				//! No need to create a NotNullConstraint, it's already present
				continue;
			}
			auto physical_index = base.columns.LogicalToPhysical(column_index);
			base.constraints.push_back(make_unique<NotNullConstraint>(column_index));
			info.bound_constraints.push_back(make_unique<BoundNotNullConstraint>(physical_index));
		}
	}
}

void Binder::BindGeneratedColumns(BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;

	vector<string> names;
	vector<LogicalType> types;

	D_ASSERT(base.type == CatalogType::TABLE_ENTRY);
	for (auto &col : base.columns.Logical()) {
		names.push_back(col.Name());
		types.push_back(col.Type());
	}
	auto table_index = GenerateTableIndex();

	// Create a new binder because we dont need (or want) these bindings in this scope
	auto binder = Binder::CreateBinder(context);
	binder->bind_context.AddGenericBinding(table_index, base.table, names, types);
	auto expr_binder = ExpressionBinder(*binder, context);
	string ignore;
	auto table_binding = binder->bind_context.GetBinding(base.table, ignore);
	D_ASSERT(table_binding && ignore.empty());

	auto bind_order = info.column_dependency_manager.GetBindOrder(base.columns);
	unordered_set<column_t> bound_indices;

	while (!bind_order.empty()) {
		auto i = bind_order.top();
		bind_order.pop();
		auto &col = base.columns.GetColumnMutable(LogicalIndex(i));

		//! Already bound this previously
		//! This can not be optimized out of the GetBindOrder function
		//! These occurrences happen because we need to make sure that ALL dependencies of a column are resolved before
		//! it gets resolved
		if (bound_indices.count(i)) {
			continue;
		}
		D_ASSERT(col.Generated());
		auto expression = col.GeneratedExpression().Copy();

		auto bound_expression = expr_binder.Bind(expression);
		D_ASSERT(bound_expression);
		D_ASSERT(!bound_expression->HasSubquery());
		if (col.Type().id() == LogicalTypeId::ANY) {
			// Do this before changing the type, so we know it's the first time the type is set
			col.ChangeGeneratedExpressionType(bound_expression->return_type);
			col.SetType(bound_expression->return_type);

			// Update the type in the binding, for future expansions
			string ignore;
			table_binding->types[i] = col.Type();
		}
		bound_indices.insert(i);
	}
}

void Binder::BindDefaultValues(ColumnList &columns, vector<unique_ptr<Expression>> &bound_defaults) {
	for (auto &column : columns.Logical()) {
		unique_ptr<Expression> bound_default;
		if (column.DefaultValue()) {
			// we bind a copy of the DEFAULT value because binding is destructive
			// and we want to keep the original expression around for serialization
			auto default_copy = column.DefaultValue()->Copy();
			ConstantBinder default_binder(*this, context, "DEFAULT value");
			default_binder.target_type = column.Type();
			bound_default = default_binder.Bind(default_copy);
		} else {
			// no default value specified: push a default value of constant null
			bound_default = make_unique<BoundConstantExpression>(Value(column.Type()));
		}
		bound_defaults.push_back(move(bound_default));
	}
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateTableInfo &)*info;

	auto result = make_unique<BoundCreateTableInfo>(move(info));
	result->schema = BindSchema(*result->base);
	if (base.query) {
		// construct the result object
		auto query_obj = Bind(*base.query);
		result->query = move(query_obj.plan);

		// construct the set of columns based on the names and types of the query
		auto &names = query_obj.names;
		auto &sql_types = query_obj.types;
		D_ASSERT(names.size() == sql_types.size());
		base.columns.SetAllowDuplicates(true);
		for (idx_t i = 0; i < names.size(); i++) {
			base.columns.AddColumn(ColumnDefinition(names[i], sql_types[i]));
		}
		CreateColumnDependencyManager(*result);
		// bind the generated column expressions
		BindGeneratedColumns(*result);
	} else {
		CreateColumnDependencyManager(*result);
		// bind the generated column expressions
		BindGeneratedColumns(*result);
		// bind any constraints
		BindConstraints(*this, *result);
		// bind the default values
		BindDefaultValues(base.columns, result->bound_defaults);
	}

	if (base.columns.PhysicalColumnCount() == 0) {
		throw BinderException("Creating a table without physical (non-generated) columns is not supported");
	}
	// bind collations to detect any unsupported collation errors
	for (idx_t i = 0; i < base.columns.PhysicalColumnCount(); i++) {
		auto &column = base.columns.GetColumnMutable(PhysicalIndex(i));
		if (column.Type().id() == LogicalTypeId::VARCHAR) {
			ExpressionBinder::TestCollation(context, StringType::GetCollation(column.Type()));
		}
		BindLogicalType(context, column.TypeMutable());
		// We add a catalog dependency
		auto type_dependency = LogicalType::GetCatalog(column.Type());
		if (type_dependency) {
			// Only if the USER comes from a create type
			result->dependencies.insert(type_dependency);
		}
	}
	properties.allow_stream_result = false;
	return result;
}

} // namespace duckdb
