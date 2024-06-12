#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
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
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include <algorithm>

namespace duckdb {

static void CreateColumnDependencyManager(BoundCreateTableInfo &info) {
	auto &base = info.base->Cast<CreateTableInfo>();
	for (auto &col : base.columns.Logical()) {
		if (!col.Generated()) {
			continue;
		}
		info.column_dependency_manager.AddGeneratedColumn(col, base.columns);
	}
}

static unique_ptr<BoundConstraint> BindCheckConstraint(Binder &binder, const string &table_name,
                                                       const ColumnList &columns, const unique_ptr<Constraint> &cond) {
	auto bound_constraint = make_uniq<BoundCheckConstraint>();
	// check constraint: bind the expression
	CheckBinder check_binder(binder, binder.context, table_name, columns, bound_constraint->bound_columns);
	auto &check = cond->Cast<CheckConstraint>();
	// create a copy of the unbound expression because the binding destroys the constraint
	auto unbound_expression = check.expression->Copy();
	// now bind the constraint and create a new BoundCheckConstraint
	bound_constraint->expression = check_binder.Bind(unbound_expression);
	return std::move(bound_constraint);
}

vector<unique_ptr<BoundConstraint>> Binder::BindConstraints(ClientContext &context,
                                                            const vector<unique_ptr<Constraint>> &constraints,
                                                            const string &table_name, const ColumnList &columns) {
	auto binder = Binder::CreateBinder(context);
	return binder->BindConstraints(constraints, table_name, columns);
}

vector<unique_ptr<BoundConstraint>> Binder::BindConstraints(const TableCatalogEntry &table) {
	return BindConstraints(table.GetConstraints(), table.name, table.GetColumns());
}

vector<unique_ptr<BoundConstraint>> Binder::BindConstraints(const vector<unique_ptr<Constraint>> &constraints,
                                                            const string &table_name, const ColumnList &columns) {
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constr : constraints) {
		switch (constr->type) {
		case ConstraintType::CHECK: {
			bound_constraints.push_back(BindCheckConstraint(*this, table_name, columns, constr));
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &not_null = constr->Cast<NotNullConstraint>();
			auto &col = columns.GetColumn(LogicalIndex(not_null.index));
			bound_constraints.push_back(make_uniq<BoundNotNullConstraint>(PhysicalIndex(col.StorageOid())));
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = constr->Cast<UniqueConstraint>();
			// have to resolve columns of the unique constraint
			vector<LogicalIndex> keys;
			logical_index_set_t key_set;
			if (unique.HasIndex()) {
				D_ASSERT(unique.GetIndex().index < columns.LogicalColumnCount());
				// unique constraint is given by single index
				unique.SetColumnName(columns.GetColumn(unique.GetIndex()).Name());
				keys.push_back(unique.GetIndex());
				key_set.insert(unique.GetIndex());
			} else {
				// unique constraint is given by list of names
				// have to resolve names
				for (auto &keyname : unique.GetColumnNames()) {
					if (!columns.ColumnExists(keyname)) {
						throw ParserException("column \"%s\" named in key does not exist", keyname);
					}
					auto &column = columns.GetColumn(keyname);
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
			bound_constraints.push_back(
			    make_uniq<BoundUniqueConstraint>(std::move(keys), std::move(key_set), unique.IsPrimaryKey()));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &fk = constr->Cast<ForeignKeyConstraint>();
			D_ASSERT((fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE);
			physical_index_set_t fk_key_set, pk_key_set;
			for (auto &pk_key : fk.info.pk_keys) {
				if (pk_key_set.find(pk_key) != pk_key_set.end()) {
					throw BinderException("Duplicate primary key referenced in FOREIGN KEY constraint");
				}
				pk_key_set.insert(pk_key);
			}
			for (auto &fk_key : fk.info.fk_keys) {
				if (fk_key_set.find(fk_key) != fk_key_set.end()) {
					throw BinderException("Duplicate key specified in FOREIGN KEY constraint");
				}
				fk_key_set.insert(fk_key);
			}
			bound_constraints.push_back(
			    make_uniq<BoundForeignKeyConstraint>(fk.info, std::move(pk_key_set), std::move(fk_key_set)));
			break;
		}
		default:
			throw NotImplementedException("unrecognized constraint type in bind");
		}
	}
	return bound_constraints;
}

vector<unique_ptr<BoundConstraint>> Binder::BindNewConstraints(vector<unique_ptr<Constraint>> &constraints,
                                                               const string &table_name, const ColumnList &columns) {
	auto bound_constraints = BindConstraints(constraints, table_name, columns);

	// handle primary keys/not null constraints
	bool has_primary_key = false;
	logical_index_set_t not_null_columns;
	vector<LogicalIndex> primary_keys;
	for (idx_t c = 0; c < constraints.size(); c++) {
		auto &constr = constraints[c];
		switch (constr->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = constr->Cast<NotNullConstraint>();
			auto &col = columns.GetColumn(LogicalIndex(not_null.index));
			bound_constraints.push_back(make_uniq<BoundNotNullConstraint>(PhysicalIndex(col.StorageOid())));
			not_null_columns.insert(not_null.index);
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = constr->Cast<UniqueConstraint>();
			auto &bound_unique = bound_constraints[c]->Cast<BoundUniqueConstraint>();
			if (unique.IsPrimaryKey()) {
				// we can only have one primary key per table
				if (has_primary_key) {
					throw ParserException("table \"%s\" has more than one primary key", table_name);
				}
				has_primary_key = true;
				primary_keys = bound_unique.keys;
			}
			break;
		}
		default:
			break;
		}
	}
	if (has_primary_key) {
		// if there is a primary key index, also create a NOT NULL constraint for each of the columns
		for (auto &column_index : primary_keys) {
			if (not_null_columns.count(column_index)) {
				//! No need to create a NotNullConstraint, it's already present
				continue;
			}
			auto physical_index = columns.LogicalToPhysical(column_index);
			constraints.push_back(make_uniq<NotNullConstraint>(column_index));
			bound_constraints.push_back(make_uniq<BoundNotNullConstraint>(physical_index));
		}
	}
	return bound_constraints;
}

void Binder::BindGeneratedColumns(BoundCreateTableInfo &info) {
	auto &base = info.base->Cast<CreateTableInfo>();

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
	binder->SetCatalogLookupCallback(entry_retriever.GetCallback());
	binder->bind_context.AddGenericBinding(table_index, base.table, names, types);
	auto expr_binder = ExpressionBinder(*binder, context);
	ErrorData ignore;
	auto table_binding = binder->bind_context.GetBinding(base.table, ignore);
	D_ASSERT(table_binding && !ignore.HasError());

	auto bind_order = info.column_dependency_manager.GetBindOrder(base.columns);
	logical_index_set_t bound_indices;

	while (!bind_order.empty()) {
		auto i = bind_order.top();
		bind_order.pop();
		auto &col = base.columns.GetColumnMutable(i);

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
			table_binding->types[i.index] = col.Type();
		}
		bound_indices.insert(i);
	}
}

void Binder::BindDefaultValues(const ColumnList &columns, vector<unique_ptr<Expression>> &bound_defaults) {
	for (auto &column : columns.Physical()) {
		unique_ptr<Expression> bound_default;
		if (column.HasDefaultValue()) {
			// we bind a copy of the DEFAULT value because binding is destructive
			// and we want to keep the original expression around for serialization
			auto default_copy = column.DefaultValue().Copy();
			if (default_copy->HasParameter()) {
				throw BinderException("DEFAULT values cannot contain parameters");
			}
			ConstantBinder default_binder(*this, context, "DEFAULT value");
			default_binder.target_type = column.Type();
			bound_default = default_binder.Bind(default_copy);
		} else {
			// no default value specified: push a default value of constant null
			bound_default = make_uniq<BoundConstantExpression>(Value(column.Type()));
		}
		bound_defaults.push_back(std::move(bound_default));
	}
}

static void ExtractExpressionDependencies(Expression &expr, LogicalDependencyList &dependencies) {
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.function.dependency) {
			function.function.dependency(function, dependencies);
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ExtractExpressionDependencies(child, dependencies); });
}

static void ExtractDependencies(BoundCreateTableInfo &info, vector<unique_ptr<Expression>> &defaults,
                                vector<unique_ptr<BoundConstraint>> &constraints) {
	for (auto &default_value : defaults) {
		if (default_value) {
			ExtractExpressionDependencies(*default_value, info.dependencies);
		}
	}
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();
			ExtractExpressionDependencies(*bound_check.expression, info.dependencies);
		}
	}
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema) {
	vector<unique_ptr<Expression>> bound_defaults;
	return BindCreateTableInfo(std::move(info), schema, bound_defaults);
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableCheckpoint(unique_ptr<CreateInfo> info,
                                                                   SchemaCatalogEntry &schema) {
	auto result = make_uniq<BoundCreateTableInfo>(schema, std::move(info));
	CreateColumnDependencyManager(*result);
	return result;
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema,
                                                             vector<unique_ptr<Expression>> &bound_defaults) {
	auto &base = info->Cast<CreateTableInfo>();
	auto result = make_uniq<BoundCreateTableInfo>(schema, std::move(info));
	auto &dependencies = result->dependencies;

	vector<unique_ptr<BoundConstraint>> bound_constraints;
	if (base.query) {
		// construct the result object
		auto query_obj = Bind(*base.query);
		base.query.reset();
		result->query = std::move(query_obj.plan);

		// construct the set of columns based on the names and types of the query
		auto &names = query_obj.names;
		auto &sql_types = query_obj.types;
		D_ASSERT(names.size() == sql_types.size());
		base.columns.SetAllowDuplicates(true);
		for (idx_t i = 0; i < names.size(); i++) {
			base.columns.AddColumn(ColumnDefinition(names[i], sql_types[i]));
		}
	} else {
		SetCatalogLookupCallback([&dependencies, &schema](CatalogEntry &entry) {
			if (&schema.ParentCatalog() != &entry.ParentCatalog()) {
				// Don't register dependencies between catalogs
				return;
			}
			dependencies.AddDependency(entry);
		});
		CreateColumnDependencyManager(*result);
		// bind the generated column expressions
		BindGeneratedColumns(*result);
		// bind any constraints
		bound_constraints = BindNewConstraints(base.constraints, base.table, base.columns);
		// bind the default values
		BindDefaultValues(base.columns, bound_defaults);
	}
	// extract dependencies from any default values or CHECK constraints
	ExtractDependencies(*result, bound_defaults, bound_constraints);

	if (base.columns.PhysicalColumnCount() == 0) {
		throw BinderException("Creating a table without physical (non-generated) columns is not supported");
	}
	// bind collations to detect any unsupported collation errors
	for (idx_t i = 0; i < base.columns.PhysicalColumnCount(); i++) {
		auto &column = base.columns.GetColumnMutable(PhysicalIndex(i));
		if (column.Type().id() == LogicalTypeId::VARCHAR) {
			ExpressionBinder::TestCollation(context, StringType::GetCollation(column.Type()));
		}
		BindLogicalType(column.TypeMutable(), &result->schema.catalog);
	}
	result->dependencies.VerifyDependencies(schema.catalog, result->Base().table);

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	return result;
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info) {
	auto &base = info->Cast<CreateTableInfo>();
	auto &schema = BindCreateSchema(base);
	return BindCreateTableInfo(std::move(info), schema);
}

} // namespace duckdb
