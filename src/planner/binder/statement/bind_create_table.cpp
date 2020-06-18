#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

using namespace duckdb;
using namespace std;

static void CreateColumnMap(BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;

	for (uint64_t oid = 0; oid < base.columns.size(); oid++) {
		auto &col = base.columns[oid];
		if (info.name_map.find(col.name) != info.name_map.end()) {
			throw CatalogException("Column with name %s already exists!", col.name.c_str());
		}

		info.name_map[col.name] = oid;
		col.oid = oid;
	}
}

static void BindConstraints(Binder &binder, BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;

	bool has_primary_key = false;
	unordered_set<idx_t> primary_keys;
	for (idx_t i = 0; i < base.constraints.size(); i++) {
		auto &cond = base.constraints[i];
		switch (cond->type) {
		case ConstraintType::CHECK: {
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
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &not_null = (NotNullConstraint &)*cond;
			info.bound_constraints.push_back(make_unique<BoundNotNullConstraint>(not_null.index));
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = (UniqueConstraint &)*cond;
			// have to resolve columns of the unique constraint
			unordered_set<idx_t> keys;
			if (unique.index != INVALID_INDEX) {
				assert(unique.index < base.columns.size());
				// unique constraint is given by single index
				keys.insert(unique.index);
			} else {
				// unique constraint is given by list of names
				// have to resolve names
				assert(unique.columns.size() > 0);
				for (auto &keyname : unique.columns) {
					auto entry = info.name_map.find(keyname);
					if (entry == info.name_map.end()) {
						throw ParserException("column \"%s\" named in key does not exist", keyname.c_str());
					}
					if (find(keys.begin(), keys.end(), entry->second) != keys.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname.c_str());
					}
					keys.insert(entry->second);
				}
			}

			if (unique.is_primary_key) {
				// we can only have one primary key per table
				if (has_primary_key) {
					throw ParserException("table \"%s\" has more than one primary key", base.table.c_str());
				}
				has_primary_key = true;
				primary_keys = keys;
			}
			info.bound_constraints.push_back(make_unique<BoundUniqueConstraint>(keys, unique.is_primary_key));
			break;
		}
		default:
			throw NotImplementedException("unrecognized constraint type in bind");
		}
	}
	if (has_primary_key) {
		// if there is a primary key index, also create a NOT NULL constraint for each of the columns
		for (auto &column_index : primary_keys) {
			base.constraints.push_back(make_unique<NotNullConstraint>(column_index));
			info.bound_constraints.push_back(make_unique<BoundNotNullConstraint>(column_index));
		}
	}
}

void Binder::BindDefaultValues(vector<ColumnDefinition> &columns, vector<unique_ptr<Expression>> &bound_defaults) {
	for (idx_t i = 0; i < columns.size(); i++) {
		unique_ptr<Expression> bound_default;
		if (columns[i].default_value) {
			// we bind a copy of the DEFAULT value because binding is destructive
			// and we want to keep the original expression around for serialization
			auto default_copy = columns[i].default_value->Copy();
			ConstantBinder default_binder(*this, context, "DEFAULT value");
			default_binder.target_type = columns[i].type;
			bound_default = default_binder.Bind(default_copy);
		} else {
			// no default value specified: push a default value of constant null
			bound_default = make_unique<BoundConstantExpression>(Value(GetInternalType(columns[i].type)));
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
		assert(names.size() == sql_types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			base.columns.push_back(ColumnDefinition(names[i], sql_types[i]));
		}
		// create the name map for the statement
		CreateColumnMap(*result);
	} else {
		// create the name map for the statement
		CreateColumnMap(*result);
		// bind any constraints
		BindConstraints(*this, *result);
		// bind the default values
		BindDefaultValues(base.columns, result->bound_defaults);
	}
	// bind collations to detect any unsupported collation errors
	for (auto &column : base.columns) {
		ExpressionBinder::PushCollation(context, nullptr, column.type.collation);
	}
	return result;
}
