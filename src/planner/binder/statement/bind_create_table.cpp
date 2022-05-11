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

#include <algorithm>

namespace duckdb {

static void CreateColumnMap(BoundCreateTableInfo &info, bool allow_duplicate_names) {
	auto &base = (CreateTableInfo &)*info.base;

	for (uint64_t oid = 0; oid < base.columns.size(); oid++) {
		auto &col = base.columns[oid];
		if (allow_duplicate_names) {
			idx_t index = 1;
			string base_name = col.name;
			while (info.name_map.find(col.name) != info.name_map.end()) {
				col.name = base_name + ":" + to_string(index++);
			}
		} else {
			if (info.name_map.find(col.name) != info.name_map.end()) {
				throw CatalogException("Column with name %s already exists!", col.name);
			}
		}

		info.name_map[col.name] = oid;
		col.oid = oid;
	}
}

static void BindConstraints(Binder &binder, BoundCreateTableInfo &info) {
	auto &base = (CreateTableInfo &)*info.base;

	bool has_primary_key = false;
	vector<idx_t> primary_keys;
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
			vector<idx_t> keys;
			unordered_set<idx_t> key_set;
			if (unique.index != DConstants::INVALID_INDEX) {
				D_ASSERT(unique.index < base.columns.size());
				// unique constraint is given by single index
				unique.columns.push_back(base.columns[unique.index].name);
				keys.push_back(unique.index);
				key_set.insert(unique.index);
			} else {
				// unique constraint is given by list of names
				// have to resolve names
				D_ASSERT(!unique.columns.empty());
				for (auto &keyname : unique.columns) {
					auto entry = info.name_map.find(keyname);
					if (entry == info.name_map.end()) {
						throw ParserException("column \"%s\" named in key does not exist", keyname);
					}
					if (key_set.find(entry->second) != key_set.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname);
					}
					keys.push_back(entry->second);
					key_set.insert(entry->second);
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
			if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE && fk.info.pk_keys.empty()) {
				for (auto &keyname : fk.pk_columns) {
					auto entry = info.name_map.find(keyname);
					if (entry == info.name_map.end()) {
						throw BinderException("column \"%s\" named in key does not exist", keyname);
					}
					fk.info.pk_keys.push_back(entry->second);
				}
			}
			if (fk.info.fk_keys.empty()) {
				for (auto &keyname : fk.fk_columns) {
					auto entry = info.name_map.find(keyname);
					if (entry == info.name_map.end()) {
						throw BinderException("column \"%s\" named in key does not exist", keyname);
					}
					fk.info.fk_keys.push_back(entry->second);
				}
			}
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
			bound_default = make_unique<BoundConstantExpression>(Value(columns[i].type));
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
		for (idx_t i = 0; i < names.size(); i++) {
			base.columns.emplace_back(names[i], sql_types[i]);
		}
		// create the name map for the statement
		CreateColumnMap(*result, true);
	} else {
		// create the name map for the statement
		CreateColumnMap(*result, false);
		// bind any constraints
		BindConstraints(*this, *result);
		// bind the default values
		BindDefaultValues(base.columns, result->bound_defaults);
	}
	// bind collations to detect any unsupported collation errors
	for (auto &column : base.columns) {
		ExpressionBinder::TestCollation(context, StringType::GetCollation(column.type));
		BindLogicalType(context, column.type);
		if (column.type.id() == LogicalTypeId::ENUM) {
			// We add a catalog dependency
			auto enum_dependency = EnumType::GetCatalog(column.type);
			if (enum_dependency) {
				// Only if the ENUM comes from a create type
				result->dependencies.insert(enum_dependency);
			}
		}
	}
	properties.allow_stream_result = false;
	return result;
}

} // namespace duckdb
