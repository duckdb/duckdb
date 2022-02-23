#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/execution/operator/schema/physical_create_table_as.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

namespace duckdb {

static void ExtractDependencies(Expression &expr, unordered_set<CatalogEntry *> &dependencies) {
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &function = (BoundFunctionExpression &)expr;
		if (function.function.dependency) {
			function.function.dependency(function, dependencies);
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractDependencies(child, dependencies); });
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateTable &op) {
	// extract dependencies from any default values
	for (auto &default_value : op.info->bound_defaults) {
		if (default_value) {
			ExtractDependencies(*default_value, op.info->dependencies);
		}
	}
	auto &create_info = (CreateTableInfo &)*op.info->base;
	auto &catalog = Catalog::GetCatalog(context);
	for (idx_t i = 0; i < create_info.constraints.size(); i++) {
		auto &cond = create_info.constraints[i];
		if (cond->type == ConstraintType::FOREIGN_KEY) {
			auto &foreign_key = (ForeignKeyConstraint &)*cond;
			if (foreign_key.is_fk_table) {
				// have to resolve columns of the foreign key table and referenced table
				D_ASSERT(!foreign_key.pk_keys.empty() && !foreign_key.fk_columns.empty());
				vector<idx_t> fk_keys;
				// foreign key is given by list of names
				// have to resolve names
				for (auto &keyname : foreign_key.fk_columns) {
					auto entry = op.info->name_map.find(keyname);
					if (entry == op.info->name_map.end()) {
						throw ParserException("column \"%s\" named in key does not exist", keyname);
					}
					fk_keys.push_back(entry->second);
				}

				// alter primary key table
				unique_ptr<ForeignKeyConstraintInfo> info = make_unique<ForeignKeyConstraintInfo>(
				    DEFAULT_SCHEMA, foreign_key.pk_table, create_info.table, foreign_key.pk_columns,
				    foreign_key.fk_columns, foreign_key.pk_keys, fk_keys);
				catalog.Alter(context, info.get());

				// make a dependency between this table and referenced table
				auto pk_table_entry_ptr =
				    catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, foreign_key.pk_table);
				if (!pk_table_entry_ptr) {
					throw ParserException("Can't find table \"%s\" in foreign key constraint", foreign_key.pk_table);
				}
				op.info->dependencies.insert(pk_table_entry_ptr);
			}
		}
	}
	auto existing_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info.schema, create_info.table, true);
	if (!existing_entry && !op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		auto create = make_unique<PhysicalCreateTableAs>(op, op.schema, move(op.info), op.estimated_cardinality);
		auto plan = CreatePlan(*op.children[0]);
		create->children.push_back(move(plan));
		return move(create);
	} else {
		return make_unique<PhysicalCreateTable>(op, op.schema, move(op.info), op.estimated_cardinality);
	}
}

} // namespace duckdb
