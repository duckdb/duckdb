#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/execution/operator/schema/physical_create_table_as.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

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
	auto existing_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info.schema, create_info.table, true);
	bool replace = op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT;
	if ((!existing_entry || replace) && !op.children.empty()) {
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
