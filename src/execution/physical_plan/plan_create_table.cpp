#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "execution/operator/schema/physical_create_table.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression_iterator.hpp"
#include "planner/operator/logical_create_table.hpp"

using namespace duckdb;
using namespace std;

static void ExtractDependencies(Expression &expr, unordered_set<CatalogEntry *> &dependencies) {
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &function = (BoundFunctionExpression &)expr;
		if (function.bound_function->dependency) {
			function.bound_function->dependency(function, dependencies);
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
	auto create = make_unique<PhysicalCreateTable>(op, op.schema, move(op.info));
	if (op.children.size() > 0) {
		assert(op.children.size() == 1);
		auto plan = CreatePlan(*op.children[0]);
		create->children.push_back(move(plan));
	}
	return move(create);
}
