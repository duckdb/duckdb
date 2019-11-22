#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFilter &op) {
	assert(op.children.size() == 1);
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	if (op.expressions.size() > 0) {
		// create a filter if there is anything to filter
		auto filter = make_unique<PhysicalFilter>(op.types, move(op.expressions));
		filter->children.push_back(move(plan));
		plan = move(filter);
	}
	return plan;
}
