#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	// now visit the children
	assert(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	assert(left && right);

	if (op.conditions.size() == 0) {
		// no conditions: insert a cross product
		return make_unique<PhysicalCrossProduct>(op, move(left), move(right));
	}

	bool has_equality = false;
	bool has_inequality = false;
	bool has_null_equal_conditions = false;
	for (auto &cond : op.conditions) {
		if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
			has_equality = true;
		}
		if (cond.comparison == ExpressionType::COMPARE_NOTEQUAL) {
			has_inequality = true;
		}
		if (cond.null_values_are_equal) {
			has_null_equal_conditions = true;
			assert(cond.comparison == ExpressionType::COMPARE_EQUAL);
		}
	}
	unique_ptr<PhysicalOperator> plan;
	if (has_equality) {
		// equality join: use hash join
		plan = make_unique<PhysicalHashJoin>(context, op, move(left), move(right), move(op.conditions), op.join_type,
		                                     op.left_projection_map, op.right_projection_map);
	} else {
		assert(!has_null_equal_conditions); // don't support this for anything but hash joins for now
		if (op.conditions.size() == 1 && (op.join_type == JoinType::MARK || op.join_type == JoinType::INNER) &&
		    !has_inequality) {
			// range join: use piecewise merge join
			plan =
			    make_unique<PhysicalPiecewiseMergeJoin>(op, move(left), move(right), move(op.conditions), op.join_type);
		} else {
			// inequality join: use nested loop
			plan = make_unique<PhysicalNestedLoopJoin>(op, move(left), move(right), move(op.conditions), op.join_type);
		}
	}
	return plan;
}
