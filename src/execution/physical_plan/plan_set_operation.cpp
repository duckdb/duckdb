#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation &op) {
	assert(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	if (left->GetTypes() != right->GetTypes()) {
		throw Exception("Type mismatch for SET OPERATION");
	}

	switch (op.type) {
	case LogicalOperatorType::UNION:
		// UNION
		return make_unique<PhysicalUnion>(op, move(left), move(right));
	default: {
		// EXCEPT/INTERSECT
		assert(op.type == LogicalOperatorType::EXCEPT || op.type == LogicalOperatorType::INTERSECT);
		auto &types = left->GetTypes();
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < types.size(); i++) {
			JoinCondition cond;
			cond.comparison = ExpressionType::COMPARE_EQUAL;
			cond.left = make_unique<BoundReferenceExpression>(types[i], i);
			cond.right = make_unique<BoundReferenceExpression>(types[i], i);
			cond.null_values_are_equal = true;
			conditions.push_back(move(cond));
		}
		// EXCEPT is ANTI join
		// INTERSECT is SEMI join
		JoinType join_type = op.type == LogicalOperatorType::EXCEPT ? JoinType::ANTI : JoinType::SEMI;
		return make_unique<PhysicalHashJoin>(context, op, move(left), move(right), move(conditions), join_type);
	}
	}
}
