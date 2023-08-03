#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb
{
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation &op)
{
	D_ASSERT(op.children.size() == 2);
	LogicalOperator* left_pop = ((LogicalOperator*)op.children[0].get());
	auto left = CreatePlan(*left_pop);
	LogicalOperator* right_pop = ((LogicalOperator*)op.children[1].get());
	auto right = CreatePlan(*right_pop);
	if (left->GetTypes() != right->GetTypes())
	{
		throw InvalidInputException("Type mismatch for SET OPERATION");
	}
	switch (op.logical_type)
	{
	case LogicalOperatorType::LOGICAL_UNION:
		// UNION
		return make_uniq<PhysicalUnion>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
	default:
	{
		// EXCEPT/INTERSECT
		D_ASSERT(op.logical_type == LogicalOperatorType::LOGICAL_EXCEPT || op.logical_type == LogicalOperatorType::LOGICAL_INTERSECT);
		auto &types = left->GetTypes();
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < types.size(); i++)
		{
			JoinCondition cond;
			cond.left = make_uniq<BoundReferenceExpression>(types[i], i);
			cond.right = make_uniq<BoundReferenceExpression>(types[i], i);
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(std::move(cond));
		}
		// EXCEPT is ANTI join
		// INTERSECT is SEMI join
		PerfectHashJoinStats join_stats; // used in inner joins only
		JoinType join_type = op.logical_type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;
		return make_uniq<PhysicalHashJoin>(op, std::move(left), std::move(right), std::move(conditions), join_type, op.estimated_cardinality, join_stats);
	}
	}
}

} // namespace duckdb
