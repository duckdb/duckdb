#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

static JoinCondition CreateNotDistinctComparison(const LogicalType &type, idx_t i) {
	JoinCondition cond;
	cond.left = make_uniq<BoundReferenceExpression>(type, i);
	cond.right = make_uniq<BoundReferenceExpression>(type, i);
	cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	return cond;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation &op) {
	D_ASSERT(op.children.size() == 2);

	unique_ptr<PhysicalOperator> result;

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	if (left->GetTypes() != right->GetTypes()) {
		throw InvalidInputException("Type mismatch for SET OPERATION");
	}

	// can't swich logical unions to semi/anti join
	// also if the operation is a INTERSECT ALL or EXCEPT ALL
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
		// UNION
		result = make_uniq<PhysicalUnion>(op.types, std::move(left), std::move(right), op.estimated_cardinality,
		                                  op.allow_out_of_order);
		break;
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		throw InternalException("Logical Except/Intersect should have been transformed to semi anti before the physical planning phase");
	}
	default:
		throw InternalException("Unexpected operator type for set operation");
	}

	// if the ALL specifier is not given, we have to ensure distinct results. Hence, push a GROUP BY ALL
	if (!op.setop_all) { // no ALL, use distinct semantics
		auto &types = result->GetTypes();
		vector<unique_ptr<Expression>> groups, aggregates /* left empty */;
		for (idx_t i = 0; i < types.size(); i++) {
			groups.push_back(make_uniq<BoundReferenceExpression>(types[i], i));
		}
		auto groupby = make_uniq<PhysicalHashAggregate>(context, op.types, std::move(aggregates), std::move(groups),
		                                                result->estimated_cardinality);
		groupby->children.push_back(std::move(result));
		result = std::move(groupby);
	}

	D_ASSERT(result);
	return (result);
}

} // namespace duckdb
