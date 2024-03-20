#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_left_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_right_delim_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static void GatherDelimScans(const PhysicalOperator &op, vector<const_reference<PhysicalOperator>> &delim_scans) {
	if (op.type == PhysicalOperatorType::DELIM_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto &child : op.children) {
		GatherDelimScans(*child, delim_scans);
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::PlanDelimJoin(LogicalComparisonJoin &op) {
	// first create the underlying join
	auto plan = PlanComparisonJoin(op);
	// this should create a join, not a cross product
	D_ASSERT(plan && plan->type != PhysicalOperatorType::CROSS_PRODUCT);
	// duplicate eliminated join
	// first gather the scans on the duplicate eliminated data set from the delim side
	const idx_t delim_idx = op.delim_flipped ? 0 : 1;
	vector<const_reference<PhysicalOperator>> delim_scans;
	GatherDelimScans(*plan->children[delim_idx], delim_scans);
	if (delim_scans.empty()) {
		// no duplicate eliminated scans in the delim side!
		// in this case we don't need to create a delim join
		// just push the normal join
		return plan;
	}
	vector<LogicalType> delim_types;
	vector<unique_ptr<Expression>> distinct_groups, distinct_expressions;
	for (auto &delim_expr : op.duplicate_eliminated_columns) {
		D_ASSERT(delim_expr->type == ExpressionType::BOUND_REF);
		auto &bound_ref = delim_expr->Cast<BoundReferenceExpression>();
		delim_types.push_back(bound_ref.return_type);
		distinct_groups.push_back(make_uniq<BoundReferenceExpression>(bound_ref.return_type, bound_ref.index));
	}
	// now create the duplicate eliminated join
	unique_ptr<PhysicalDelimJoin> delim_join;
	if (op.delim_flipped) {
		delim_join =
		    make_uniq<PhysicalRightDelimJoin>(op.types, std::move(plan), delim_scans, op.estimated_cardinality);
	} else {
		delim_join = make_uniq<PhysicalLeftDelimJoin>(op.types, std::move(plan), delim_scans, op.estimated_cardinality);
	}
	// we still have to create the DISTINCT clause that is used to generate the duplicate eliminated chunk
	delim_join->distinct = make_uniq<PhysicalHashAggregate>(context, delim_types, std::move(distinct_expressions),
	                                                        std::move(distinct_groups), op.estimated_cardinality);
	return std::move(delim_join);
}

} // namespace duckdb
