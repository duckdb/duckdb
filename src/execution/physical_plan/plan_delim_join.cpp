#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_left_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_right_delim_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"

namespace duckdb {

static void GatherDelimScans(PhysicalOperator &op, vector<const_reference<PhysicalOperator>> &delim_scans,
                             idx_t delim_index) {
	if (op.type == PhysicalOperatorType::DELIM_SCAN) {
		auto &scan = op.Cast<PhysicalColumnDataScan>();
		scan.delim_index = optional_idx(delim_index);
		delim_scans.push_back(op);
	}
	for (auto &child : op.children) {
		GatherDelimScans(child, delim_scans, delim_index);
	}
}

PhysicalOperator &PhysicalPlanGenerator::PlanDelimJoin(LogicalComparisonJoin &op) {
	// first create the underlying join
	auto &plan = PlanComparisonJoin(op);
	// this should create a join, not a cross product
	D_ASSERT(plan.type != PhysicalOperatorType::CROSS_PRODUCT);
	// duplicate eliminated join
	// first gather the scans on the duplicate eliminated data set from the delim side
	const idx_t delim_idx = op.delim_flipped ? 0 : 1;
	vector<const_reference<PhysicalOperator>> delim_scans;
	GatherDelimScans(plan.children[delim_idx], delim_scans, ++this->delim_index);
	if (delim_scans.empty()) {
		// no duplicate eliminated scans in the delim side!
		// in this case we don't need to create a delim join
		// just push the normal join
		return plan;
	}
	vector<LogicalType> delim_types;
	vector<unique_ptr<Expression>> distinct_groups, distinct_expressions;
	for (auto &delim_expr : op.duplicate_eliminated_columns) {
		D_ASSERT(delim_expr->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref = delim_expr->Cast<BoundReferenceExpression>();
		delim_types.push_back(bound_ref.return_type);
		distinct_groups.push_back(make_uniq<BoundReferenceExpression>(bound_ref.return_type, bound_ref.index));
	}

	// we still have to create the DISTINCT clause that is used to generate the duplicate eliminated chunk
	auto &distinct = Make<PhysicalHashAggregate>(context, delim_types, std::move(distinct_expressions),
	                                             std::move(distinct_groups), op.estimated_cardinality);

	// Create the duplicate eliminated join.
	if (op.delim_flipped) {
		return Make<PhysicalRightDelimJoin>(*this, op.types, plan, distinct, delim_scans, op.estimated_cardinality,
		                                    optional_idx(this->delim_index));
	}
	return Make<PhysicalLeftDelimJoin>(*this, op.types, plan, distinct, delim_scans, op.estimated_cardinality,
	                                   optional_idx(this->delim_index));
}

} // namespace duckdb
