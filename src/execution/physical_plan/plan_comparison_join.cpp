#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/operator/join/physical_recursive_cte_key_join.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {
static void RewriteJoinCondition(unique_ptr<Expression> &root_expr, idx_t offset) {
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    root_expr, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &expr) { ref.IndexMutable() += offset; });
}

static optional_ptr<PhysicalRecursiveCTEStateScan> FindDirectRecursiveStateScan(
    PhysicalOperator &op,
    const unordered_map<TableIndex, vector<reference<PhysicalRecursiveCTEStateScan>>> &recursive_state_scans) {
	if (op.type != PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN) {
		return nullptr;
	}
	for (auto &entry : recursive_state_scans) {
		for (auto &scan_ref : entry.second) {
			if (&scan_ref.get() == &op) {
				return scan_ref.get();
			}
		}
	}
	return nullptr;
}

static bool TryGetRecursiveKeyProbe(LogicalComparisonJoin &op, PhysicalOperator &left, PhysicalOperator &right,
                                    optional_ptr<PhysicalRecursiveCTEStateScan> left_state,
                                    optional_ptr<PhysicalRecursiveCTEStateScan> right_state,
                                    vector<idx_t> &probe_key_indices, bool &state_on_left) {
	if (op.join_type != JoinType::INNER || (left_state && right_state)) {
		return false;
	}
	auto state_scan = left_state ? left_state : right_state;
	if (!state_scan) {
		return false;
	}
	state_on_left = left_state != nullptr;
	auto &probe = state_on_left ? right : left;
	if (op.conditions.size() != state_scan->distinct_idx.size()) {
		return false;
	}

	vector<idx_t> state_key_map(state_scan->GetTypes().size(), DConstants::INVALID_INDEX);
	for (idx_t key_idx = 0; key_idx < state_scan->distinct_idx.size(); key_idx++) {
		state_key_map[state_scan->distinct_idx[key_idx]] = key_idx;
	}
	probe_key_indices.resize(state_scan->distinct_idx.size(), DConstants::INVALID_INDEX);
	for (auto &condition : op.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		auto &state_expr = state_on_left ? condition.GetLHS() : condition.GetRHS();
		auto &probe_expr = state_on_left ? condition.GetRHS() : condition.GetLHS();
		if (state_expr.GetExpressionClass() != ExpressionClass::BOUND_REF ||
		    probe_expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return false;
		}
		auto &state_ref = state_expr.Cast<BoundReferenceExpression>();
		auto &probe_ref = probe_expr.Cast<BoundReferenceExpression>();
		if (state_ref.Index() >= state_key_map.size() || probe_ref.Index() >= probe.GetTypes().size()) {
			return false;
		}
		const auto key_idx = state_key_map[state_ref.Index()];
		if (key_idx == DConstants::INVALID_INDEX || probe_key_indices[key_idx] != DConstants::INVALID_INDEX) {
			return false;
		}
		const auto &key_type = state_scan->GetTypes()[state_ref.Index()];
		if (key_type.IsNested() || key_type != probe.GetTypes()[probe_ref.Index()] ||
		    key_type != state_ref.GetReturnType() || key_type != probe_ref.GetReturnType()) {
			return false;
		}
		probe_key_indices[key_idx] = probe_ref.Index();
	}
	return true;
}

PhysicalOperator &PhysicalPlanGenerator::PlanComparisonJoin(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto &left = CreatePlan(*op.children[0]);
	auto &right = CreatePlan(*op.children[1]);
	left.estimated_cardinality = lhs_cardinality;
	right.estimated_cardinality = rhs_cardinality;
	auto left_state = FindDirectRecursiveStateScan(left, recursive_state_scans);
	auto right_state = FindDirectRecursiveStateScan(right, recursive_state_scans);
	vector<idx_t> probe_key_indices;
	bool state_on_left;
	if (TryGetRecursiveKeyProbe(op, left, right, left_state, right_state, probe_key_indices, state_on_left)) {
		auto &state_scan = state_on_left ? *left_state : *right_state;
		auto &probe = state_on_left ? right : left;
		auto left_projection_map = PhysicalJoin::FillProjectionMap(left, op.left_projection_map);
		auto right_projection_map = PhysicalJoin::FillProjectionMap(right, op.right_projection_map);
		return Make<PhysicalRecursiveCTEKeyJoin>(op, probe, state_scan, state_on_left, std::move(probe_key_indices),
		                                         std::move(left_projection_map), std::move(right_projection_map),
		                                         op.estimated_cardinality);
	}

	if (op.conditions.empty()) {
		// no conditions: insert a cross product
		return Make<PhysicalCrossProduct>(op.types, left, right, op.estimated_cardinality);
	}

	idx_t has_range = 0;
	bool has_equality = op.HasEquality(has_range);
	bool can_merge = has_range > 0;
	bool can_iejoin = has_range >= 2 && recursive_cte_tables.empty();
	switch (op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		can_merge = can_merge && op.conditions.size() == 1;
		break;
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		can_merge = can_merge && op.conditions.size() == 1;
		can_iejoin = false;
		break;
	default:
		break;
	}

	//	TODO: Extend PWMJ to handle all comparisons and projection maps
	bool prefer_range_joins = Settings::Get<PreferRangeJoinsSetting>(context);
	prefer_range_joins = prefer_range_joins && can_iejoin;
	if (has_equality && !prefer_range_joins) {
		// pass separately to PhysicalHashJoin
		auto &join = Make<PhysicalHashJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                    op.left_projection_map, op.right_projection_map, std::move(op.mark_types),
		                                    op.estimated_cardinality, std::move(op.filter_pushdown));
		return join;
	}

	D_ASSERT(op.left_projection_map.empty());
	idx_t nested_loop_join_threshold = Settings::Get<NestedLoopJoinThresholdSetting>(context);
	if (left.estimated_cardinality < nested_loop_join_threshold ||
	    right.estimated_cardinality < nested_loop_join_threshold) {
		can_iejoin = false;
		can_merge = false;
	}

	if (can_merge && can_iejoin) {
		idx_t merge_join_threshold = Settings::Get<MergeJoinThresholdSetting>(context);
		if (left.estimated_cardinality < merge_join_threshold || right.estimated_cardinality < merge_join_threshold) {
			can_iejoin = false;
		}
	}

	if (can_iejoin) {
		return Make<PhysicalIEJoin>(op, left, right, std::move(op.conditions), op.join_type, op.estimated_cardinality,
		                            std::move(op.filter_pushdown));
	}
	if (can_merge) {
		// range join: use piecewise merge join
		return Make<PhysicalPiecewiseMergeJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                        op.estimated_cardinality, std::move(op.filter_pushdown));
	}
	if (PhysicalNestedLoopJoin::IsSupported(op.conditions, op.join_type)) {
		// inequality join: use nested loop
		return Make<PhysicalNestedLoopJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                    op.estimated_cardinality, std::move(op.filter_pushdown));
	}

	for (auto &cond : op.conditions) {
		if (cond.IsComparison()) {
			RewriteJoinCondition(cond.RightReference(), left.types.size());
		}
	}
	auto condition = JoinCondition::CreateExpression(std::move(op.conditions));
	return Make<PhysicalBlockwiseNLJoin>(op, left, right, std::move(condition), op.join_type, op.estimated_cardinality);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return PlanAsOfJoin(op);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return PlanComparisonJoin(op);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PlanDelimJoin(op);
	default:
		throw InternalException("Unrecognized operator type for LogicalComparisonJoin");
	}
}

} // namespace duckdb
