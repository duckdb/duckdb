#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

namespace duckdb {

bool ExtractNumericValue(Value val, int64_t &result) {
	if (!val.type().IsIntegral()) {
		switch (val.type().InternalType()) {
		case PhysicalType::INT16:
			result = val.GetValueUnsafe<int16_t>();
			break;
		case PhysicalType::INT32:
			result = val.GetValueUnsafe<int32_t>();
			break;
		case PhysicalType::INT64:
			result = val.GetValueUnsafe<int64_t>();
			break;
		default:
			return false;
		}
	} else {
		if (!val.DefaultTryCastAs(LogicalType::BIGINT)) {
			return false;
		}
		result = val.GetValue<int64_t>();
	}
	return true;
}

void CheckForPerfectJoinOpt(LogicalComparisonJoin &op, PerfectHashJoinStats &join_state) {
	// we only do this optimization for inner joins
	if (op.join_type != JoinType::INNER) {
		return;
	}
	// with one condition
	if (op.conditions.size() != 1) {
		return;
	}
	// with propagated statistics
	if (op.join_stats.empty()) {
		return;
	}
	for (auto &type : op.children[1]->types) {
		switch (type.InternalType()) {
		case PhysicalType::STRUCT:
		case PhysicalType::LIST:
		case PhysicalType::ARRAY:
			return;
		default:
			break;
		}
	}
	// with equality condition and null values not equal
	for (auto &&condition : op.conditions) {
		if (condition.comparison != ExpressionType::COMPARE_EQUAL) {
			return;
		}
	}
	// with integral internal types
	for (auto &&join_stat : op.join_stats) {
		if (!TypeIsInteger(join_stat->GetType().InternalType()) ||
		    join_stat->GetType().InternalType() == PhysicalType::INT128 ||
		    join_stat->GetType().InternalType() == PhysicalType::UINT128) {
			// perfect join not possible for non-integral types or hugeint
			return;
		}
	}

	// and when the build range is smaller than the threshold
	auto &stats_build = *op.join_stats[1].get(); // rhs stats
	if (!NumericStats::HasMinMax(stats_build)) {
		return;
	}
	int64_t min_value, max_value;
	if (!ExtractNumericValue(NumericStats::Min(stats_build), min_value) ||
	    !ExtractNumericValue(NumericStats::Max(stats_build), max_value)) {
		return;
	}
	if (max_value < min_value) {
		// empty table
		return;
	}
	int64_t build_range;
	if (!TrySubtractOperator::Operation(max_value, min_value, build_range)) {
		return;
	}

	// Fill join_stats for invisible join
	auto &stats_probe = *op.join_stats[0].get(); // lhs stats
	if (!NumericStats::HasMinMax(stats_probe)) {
		return;
	}

	// The max size our build must have to run the perfect HJ
	const idx_t MAX_BUILD_SIZE = 1000000;
	join_state.probe_min = NumericStats::Min(stats_probe);
	join_state.probe_max = NumericStats::Max(stats_probe);
	join_state.build_min = NumericStats::Min(stats_build);
	join_state.build_max = NumericStats::Max(stats_build);
	join_state.estimated_cardinality = op.estimated_cardinality;
	join_state.build_range = NumericCast<idx_t>(build_range);
	if (join_state.build_range > MAX_BUILD_SIZE) {
		return;
	}
	join_state.is_build_small = true;
	return;
}

static void RewriteJoinCondition(Expression &expr, idx_t offset) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		ref.index += offset;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { RewriteJoinCondition(child, offset); });
}

bool PhysicalPlanGenerator::HasEquality(vector<JoinCondition> &conds, idx_t &range_count) {
	for (size_t c = 0; c < conds.size(); ++c) {
		auto &cond = conds[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			return true;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			++range_count;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			break;
		default:
			throw NotImplementedException("Unimplemented comparison join");
		}
	}
	return false;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::PlanComparisonJoin(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	left->estimated_cardinality = lhs_cardinality;
	right->estimated_cardinality = rhs_cardinality;
	D_ASSERT(left && right);

	if (op.conditions.empty()) {
		// no conditions: insert a cross product
		return make_uniq<PhysicalCrossProduct>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
	}

	idx_t has_range = 0;
	bool has_equality = HasEquality(op.conditions, has_range);
	bool can_merge = has_range > 0;
	bool can_iejoin = has_range >= 2 && recursive_cte_tables.empty();
	switch (op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
	case JoinType::MARK:
		can_merge = can_merge && op.conditions.size() == 1;
		can_iejoin = false;
		break;
	default:
		break;
	}
	auto &client_config = ClientConfig::GetConfig(context);

	//	TODO: Extend PWMJ to handle all comparisons and projection maps
	const auto prefer_range_joins = client_config.prefer_range_joins && can_iejoin;

	unique_ptr<PhysicalOperator> plan;
	if (has_equality && !prefer_range_joins) {
		// Equality join with small number of keys : possible perfect join optimization
		PerfectHashJoinStats perfect_join_stats;
		CheckForPerfectJoinOpt(op, perfect_join_stats);
		plan =
		    make_uniq<PhysicalHashJoin>(op, std::move(left), std::move(right), std::move(op.conditions), op.join_type,
		                                op.left_projection_map, op.right_projection_map, std::move(op.mark_types),
		                                op.estimated_cardinality, perfect_join_stats, std::move(op.filter_pushdown));

	} else {
		if (left->estimated_cardinality <= client_config.nested_loop_join_threshold ||
		    right->estimated_cardinality <= client_config.nested_loop_join_threshold) {
			can_iejoin = false;
			can_merge = false;
		}
		if (can_merge && can_iejoin) {
			if (left->estimated_cardinality <= client_config.merge_join_threshold ||
			    right->estimated_cardinality <= client_config.merge_join_threshold) {
				can_iejoin = false;
			}
		}
		if (can_iejoin) {
			plan = make_uniq<PhysicalIEJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                 op.join_type, op.estimated_cardinality);
		} else if (can_merge) {
			// range join: use piecewise merge join
			plan =
			    make_uniq<PhysicalPiecewiseMergeJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                          op.join_type, op.estimated_cardinality);
		} else if (PhysicalNestedLoopJoin::IsSupported(op.conditions, op.join_type)) {
			// inequality join: use nested loop
			plan = make_uniq<PhysicalNestedLoopJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                         op.join_type, op.estimated_cardinality);
		} else {
			for (auto &cond : op.conditions) {
				RewriteJoinCondition(*cond.right, left->types.size());
			}
			auto condition = JoinCondition::CreateExpression(std::move(op.conditions));
			plan = make_uniq<PhysicalBlockwiseNLJoin>(op, std::move(left), std::move(right), std::move(condition),
			                                          op.join_type, op.estimated_cardinality);
		}
	}
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
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
