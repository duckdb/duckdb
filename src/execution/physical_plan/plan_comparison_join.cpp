#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
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

static bool CanPlanIndexJoin(ClientContext &context, TableScanBindData &bind_data, PhysicalTableScan &scan) {
	auto &table = bind_data.table;
	auto &transaction = DuckTransaction::Get(context, table.catalog);
	auto &local_storage = LocalStorage::Get(transaction);
	if (local_storage.Find(table.GetStorage())) {
		// transaction local appends: skip index join
		return false;
	}
	if (scan.table_filters && !scan.table_filters->filters.empty()) {
		// table scan filters
		return false;
	}
	return true;
}

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
		    join_stat->GetType().InternalType() == PhysicalType::INT128) {
			// perfect join not possible for non-integral types or hugeint
			return;
		}
	}

	// and when the build range is smaller than the threshold
	auto &stats_build = *op.join_stats[0].get(); // lhs stats
	if (!NumericStats::HasMinMax(stats_build)) {
		return;
	}
	int64_t min_value, max_value;
	if (!ExtractNumericValue(NumericStats::Min(stats_build), min_value) ||
	    !ExtractNumericValue(NumericStats::Max(stats_build), max_value)) {
		return;
	}
	int64_t build_range;
	if (!TrySubtractOperator::Operation(max_value, min_value, build_range)) {
		return;
	}

	// Fill join_stats for invisible join
	auto &stats_probe = *op.join_stats[1].get(); // rhs stats
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
	join_state.build_range = build_range;
	if (join_state.build_range > MAX_BUILD_SIZE) {
		return;
	}
	if (NumericStats::Min(stats_build) <= NumericStats::Min(stats_probe) &&
	    NumericStats::Max(stats_probe) <= NumericStats::Max(stats_build)) {
		join_state.is_probe_in_domain = true;
	}
	join_state.is_build_small = true;
	return;
}

static optional_ptr<Index> CanUseIndexJoin(TableScanBindData &tbl, Expression &expr) {
	optional_ptr<Index> result;
	tbl.table.GetStorage().info->indexes.Scan([&](Index &index) {
		if (index.unbound_expressions.size() != 1) {
			return false;
		}
		if (expr.alias == index.unbound_expressions[0]->alias) {
			result = &index;
			return true;
		}
		return false;
	});
	return result;
}

optional_ptr<Index> CheckIndexJoin(ClientContext &context, LogicalComparisonJoin &op, PhysicalOperator &plan,
                                   Expression &condition) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return nullptr;
	}
	// check if one of the tables has an index on column
	if (op.join_type != JoinType::INNER) {
		return nullptr;
	}
	if (op.conditions.size() != 1) {
		return nullptr;
	}
	// check if the child is (1) a table scan, and (2) has an index on the join condition
	if (plan.type != PhysicalOperatorType::TABLE_SCAN) {
		return nullptr;
	}
	auto &tbl_scan = plan.Cast<PhysicalTableScan>();
	auto tbl_data = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
	if (!tbl_data) {
		return nullptr;
	}
	optional_ptr<Index> result;
	if (CanPlanIndexJoin(context, *tbl_data, tbl_scan)) {
		result = CanUseIndexJoin(*tbl_data, condition);
	}
	return result;
}

static bool PlanIndexJoin(ClientContext &context, LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> &plan,
                          unique_ptr<PhysicalOperator> &left, unique_ptr<PhysicalOperator> &right,
                          optional_ptr<Index> index, bool swap_condition = false) {
	if (!index) {
		return false;
	}
	// index joins are not supported if there are pushed down table filters
	D_ASSERT(right->type == PhysicalOperatorType::TABLE_SCAN);
	auto &tbl_scan = right->Cast<PhysicalTableScan>();
	//	if (tbl_scan.table_filters && !tbl_scan.table_filters->filters.empty()) {
	//		return false;
	//	}
	// index joins are disabled if enable_optimizer is false
	if (!ClientConfig::GetConfig(context).enable_optimizer) {
		return false;
	}
	// check if the cardinality difference justifies an index join
	if (!((ClientConfig::GetConfig(context).force_index_join ||
	       left->estimated_cardinality < 0.01 * right->estimated_cardinality))) {
		return false;
	}

	// plan the index join
	if (swap_condition) {
		swap(op.conditions[0].left, op.conditions[0].right);
		swap(op.left_projection_map, op.right_projection_map);
	}
	plan = make_uniq<PhysicalIndexJoin>(op, std::move(left), std::move(right), std::move(op.conditions), op.join_type,
	                                    op.left_projection_map, op.right_projection_map, tbl_scan.column_ids, *index,
	                                    !swap_condition, op.estimated_cardinality);
	return true;
}

static bool PlanIndexJoin(ClientContext &context, LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> &plan,
                          unique_ptr<PhysicalOperator> &left, unique_ptr<PhysicalOperator> &right) {
	if (op.conditions.empty()) {
		return false;
	}
	// check if we can plan an index join on the RHS
	auto right_index = CheckIndexJoin(context, op, *right, *op.conditions[0].right);
	if (PlanIndexJoin(context, op, plan, left, right, right_index)) {
		return true;
	}
	// else check if we can plan an index join on the left side
	auto left_index = CheckIndexJoin(context, op, *left, *op.conditions[0].left);
	if (PlanIndexJoin(context, op, plan, right, left, left_index, true)) {
		return true;
	}
	return false;
}

static void RewriteJoinCondition(Expression &expr, idx_t offset) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		ref.index += offset;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { RewriteJoinCondition(child, offset); });
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

	bool has_equality = false;
	// bool has_inequality = false;
	size_t has_range = 0;
	for (size_t c = 0; c < op.conditions.size(); ++c) {
		auto &cond = op.conditions[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			has_equality = true;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			++has_range;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			// has_inequality = true;
			break;
		default:
			throw NotImplementedException("Unimplemented comparison join");
		}
	}

	bool can_merge = has_range > 0;
	bool can_iejoin = has_range >= 2 && recursive_cte_tables.empty();
	switch (op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		can_merge = can_merge && op.conditions.size() == 1;
		can_iejoin = false;
		break;
	default:
		break;
	}

	//	TODO: Extend PWMJ to handle all comparisons and projection maps
	const auto prefer_range_joins = (ClientConfig::GetConfig(context).prefer_range_joins && can_iejoin);

	unique_ptr<PhysicalOperator> plan;
	if (has_equality && !prefer_range_joins) {
		// check if we can use an index join
		if (PlanIndexJoin(context, op, plan, left, right)) {
			return plan;
		}
		// Equality join with small number of keys : possible perfect join optimization
		PerfectHashJoinStats perfect_join_stats;
		CheckForPerfectJoinOpt(op, perfect_join_stats);
		plan = make_uniq<PhysicalHashJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
		                                   op.join_type, op.left_projection_map, op.right_projection_map,
		                                   std::move(op.mark_types), op.estimated_cardinality, perfect_join_stats);

	} else {
		static constexpr const idx_t NESTED_LOOP_JOIN_THRESHOLD = 5;
		if (left->estimated_cardinality <= NESTED_LOOP_JOIN_THRESHOLD ||
		    right->estimated_cardinality <= NESTED_LOOP_JOIN_THRESHOLD) {
			can_iejoin = false;
			can_merge = false;
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
