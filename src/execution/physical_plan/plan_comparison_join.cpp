#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

namespace duckdb {

static bool CanPlanIndexJoin(ClientContext &context, TableScanBindData *bind_data, PhysicalTableScan &scan) {
	if (!bind_data) {
		// not a table scan
		return false;
	}
	auto table = bind_data->table;
	auto &transaction = DuckTransaction::Get(context, *table->catalog);
	auto &local_storage = LocalStorage::Get(transaction);
	if (local_storage.Find(table->GetStoragePtr())) {
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

static void CanUseIndexJoin(TableScanBindData *tbl, Expression &expr, Index **result_index) {
	tbl->table->GetStorage().info->indexes.Scan([&](Index &index) {
		if (index.unbound_expressions.size() != 1) {
			return false;
		}
		if (expr.alias == index.unbound_expressions[0]->alias) {
			*result_index = &index;
			return true;
		}
		return false;
	});
}

void TransformIndexJoin(ClientContext &context, LogicalComparisonJoin &op, Index **left_index, Index **right_index,
                        PhysicalOperator *left, PhysicalOperator *right) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}
	// check if one of the tables has an index on column
	if (op.join_type == JoinType::INNER && op.conditions.size() == 1) {
		// check if one of the children are table scans and if they have an index in the join attribute
		// (op.condition)
		if (left->type == PhysicalOperatorType::TABLE_SCAN) {
			auto &tbl_scan = (PhysicalTableScan &)*left;
			auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
			if (CanPlanIndexJoin(context, tbl, tbl_scan)) {
				CanUseIndexJoin(tbl, *op.conditions[0].left, left_index);
			}
		}
		if (right->type == PhysicalOperatorType::TABLE_SCAN) {
			auto &tbl_scan = (PhysicalTableScan &)*right;
			auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
			if (CanPlanIndexJoin(context, tbl, tbl_scan)) {
				CanUseIndexJoin(tbl, *op.conditions[0].right, right_index);
			}
		}
	}
}

static void RewriteJoinCondition(Expression &expr, idx_t offset) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &ref = (BoundReferenceExpression &)expr;
		ref.index += offset;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { RewriteJoinCondition(child, offset); });
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	D_ASSERT(left && right);

	if (op.conditions.empty()) {
		// no conditions: insert a cross product
		return make_unique<PhysicalCrossProduct>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
	}

	if (op.join_type == JoinType::ASOF) {
		//	Validate
		vector<idx_t> equi_indexes;
		auto asof_idx = op.conditions.size();
		for (size_t c = 0; c < op.conditions.size(); ++c) {
			auto &cond = op.conditions[c];
			switch (cond.comparison) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				equi_indexes.emplace_back(c);
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				if (asof_idx < op.conditions.size()) {
					throw NotImplementedException("Multiple ASOF JOIN inequalities");
				}
				asof_idx = c;
				break;
			default:
				throw NotImplementedException("Unimplemented ASOF JOIN condition");
			}
		}
		if (asof_idx == op.conditions.size()) {
			throw NotImplementedException("Missing ASOF JOIN inequality");
		}

		//	 Temporary implementation: IEJoin of Window
		//	LEAD(asof_column, 1, infinity) OVER (PARTITION BY equi_column... ORDER BY asof_column) AS asof_temp
		auto &asof_comp = op.conditions[asof_idx];
		auto &asof_column = asof_comp.right;
		auto asof_type = asof_column->return_type;
		auto asof_temp = make_unique<BoundWindowExpression>(ExpressionType::WINDOW_LEAD, asof_type, nullptr, nullptr);
		asof_temp->children.emplace_back(asof_column->Copy());
		asof_temp->offset_expr = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		asof_temp->default_expr = make_unique<BoundConstantExpression>(Value::Infinity(asof_type));
		for (auto equi_idx : equi_indexes) {
			asof_temp->partitions.emplace_back(op.conditions[equi_idx].right->Copy());
		}
		asof_temp->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, asof_column->Copy());
		asof_temp->start = WindowBoundary::UNBOUNDED_PRECEDING;
		asof_temp->end = WindowBoundary::CURRENT_ROW_ROWS;

		vector<unique_ptr<Expression>> window_select;
		window_select.emplace_back(std::move(asof_temp));

		auto window_types = right->types;
		window_types.emplace_back(asof_type);

		auto window = make_unique<PhysicalWindow>(window_types, std::move(window_select), rhs_cardinality);
		window->children.emplace_back(std::move(right));

		// IEJoin(left, window, conditions || asof_column < asof_temp)
		JoinCondition asof_upper;
		asof_upper.left = asof_comp.left->Copy();
		asof_upper.right = make_unique<BoundReferenceExpression>(asof_type, window_types.size() - 1);
		asof_upper.comparison = ExpressionType::COMPARE_LESSTHAN;

		//	Project away asof_temp
		auto proj_types = op.types;
		vector<unique_ptr<Expression>> proj_selects(proj_types.size());
		for (storage_t i = 0; i < proj_types.size(); ++i) {
			proj_selects[i] = make_unique<BoundReferenceExpression>(proj_types[i], i);
		}

		op.types.emplace_back(asof_type);
		op.conditions.emplace_back(std::move(asof_upper));
		auto iejoin = make_unique<PhysicalIEJoin>(op, std::move(left), std::move(window), std::move(op.conditions),
		                                          JoinType::LEFT, op.estimated_cardinality);

		auto proj =
		    make_unique<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), op.estimated_cardinality);
		proj->children.push_back(std::move(iejoin));

		return proj;
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

	unique_ptr<PhysicalOperator> plan;
	if (has_equality) {
		Index *left_index {}, *right_index {};
		TransformIndexJoin(context, op, &left_index, &right_index, left.get(), right.get());
		if (left_index &&
		    (ClientConfig::GetConfig(context).force_index_join || rhs_cardinality < 0.01 * lhs_cardinality)) {
			auto &tbl_scan = (PhysicalTableScan &)*left;
			swap(op.conditions[0].left, op.conditions[0].right);
			return make_unique<PhysicalIndexJoin>(op, std::move(right), std::move(left), std::move(op.conditions),
			                                      op.join_type, op.right_projection_map, op.left_projection_map,
			                                      tbl_scan.column_ids, left_index, false, op.estimated_cardinality);
		}
		if (right_index &&
		    (ClientConfig::GetConfig(context).force_index_join || lhs_cardinality < 0.01 * rhs_cardinality)) {
			auto &tbl_scan = (PhysicalTableScan &)*right;
			return make_unique<PhysicalIndexJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                      op.join_type, op.left_projection_map, op.right_projection_map,
			                                      tbl_scan.column_ids, right_index, true, op.estimated_cardinality);
		}
		// Equality join with small number of keys : possible perfect join optimization
		PerfectHashJoinStats perfect_join_stats;
		CheckForPerfectJoinOpt(op, perfect_join_stats);
		plan = make_unique<PhysicalHashJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
		                                     op.join_type, op.left_projection_map, op.right_projection_map,
		                                     std::move(op.delim_types), op.estimated_cardinality, perfect_join_stats);

	} else {
		static constexpr const idx_t NESTED_LOOP_JOIN_THRESHOLD = 5;
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
		if (left->estimated_cardinality <= NESTED_LOOP_JOIN_THRESHOLD ||
		    right->estimated_cardinality <= NESTED_LOOP_JOIN_THRESHOLD) {
			can_iejoin = false;
			can_merge = false;
		}
		if (can_iejoin) {
			plan = make_unique<PhysicalIEJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                   op.join_type, op.estimated_cardinality);
		} else if (can_merge) {
			// range join: use piecewise merge join
			plan =
			    make_unique<PhysicalPiecewiseMergeJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                            op.join_type, op.estimated_cardinality);
		} else if (PhysicalNestedLoopJoin::IsSupported(op.conditions, op.join_type)) {
			// inequality join: use nested loop
			plan = make_unique<PhysicalNestedLoopJoin>(op, std::move(left), std::move(right), std::move(op.conditions),
			                                           op.join_type, op.estimated_cardinality);
		} else {
			for (auto &cond : op.conditions) {
				RewriteJoinCondition(*cond.right, left->types.size());
			}
			auto condition = JoinCondition::CreateExpression(std::move(op.conditions));
			plan = make_unique<PhysicalBlockwiseNLJoin>(op, std::move(left), std::move(right), std::move(condition),
			                                            op.join_type, op.estimated_cardinality);
		}
	}
	return plan;
}

} // namespace duckdb
