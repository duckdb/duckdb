#include "duckdb/optimizer/build_probe_side_optimizer.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static constexpr idx_t JOIN_FILTER_TARGET_MIN_CARDINALITY = 1000000;
static constexpr idx_t JOIN_FILTER_TARGET_BUILD_RATIO = 64;

static void GetRowidBindings(LogicalOperator &op, vector<ColumnBinding> &bindings) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto get_bindings = get.GetColumnBindings();
		auto &column_ids = get.GetColumnIds();
		bool has_row_id = false;
		for (auto &col_id : column_ids) {
			if (col_id.IsRowIdColumn()) {
				has_row_id = true;
				break;
			}
		}
		if (has_row_id) {
			for (auto &binding : get_bindings) {
				bindings.push_back(binding);
			}
		}
	}
	for (auto &child : op.children) {
		GetRowidBindings(*child, bindings);
	}
}

BuildProbeSideOptimizer::BuildProbeSideOptimizer(ClientContext &context, LogicalOperator &op) : context(context) {
	vector<ColumnBinding> updating_columns, current_op_bindings;
	auto bindings = op.GetColumnBindings();
	vector<ColumnBinding> row_id_bindings;
	// If any column bindings are a row_id, there is a good chance the statement is an insert/delete/update statement.
	// As an initialization step, we travers the plan and find which bindings are row_id bindings.
	// When we eventually do our build side probe side optimizations, if we get to a join where the left and right
	// cardinalities are the same, we prefer to have the child with the rowid bindings in the probe side.
	GetRowidBindings(op, preferred_on_probe_side);
	op.ResolveOperatorTypes();
}

static void FlipChildren(LogicalOperator &op) {
	std::swap(op.children[0], op.children[1]);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		join.join_type = InverseJoinType(join.join_type);
		for (idx_t i = 0; i < join.conditions.size(); i++) {
			auto &cond = join.conditions[i];
			if (cond.IsComparison()) {
				auto left_expr = cond.RightReference()->Copy();
				auto right_expr = cond.LeftReference()->Copy();
				auto flipped_comparison = FlipComparisonExpression(cond.GetComparisonType());

				join.conditions[i] = JoinCondition(std::move(left_expr), std::move(right_expr), flipped_comparison);
			}
		}
		std::swap(join.left_projection_map, join.right_projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		join.join_type = InverseJoinType(join.join_type);
		std::swap(join.left_projection_map, join.right_projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// don't need to do anything here
		return;
	}
	default:
		throw InternalException("Flipping children, but children were not flipped");
	}
}

static inline idx_t ComputeOverlappingBindings(const vector<ColumnBinding> &haystack,
                                               const vector<ColumnBinding> &needles) {
	idx_t result = 0;
	for (auto &needle : needles) {
		if (std::find(haystack.begin(), haystack.end(), needle) != haystack.end()) {
			result++;
		}
	}
	return result;
}

static bool ExtractPushdownColumn(const Expression &expr, JoinFilterPushdownColumn &filter) {
	if (expr.return_type.IsNested() || expr.return_type.id() == LogicalTypeId::INTERVAL) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
		filter.probe_column_index = expr.Cast<BoundColumnRefExpression>().binding;
		return true;
	case ExpressionClass::BOUND_CAST: {
		auto &bound_cast = expr.Cast<BoundCastExpression>();
		const auto &src = bound_cast.child->return_type;
		const auto &tgt = bound_cast.return_type;
		if (!src.IsIntegral() || !tgt.IsIntegral()) {
			return false;
		}
		if (GetTypeIdSize(src.InternalType()) > GetTypeIdSize(PhysicalType::INT64) ||
		    GetTypeIdSize(tgt.InternalType()) > GetTypeIdSize(PhysicalType::INT64)) {
			return false;
		}
		return ExtractPushdownColumn(*bound_cast.child, filter);
	}
	default:
		return false;
	}
}

static idx_t MaxPushdownTargetCardinality(LogicalOperator &op, const Expression &expr) {
	JoinFilterPushdownColumn column;
	if (!ExtractPushdownColumn(expr, column)) {
		return 0;
	}

	vector<JoinFilterPushdownColumn> columns;
	columns.push_back(column);
	vector<PushdownFilterTarget> targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(op, std::move(columns), targets);

	idx_t result = 0;
	for (auto &target : targets) {
		auto &get = target.get;
		if (!get.table_filters.HasFilters()) {
			continue;
		}
		result = MaxValue(result, get.has_estimated_cardinality ? get.estimated_cardinality : idx_t(0));
	}
	return result;
}

static idx_t DynamicFilterBuildScore(LogicalComparisonJoin &join, const idx_t probe_idx, const idx_t build_idx,
                                     const idx_t build_cardinality) {
	if (!JoinFilterPushdownOptimizer::IsFiltering(join.children[build_idx])) {
		return 0;
	}

	idx_t max_target_cardinality = 0;
	for (auto &cond : join.conditions) {
		if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &probe_expr = probe_idx == 0 ? cond.GetLHS() : cond.GetRHS();
		max_target_cardinality =
		    MaxValue(max_target_cardinality, MaxPushdownTargetCardinality(*join.children[probe_idx], probe_expr));
	}
	if (max_target_cardinality < JOIN_FILTER_TARGET_MIN_CARDINALITY) {
		return 0;
	}
	if (build_cardinality > 0 && build_cardinality > max_target_cardinality / JOIN_FILTER_TARGET_BUILD_RATIO) {
		return 0;
	}
	return max_target_cardinality / MaxValue<idx_t>(build_cardinality, 1);
}

BuildSize BuildProbeSideOptimizer::GetBuildSizes(const LogicalOperator &op, const idx_t lhs_cardinality,
                                                 const idx_t rhs_cardinality) {
	BuildSize ret;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		ret.left_side = GetBuildSize(op.children[0]->types, lhs_cardinality);
		ret.right_side = GetBuildSize(op.children[1]->types, rhs_cardinality);
		return ret;
	}
	default:
		break;
	}
	return ret;
}

double BuildProbeSideOptimizer::GetBuildSize(vector<LogicalType> types, const idx_t cardinality) {
	// Row width in the hash table
	types.push_back(LogicalType::HASH);
	auto tuple_layout = TupleDataLayout();
	tuple_layout.Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	auto row_width = tuple_layout.GetRowWidth();

	for (const auto &type : types) {
		TypeVisitor::VisitReplace(type, [&](const LogicalType &visited_type) {
			// Penalty for variable-size types (we don't have statistics here yet)
			switch (visited_type.InternalType()) {
			case PhysicalType::VARCHAR:
				row_width += 8;
				break;
			case PhysicalType::LIST:
			case PhysicalType::ARRAY:
				row_width += 32;
				break;
			default:
				break;
			}

			// Penalty for number of (recursive) columns
			row_width += COLUMN_COUNT_PENALTY;

			return visited_type;
		});
	}

	// There is also a cost of NextPowerOfTwo(count * 2) * sizeof(ht_entry_t) per tuple in the hash table
	// This is a not a smooth cost function, so instead we do the average, which is ~3 * sizeof(ht_entry_t)
	row_width += 3 * sizeof(ht_entry_t);

	return static_cast<double>(row_width * cardinality);
}

idx_t BuildProbeSideOptimizer::ChildHasJoins(LogicalOperator &op) {
	if (op.children.empty()) {
		return 0;
	} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	           op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
	           op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return 1 + ChildHasJoins(*op.children[0]) + ChildHasJoins(*op.children[1]);
	}
	return ChildHasJoins(*op.children[0]);
}

bool BuildProbeSideOptimizer::TryFlipJoinChildren(LogicalOperator &op) const {
	auto &left_child = *op.children[0];
	auto &right_child = *op.children[1];
	const auto lhs_cardinality = left_child.has_estimated_cardinality ? left_child.estimated_cardinality
	                                                                  : left_child.EstimateCardinality(context);
	const auto rhs_cardinality = right_child.has_estimated_cardinality ? right_child.estimated_cardinality
	                                                                   : right_child.EstimateCardinality(context);

	auto build_sizes = GetBuildSizes(op, lhs_cardinality, rhs_cardinality);
	auto &left_side_build_cost = build_sizes.left_side;
	auto &right_side_build_cost = build_sizes.right_side;

	bool swap = false;
	bool has_dynamic_filter_preference = false;

	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			const auto right_build_score = DynamicFilterBuildScore(join, 0, 1, rhs_cardinality);
			const auto left_build_score = DynamicFilterBuildScore(join, 1, 0, lhs_cardinality);
			if (right_build_score > left_build_score * 4) {
				swap = false;
				has_dynamic_filter_preference = true;
			} else if (left_build_score > right_build_score * 4) {
				swap = true;
				has_dynamic_filter_preference = true;
			}
		}
	}

	idx_t left_child_joins = ChildHasJoins(*op.children[0]);
	idx_t right_child_joins = ChildHasJoins(*op.children[1]);
	// if the right child is a table scan, and the left child has joins, we should prefer the left child
	// to be the build side. Since the tuples of the left side will already have been built on/be in flight,
	// it will be faster to build on them again.
	if (right_child_joins == 0 && left_child_joins > 0) {
		right_side_build_cost *= (1 + PREFER_RIGHT_DEEP_PENALTY);
	}

	// RHS is build side.
	// if right_side metric is larger than left_side metric, then right_side is more costly to build on
	// than the lhs. So we swap
	if (!has_dynamic_filter_preference && right_side_build_cost > left_side_build_cost) {
		swap = true;
	}

	// swap for preferred on probe side
	if (!has_dynamic_filter_preference && rhs_cardinality == lhs_cardinality && !preferred_on_probe_side.empty()) {
		// inspect final bindings, we prefer them on the probe side
		auto bindings_left = left_child.GetColumnBindings();
		auto bindings_right = right_child.GetColumnBindings();
		auto bindings_in_left = ComputeOverlappingBindings(bindings_left, preferred_on_probe_side);
		auto bindings_in_right = ComputeOverlappingBindings(bindings_right, preferred_on_probe_side);
		// (if the sides are planning to be swapped AND
		// if more projected bindings are in the left (meaning right/build side after the swap)
		// then swap them back. The projected bindings stay in the left/probe side.)
		// OR
		// (if the sides are planning not to be swapped AND
		// if more projected bindings are in the right (meaning right/build)
		// then swap them. The projected bindings are swapped to the left/probe side.)
		if ((swap && bindings_in_left > bindings_in_right) || (!swap && bindings_in_right > bindings_in_left)) {
			swap = !swap;
		}
	}

	if (swap) {
		FlipChildren(op);
	}
	return swap;
}

void BuildProbeSideOptimizer::VisitOperator(LogicalOperator &op) {
	// then the currentoperator
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (HasInverseJoinType(join.join_type)) {
			join.delim_flipped = TryFlipJoinChildren(join);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::SEMI:
		case JoinType::ANTI: {
			// if the conditions have no equality, do not flip the children.
			// There is no physical join operator (yet) that can do an inequality right_semi/anti join.
			idx_t has_range = 0;
			bool prefer_range_joins = Settings::Get<PreferRangeJoinsSetting>(context);
			if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
			    (op.Cast<LogicalComparisonJoin>().HasEquality(has_range) && !prefer_range_joins)) {
				TryFlipJoinChildren(join);
			}
			break;
		}
		default:
			if (HasInverseJoinType(join.join_type)) {
				TryFlipJoinChildren(op);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		// We do not yet support the RIGHT_SEMI or RIGHT_ANTI join types for these, so don't try to flip
		switch (join.join_type) {
		case JoinType::SEMI:
		case JoinType::ANTI:
			break; // RIGHT_SEMI/RIGHT_ANTI not supported yet for ANY/ASOF
		default:
			// We cannot flip projection maps are set (YET), but not flipping is worse than just clearing them
			// They will be set in the 2nd round of ColumnLifetimeAnalyzer
			join.left_projection_map.clear();
			join.right_projection_map.clear();
			TryFlipJoinChildren(op);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		TryFlipJoinChildren(op);
		break;
	}
	default:
		break;
	}

	VisitOperatorChildren(op);
}

} // namespace duckdb
