#include "duckdb/optimizer/build_probe_side_optimizer.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/enums/join_type.hpp"

namespace duckdb {

static void GetRowidBindings(LogicalOperator &op, vector<ColumnBinding> &bindings) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto get_bindings = get.GetColumnBindings();
		auto column_ids = get.column_ids;
		if (std::find(column_ids.begin(), column_ids.end(), DConstants::INVALID_INDEX) != column_ids.end()) {
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
}

static void FlipChildren(LogicalOperator &op) {
	std::swap(op.children[0], op.children[1]);
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		join.join_type = InverseJoinType(join.join_type);
		for (auto &cond : join.conditions) {
			std::swap(cond.left, cond.right);
			cond.comparison = FlipComparisonExpression(cond.comparison);
		}
		std::swap(join.left_projection_map, join.right_projection_map);
	}
	if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &join = op.Cast<LogicalAnyJoin>();
		join.join_type = InverseJoinType(join.join_type);
		std::swap(join.left_projection_map, join.right_projection_map);
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

void BuildProbeSideOptimizer::TryFlipJoinChildren(LogicalOperator &op, idx_t cardinality_ratio) {
	auto &left_child = op.children[0];
	auto &right_child = op.children[1];
	auto lhs_cardinality = left_child->has_estimated_cardinality ? left_child->estimated_cardinality
	                                                             : left_child->EstimateCardinality(context);
	auto rhs_cardinality = right_child->has_estimated_cardinality ? right_child->estimated_cardinality
	                                                              : right_child->EstimateCardinality(context);

	if (rhs_cardinality < lhs_cardinality * cardinality_ratio) {
		return;
	}
	if (rhs_cardinality == lhs_cardinality * cardinality_ratio && !preferred_on_probe_side.empty()) {
		// inspect final bindings, we prefer them on the probe side
		auto bindings_left = left_child->GetColumnBindings();
		auto bindings_right = right_child->GetColumnBindings();
		auto bindings_in_left = ComputeOverlappingBindings(bindings_left, preferred_on_probe_side);
		auto bindings_in_right = ComputeOverlappingBindings(bindings_right, preferred_on_probe_side);
		if (bindings_in_right > bindings_in_left) {
			FlipChildren(op);
		}
		return;
	}
	FlipChildren(op);
}

void BuildProbeSideOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();

		switch (join.join_type) {
		case JoinType::INNER:
		case JoinType::OUTER:
			TryFlipJoinChildren(join);
			break;
		case JoinType::LEFT:
		case JoinType::RIGHT:
			if (join.right_projection_map.empty()) {
				TryFlipJoinChildren(join, 2);
			}
			break;
		case JoinType::SEMI:
		case JoinType::ANTI: {
			idx_t has_range = 0;
			if (!PhysicalPlanGenerator::HasEquality(join.conditions, has_range)) {
				// if the conditions have no equality, do not flip the children.
				// There is no physical join operator (yet) that can do a right_semi/anti join.
				break;
			}
			TryFlipJoinChildren(join, 2);
			break;
		}
		default:
			break;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		TryFlipJoinChildren(op, 1);
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
			TryFlipJoinChildren(join, 2);
		} else if (join.join_type == JoinType::INNER) {
			TryFlipJoinChildren(join, 1);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (HasInverseJoinType(join.join_type) && join.right_projection_map.empty()) {
			FlipChildren(join);
			join.delim_flipped = true;
		}
		break;
	}
	default:
		break;
	}
	VisitOperatorChildren(op);
}

} // namespace duckdb
