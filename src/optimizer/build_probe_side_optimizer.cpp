#include "duckdb/optimizer/build_probe_side_optimizer.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/enums/join_type.hpp"

namespace duckdb {

BuildProbeSideOptimizer::BuildProbeSideOptimizer(ClientContext &context) : context(context) {
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

BuildSize BuildProbeSideOptimizer::GetBuildSide(LogicalOperator &op) {
	BuildSize ret;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &left_child = op.children[0];
		auto &right_child = op.children[1];

		auto left_column_count = left_child->GetColumnBindings();
		auto right_column_count = right_child->GetColumnBindings();

		// resolve operator types to determine how big the build side is going to be
		op.ResolveOperatorTypes();
		auto left_column_types = left_child->types;
		auto right_column_types = right_child->types;

		idx_t left_build_side = 0;
		for (auto &type : left_column_types) {
			left_build_side += GetTypeIdSize(type.InternalType());
		}
		idx_t right_build_side = 0;
		for (auto &type : right_column_types) {
			right_build_side += GetTypeIdSize(type.InternalType());
		}
		// Don't multiply by cardinalities, the only important metric is the size of the row
		// in the hash table
		ret.left_side = left_build_side;
		ret.right_side = right_build_side;
		return ret;
	}
	default:
		break;
	}
	return ret;
}

void BuildProbeSideOptimizer::TryFlipJoinChildren(LogicalOperator &op, idx_t cardinality_ratio) {
	auto &left_child = op.children[0];
	auto &right_child = op.children[1];
	auto lhs_cardinality = left_child->has_estimated_cardinality ? left_child->estimated_cardinality
	                                                             : left_child->EstimateCardinality(context);
	auto rhs_cardinality = right_child->has_estimated_cardinality ? right_child->estimated_cardinality
	                                                              : right_child->EstimateCardinality(context);

	auto build_sizes = GetBuildSide(op);

	auto flip_bc_cardinality = rhs_cardinality > (lhs_cardinality * cardinality_ratio);

	// special math.
	auto left_side_metric = lhs_cardinality * cardinality_ratio * build_sizes.left_side;
	auto right_side_metric = rhs_cardinality * build_sizes.right_side;
	// if the right side has a larger possible build size, build on the left.
	if (right_side_metric > left_side_metric) {
		FlipChildren(op);
	}
	// last option is both stats say don't flip. So we return.
	return;
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
