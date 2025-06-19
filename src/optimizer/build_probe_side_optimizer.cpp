#include "duckdb/optimizer/build_probe_side_optimizer.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

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
		for (auto &cond : join.conditions) {
			std::swap(cond.left, cond.right);
			cond.comparison = FlipComparisonExpression(cond.comparison);
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
	tuple_layout.Initialize(types);
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

void BuildProbeSideOptimizer::TryFlipJoinChildren(LogicalOperator &op) const {
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
	if (right_side_build_cost > left_side_build_cost) {
		swap = true;
	}

	// swap for preferred on probe side
	if (rhs_cardinality == lhs_cardinality && !preferred_on_probe_side.empty()) {
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
}

void BuildProbeSideOptimizer::VisitOperator(LogicalOperator &op) {
	// then the currentoperator
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (HasInverseJoinType(join.join_type)) {
			FlipChildren(join);
			join.delim_flipped = true;
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
			if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
			    (op.Cast<LogicalComparisonJoin>().HasEquality(has_range) && !context.config.prefer_range_joins)) {
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
