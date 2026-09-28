#include "duckdb/cascade/groupby_reorder.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! The GroupBy being reordered around, plus the projection that may sit on top of
//! it. A derived table is planned as `Projection(GroupBy(...))`, and a predicate
//! above it names the projection's columns rather than the GroupBy's, so the rule
//! has to see through such a projection. It is transparent exactly when it only
//! renames or reorders columns of the GroupBy - which is checked per referenced
//! column rather than for the projection as a whole, because a derived table
//! normally exposes an aggregate value as well.
struct GroupBySide {
	LogicalAggregate *aggregate = nullptr;
	LogicalProjection *projection = nullptr;
};

//! Resolve one column reference to the grouping column it stands for.
static bool ResolveGroupingColumn(const BoundColumnRefExpression &colref, const GroupBySide &side,
                                  idx_t &group_index) {
	if (side.projection) {
		if (colref.Binding().table_index != side.projection->table_index) {
			return false;
		}
		auto position = colref.Binding().column_index.GetIndexUnsafe();
		if (position >= side.projection->expressions.size()) {
			return false;
		}
		auto &mapped = *side.projection->expressions[position];
		if (mapped.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		GroupBySide lower {side.aggregate, nullptr};
		return ResolveGroupingColumn(mapped.Cast<BoundColumnRefExpression>(), lower, group_index);
	}
	if (colref.Binding().table_index != side.aggregate->group_index) {
		return false;
	}
	auto index = colref.Binding().column_index.GetIndexUnsafe();
	if (index >= side.aggregate->groups.size()) {
		return false;
	}
	group_index = index;
	return true;
}

//! True if the expression reads anything of the GroupBy side at all.
static bool ReadsGroupBySide(const Expression &expr, const GroupBySide &side) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    auto table_index = colref.Binding().table_index;
		    if (table_index == side.aggregate->group_index || table_index == side.aggregate->aggregate_index) {
			    found = true;
		    }
		    if (side.projection && table_index == side.projection->table_index) {
			    found = true;
		    }
	    });
	return found;
}

//! A GroupBy with several grouping sets pads the columns a set does not mention with
//! NULL, so a predicate on such a column is *not* constant within a group and must
//! not move below it. DuckDB's own filter pushdown carries the same guard; an empty
//! grouping_sets means the plain single set the ordinary GROUP BY produces.
static bool IsInEveryGroupingSet(const LogicalAggregate &aggregate, idx_t group_index) {
	if (aggregate.grouping_sets.empty()) {
		return true;
	}
	for (auto &set : aggregate.grouping_sets) {
		if (set.find(ProjectionIndex(group_index)) == set.end()) {
			return false;
		}
	}
	return true;
}

//! The paper's condition, in the form that can be decided from the plan alone: every
//! column the predicate reads must be one of the grouping columns, because a column
//! that *is* a grouping column is certainly functionally determined by them, and a
//! predicate over grouping columns has the same value for every row of a group.
//! Re-evaluating the grouping expression below the aggregate must also be safe, so a
//! volatile one blocks the move.
static bool IsConstantWithinGroup(const Expression &expr, const GroupBySide &side) {
	vector<idx_t> indices;
	bool valid = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    idx_t index;
		    if (!ResolveGroupingColumn(colref, side, index)) {
			    valid = false;
			    return;
		    }
		    if (!IsInEveryGroupingSet(*side.aggregate, index)) {
			    valid = false;
			    return;
		    }
		    for (auto existing : indices) {
			    if (existing == index) {
				    return;
			    }
		    }
		    indices.push_back(index);
	    });
	if (!valid || indices.empty()) {
		// A predicate with no column of the GroupBy side in it is not worth moving,
		// and one that reads an aggregate result cannot move at all.
		return false;
	}
	for (auto index : indices) {
		if (side.aggregate->groups[index]->IsVolatile()) {
			return false;
		}
	}
	return true;
}

//! Re-express a predicate below the GroupBy: the grouping columns do not exist down
//! there, so each reference becomes the expression that computed it.
static void SubstituteGroupingColumns(unique_ptr<Expression> &expr, const GroupBySide &side) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &owner) {
		    idx_t index;
		    if (!ResolveGroupingColumn(colref, side, index)) {
			    return;
		    }
		    owner = side.aggregate->groups[index]->Copy();
	    });
}

//! Move the predicates that are constant within a group below the GroupBy, keeping
//! the rest - HAVING on an aggregate result, above all - where they were.
static unique_ptr<LogicalOperator> PushFilterBelow(unique_ptr<LogicalOperator> plan) {
	auto &filter = plan->Cast<LogicalFilter>();
	if (filter.children.size() != 1) {
		return plan;
	}
	GroupBySide side;
	auto &child = *filter.children[0];
	if (child.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		side.aggregate = &child.Cast<LogicalAggregate>();
	} else if (child.type == LogicalOperatorType::LOGICAL_PROJECTION && child.children.size() == 1 &&
	           child.children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		side.projection = &child.Cast<LogicalProjection>();
		side.aggregate = &child.children[0]->Cast<LogicalAggregate>();
	} else {
		return plan;
	}

	// Split into the predicates that can move and those that cannot. Nothing is
	// changed until it is known that at least one moves: a filter left holding
	// moved-from predicates would be a broken plan that only shows up much later.
	vector<unique_ptr<Expression>> pushable;
	vector<unique_ptr<Expression>> remaining;
	for (auto &expr : filter.expressions) {
		if (IsConstantWithinGroup(*expr, side)) {
			pushable.push_back(std::move(expr));
		} else {
			remaining.push_back(std::move(expr));
		}
	}
	if (pushable.empty()) {
		filter.expressions = std::move(remaining);
		return plan;
	}
	for (auto &expr : pushable) {
		SubstituteGroupingColumns(expr, side);
	}
	auto pushed = make_uniq<LogicalFilter>();
	pushed->expressions = std::move(pushable);
	pushed->children.push_back(std::move(side.aggregate->children[0]));
	side.aggregate->children[0] = std::move(pushed);

	auto top = std::move(filter.children[0]);
	if (remaining.empty()) {
		// nothing is left above the GroupBy: the filter itself disappears
		return top;
	}
	filter.expressions = std::move(remaining);
	filter.children[0] = std::move(top);
	return plan;
}

//! `(G_{A,F} R) semijoin_p S  =  G_{A,F}( R semijoin_p' S )`.
//!
//! The paper derives this from the filter case: a semijoin includes or excludes rows
//! based on column values, so it is a filter, and the same condition applies - the
//! predicate must not read an aggregate result and its GroupBy-side columns must be
//! determined by the grouping columns. Antijoin is the same rule, and is in fact
//! where the condition earns its keep: if a predicate could vary within a group, an
//! antijoin below the aggregate would keep the group alive on its non-matching rows,
//! while above the aggregate the whole group is removed.
static unique_ptr<LogicalOperator> PushSemiJoinBelow(unique_ptr<LogicalOperator> plan) {
	auto &join = plan->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::SEMI && join.join_type != JoinType::ANTI) {
		return plan;
	}
	if (join.children.size() != 2) {
		return plan;
	}
	GroupBySide side;
	auto &left = *join.children[0];
	if (left.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		side.aggregate = &left.Cast<LogicalAggregate>();
	} else if (left.type == LogicalOperatorType::LOGICAL_PROJECTION && left.children.size() == 1 &&
	           left.children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		side.projection = &left.Cast<LogicalProjection>();
		side.aggregate = &left.children[0]->Cast<LogicalAggregate>();
	} else {
		return plan;
	}
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			return plan;
		}
		// The other side is resolved against the other relation; if it reads the
		// GroupBy side, moving the join down would strand it.
		if (ReadsGroupBySide(condition.GetRHS(), side)) {
			return plan;
		}
		if (!IsConstantWithinGroup(condition.GetLHS(), side)) {
			return plan;
		}
	}

	auto join_owner = std::move(plan);
	auto &join_ref = join_owner->Cast<LogicalComparisonJoin>();
	auto top = std::move(join_ref.children[0]);
	for (auto &condition : join_ref.conditions) {
		SubstituteGroupingColumns(condition.LeftReference(), side);
	}
	// The join keeps filtering, it just does it before the aggregation now, and the
	// aggregate keeps producing exactly the same output columns.
	auto body = std::move(side.aggregate->children[0]);
	join_ref.children[0] = std::move(body);
	// As in the local aggregate pass: the join's projection map names positions in the
	// child it was built for, and that child has just been replaced by the GroupBy's
	// input. The GroupBy above it names bindings, so dropping the map is safe.
	join_ref.left_projection_map.clear();
	join_ref.right_projection_map.clear();
	side.aggregate->children[0] = std::move(join_owner);
	return top;
}

unique_ptr<LogicalOperator> ReorderGroupBy(unique_ptr<LogicalOperator> plan, bool move_semijoins) {
	for (auto &child : plan->children) {
		child = ReorderGroupBy(std::move(child), move_semijoins);
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return PushFilterBelow(std::move(plan));
	// Only an ordinary comparison join: a delimited join carries duplicate
	// elimination machinery that expects the shape it was built for.
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return move_semijoins ? PushSemiJoinBelow(std::move(plan)) : std::move(plan);
	default:
		return plan;
	}
}

} // namespace duckdb
