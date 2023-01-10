#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

class DeliminatorPlanUpdater : LogicalOperatorVisitor {
public:
	DeliminatorPlanUpdater() {
	}
	//! Update the plan after a DelimGet has been removed
	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override;

public:
	expression_map_t<Expression *> expr_map;
	column_binding_map_t<bool> projection_map;
	column_binding_map_t<Expression *> reverse_proj_or_agg_map;
	unique_ptr<LogicalOperator> temp_ptr;
};

static idx_t DelimGetCount(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return 1;
	}
	idx_t child_count = 0;
	for (auto &child : op.children) {
		child_count += DelimGetCount(*child);
	}
	return child_count;
}

static bool IsEqualityJoinCondition(JoinCondition &cond) {
	switch (cond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool InequalityDelimJoinCanBeEliminated(JoinType &join_type) {
	switch (join_type) {
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::SEMI:
	case JoinType::SINGLE:
		return true;
	default:
		return false;
	}
}

void DeliminatorPlanUpdater::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN && DelimGetCount(op) == 0) {
		auto &delim_join = (LogicalDelimJoin &)op;
		auto decs = &delim_join.duplicate_eliminated_columns;
		for (auto &cond : delim_join.conditions) {
			if (!IsEqualityJoinCondition(cond)) {
				continue;
			}
			auto rhs = cond.right.get();
			while (rhs->type == ExpressionType::OPERATOR_CAST) {
				auto &cast = (BoundCastExpression &)*rhs;
				rhs = cast.child.get();
			}
			if (rhs->type != ExpressionType::BOUND_COLUMN_REF) {
				throw InternalException("Error in Deliminator: expected a bound column reference");
			}
			auto &colref = (BoundColumnRefExpression &)*rhs;
			if (projection_map.find(colref.binding) != projection_map.end()) {
				// value on the right is a projection of removed DelimGet
				for (idx_t i = 0; i < decs->size(); i++) {
					if (decs->at(i)->Equals(cond.left.get())) {
						// the value on the left no longer needs to be a duplicate-eliminated column
						decs->erase(decs->begin() + i);
						break;
					}
				}
				// whether we applied an IS NOT NULL filter
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			}
		}
		// change type if there are no more duplicate-eliminated columns
		if (decs->empty()) {
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		}
	}
}

void DeliminatorPlanUpdater::VisitExpression(unique_ptr<Expression> *expression) {
	if (expr_map.find(expression->get()) != expr_map.end()) {
		*expression = expr_map[expression->get()]->Copy();
	} else {
		VisitExpressionChildren(**expression);
	}
}

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	for (auto &candidate : candidates) {
		DeliminatorPlanUpdater updater;
		if (RemoveCandidate(&op, candidate, updater)) {
			updater.VisitOperator(*op);
		}
	}
	return op;
}

void Deliminator::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                 vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(&child, candidates);
	}
	// search for projection/aggregate
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
	    op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	// followed by a join
	if (op->children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = *op->children[0];
	// with a DelimGet as a direct child (left or right)
	if (join.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET ||
	    join.children[1]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidates.push_back(op_ptr);
		return;
	}
	// or a filter followed by a DelimGet (left)
	if (join.children[0]->type == LogicalOperatorType::LOGICAL_FILTER &&
	    join.children[0]->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidates.push_back(op_ptr);
		return;
	}
	// filter followed by a DelimGet (right)
	if (join.children[1]->type == LogicalOperatorType::LOGICAL_FILTER &&
	    join.children[1]->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidates.push_back(op_ptr);
		return;
	}
}

static bool OperatorIsDelimGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	return false;
}

static bool ChildJoinTypeCanBeDeliminated(JoinType &join_type) {
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

bool Deliminator::RemoveCandidate(unique_ptr<LogicalOperator> *plan, unique_ptr<LogicalOperator> *candidate,
                                  DeliminatorPlanUpdater &updater) {
	auto &proj_or_agg = **candidate;
	auto &join = (LogicalComparisonJoin &)*proj_or_agg.children[0];
	if (!ChildJoinTypeCanBeDeliminated(join.join_type)) {
		return false;
	}

	// get the index (left or right) of the DelimGet side of the join
	idx_t delim_idx = OperatorIsDelimGet(*join.children[0]) ? 0 : 1;
	D_ASSERT(OperatorIsDelimGet(*join.children[delim_idx]));
	// get the filter (if any)
	LogicalFilter *filter = nullptr;
	if (join.children[delim_idx]->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter = (LogicalFilter *)join.children[delim_idx].get();
	}
	auto &delim_get = (LogicalDelimGet &)*(filter ? filter->children[0].get() : join.children[delim_idx].get());
	if (join.conditions.size() != delim_get.chunk_types.size()) {
		// joining with DelimGet adds new information
		return false;
	}
	// check if joining with the DelimGet is redundant, and collect relevant column information
	bool all_equality_conditions = true;
	vector<Expression *> nulls_are_not_equal_exprs;
	for (auto &cond : join.conditions) {
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(cond);
		auto delim_side = delim_idx == 0 ? cond.left.get() : cond.right.get();
		auto other_side = delim_idx == 0 ? cond.right.get() : cond.left.get();
		if (delim_side->type != ExpressionType::BOUND_COLUMN_REF) {
			// non-colref e.g. expression -(4, 1) in 4-i=j where i is from DelimGet
			// FIXME: might be possible to also eliminate these
			return false;
		}
		updater.expr_map[delim_side] = other_side;
		if (cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			nulls_are_not_equal_exprs.push_back(other_side);
		}
	}

	// removed DelimGet columns are assigned a new ColumnBinding by Projection/Aggregation, keep track here
	if (proj_or_agg.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &cb : proj_or_agg.GetColumnBindings()) {
			updater.projection_map[cb] = true;
			updater.reverse_proj_or_agg_map[cb] = proj_or_agg.expressions[cb.column_index].get();
			for (auto &expr : nulls_are_not_equal_exprs) {
				if (proj_or_agg.expressions[cb.column_index]->Equals(expr)) {
					updater.projection_map[cb] = false;
					break;
				}
			}
		}
	} else {
		auto &agg = (LogicalAggregate &)proj_or_agg;

		// Create a vector of all exprs in the agg
		vector<Expression *> all_agg_exprs;
		all_agg_exprs.reserve(agg.groups.size() + agg.expressions.size());
		for (auto &expr : agg.groups) {
			all_agg_exprs.push_back(expr.get());
		}
		for (auto &expr : agg.expressions) {
			all_agg_exprs.push_back(expr.get());
		}

		for (auto &cb : agg.GetColumnBindings()) {
			updater.projection_map[cb] = true;
			updater.reverse_proj_or_agg_map[cb] = all_agg_exprs[cb.column_index];
			for (auto &expr : nulls_are_not_equal_exprs) {
				if ((cb.table_index == agg.group_index && agg.groups[cb.column_index]->Equals(expr)) ||
				    (cb.table_index == agg.aggregate_index && agg.expressions[cb.column_index]->Equals(expr))) {
					updater.projection_map[cb] = false;
					break;
				}
			}
		}
	}

	if (!all_equality_conditions) {
		// we can get rid of an inequality join with a DelimGet, but only under specific circumstances
		if (!RemoveInequalityCandidate(plan, candidate, updater)) {
			return false;
		}
	}

	// make a filter if needed
	if (!nulls_are_not_equal_exprs.empty() || filter != nullptr) {
		auto filter_op = make_unique<LogicalFilter>();
		if (!nulls_are_not_equal_exprs.empty()) {
			// add an IS NOT NULL filter that was implicitly in JoinCondition::null_values_are_equal
			for (auto &expr : nulls_are_not_equal_exprs) {
				auto is_not_null_expr =
				    make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null_expr->children.push_back(expr->Copy());
				filter_op->expressions.push_back(move(is_not_null_expr));
			}
		}
		if (filter != nullptr) {
			for (auto &expr : filter->expressions) {
				filter_op->expressions.push_back(move(expr));
			}
		}
		filter_op->children.push_back(move(join.children[1 - delim_idx]));
		join.children[1 - delim_idx] = move(filter_op);
	}
	// temporarily save deleted operator so its expressions are still available
	updater.temp_ptr = move(proj_or_agg.children[0]);
	// replace the redundant join
	proj_or_agg.children[0] = move(join.children[1 - delim_idx]);
	return true;
}

static void GetDelimJoins(LogicalOperator &op, vector<LogicalOperator *> &delim_joins) {
	for (auto &child : op.children) {
		GetDelimJoins(*child, delim_joins);
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		delim_joins.push_back(&op);
	}
}

static bool HasChild(LogicalOperator *haystack, LogicalOperator *needle, idx_t &side) {
	if (haystack == needle) {
		return true;
	}
	for (idx_t i = 0; i < haystack->children.size(); i++) {
		auto &child = haystack->children[i];
		idx_t dummy_side;
		if (HasChild(child.get(), needle, dummy_side)) {
			side = i;
			return true;
		}
	}
	return false;
}

bool Deliminator::RemoveInequalityCandidate(unique_ptr<LogicalOperator> *plan, unique_ptr<LogicalOperator> *candidate,
                                            DeliminatorPlanUpdater &updater) {
	auto &proj_or_agg = **candidate;
	// first, we find a DelimJoin in "plan" that has only one DelimGet as a child, which is in "candidate"
	if (DelimGetCount(proj_or_agg) != 1) {
		// the candidate therefore must have only a single DelimGet in its children
		return false;
	}

	vector<LogicalOperator *> delim_joins;
	GetDelimJoins(**plan, delim_joins);

	LogicalOperator *parent = nullptr;
	idx_t parent_delim_get_side = 0;
	for (auto dj : delim_joins) {
		D_ASSERT(dj->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
		if (!HasChild(dj, &proj_or_agg, parent_delim_get_side)) {
			continue;
		}
		// we found a parent DelimJoin
		if (DelimGetCount(*dj) != 1) {
			// it has more than one DelimGet children
			continue;
		}

		// we can only remove inequality join with a DelimGet if the parent DelimJoin has one of these join types
		auto &delim_join = (LogicalDelimJoin &)*dj;
		if (!InequalityDelimJoinCanBeEliminated(delim_join.join_type)) {
			continue;
		}

		parent = dj;
		break;
	}
	if (!parent) {
		return false;
	}

	// we found the parent delim join, and we may be able to remove the child DelimGet join
	// but we need to make sure that their conditions refer to exactly the same columns
	auto &parent_delim_join = (LogicalDelimJoin &)*parent;
	auto &join = (LogicalComparisonJoin &)*proj_or_agg.children[0];
	if (parent_delim_join.conditions.size() != join.conditions.size()) {
		// different number of conditions, can't replace
		return false;
	}

	// we can only do this optimization under the following conditions:
	// 1. all join expressions coming from the DelimGet side are colrefs
	// 2. these expressions refer to colrefs coming from the proj/agg on top of the child DelimGet join
	// 3. the expression (before it was proj/agg) can be found in the conditions of the child DelimGet join
	for (auto &parent_cond : parent_delim_join.conditions) {
		auto &parent_expr = parent_delim_get_side == 0 ? parent_cond.left : parent_cond.right;
		if (parent_expr->type != ExpressionType::BOUND_COLUMN_REF) {
			// can only deal with colrefs
			return false;
		}
		auto &parent_colref = (BoundColumnRefExpression &)*parent_expr;
		auto it = updater.reverse_proj_or_agg_map.find(parent_colref.binding);
		if (it == updater.reverse_proj_or_agg_map.end()) {
			// refers to a column that was not in the child DelimGet join
			return false;
		}
		// try to find the corresponding child condition
		// TODO: can be more flexible - allow CAST
		auto child_expr = it->second;
		bool found = false;
		for (auto &child_cond : join.conditions) {
			if (child_cond.left->Equals(child_expr) || child_cond.right->Equals(child_expr)) {
				found = true;
				break;
			}
		}
		if (!found) {
			// could not find the mapped expression in the child condition expressions
			return false;
		}
	}

	// TODO: we cannot perform the optimization here because our pure inequality joins don't implement
	//  JoinType::SINGLE yet
	if (parent_delim_join.join_type == JoinType::SINGLE) {
		bool has_one_equality = false;
		for (auto &cond : join.conditions) {
			has_one_equality = has_one_equality || IsEqualityJoinCondition(cond);
		}
		if (!has_one_equality) {
			return false;
		}
	}

	// we are now sure that we can remove the child DelimGet join, so we basically do the same loop as above
	// this time without checks because we already did them, and replace the expressions
	for (auto &parent_cond : parent_delim_join.conditions) {
		auto &parent_expr = parent_delim_get_side == 0 ? parent_cond.left : parent_cond.right;
		auto &parent_colref = (BoundColumnRefExpression &)*parent_expr;
		auto it = updater.reverse_proj_or_agg_map.find(parent_colref.binding);
		auto child_expr = it->second;
		for (auto &child_cond : join.conditions) {
			if (!child_cond.left->Equals(child_expr) && !child_cond.right->Equals(child_expr)) {
				continue;
			}
			parent_expr =
			    make_unique<BoundColumnRefExpression>(parent_expr->alias, parent_expr->return_type, it->first);
			parent_cond.comparison =
			    parent_delim_get_side == 0 ? child_cond.comparison : FlipComparisionExpression(child_cond.comparison);
			break;
		}
	}

	// no longer needs to be a delim join
	parent_delim_join.duplicate_eliminated_columns.clear();
	parent_delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;

	return true;
}

} // namespace duckdb
