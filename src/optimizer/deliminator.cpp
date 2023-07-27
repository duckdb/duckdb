#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/remove_duplicate_groups.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

struct DelimCandidate {
public:
	explicit DelimCandidate(unique_ptr<LogicalOperator> &op, LogicalComparisonJoin &delim_join)
	    : op(op), delim_join(delim_join), delim_get_count(0) {
	}

public:
	unique_ptr<LogicalOperator> &op;
	LogicalComparisonJoin &delim_join;
	vector<reference<unique_ptr<LogicalOperator>>> joins;
	idx_t delim_get_count;
};

static bool IsEqualityJoinCondition(const JoinCondition &cond) {
	switch (cond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	root = op;

	vector<DelimCandidate> candidates;
	FindCandidates(op, candidates);

	for (auto &candidate : candidates) {
		auto &delim_join = candidate.delim_join;

		bool all_removed = true;
		bool all_equality_conditions = true;
		for (auto &join : candidate.joins) {
			all_removed =
			    RemoveJoinWithDelimGet(delim_join, candidate.delim_get_count, join, all_equality_conditions) &&
			    all_removed;
		}

		// Change type if there are no more duplicate-eliminated columns
		if (candidate.joins.size() == candidate.delim_get_count && all_removed) {
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
			delim_join.duplicate_eliminated_columns.clear();
			if (all_equality_conditions) {
				for (auto &cond : delim_join.conditions) {
					if (IsEqualityJoinCondition(cond)) {
						cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
					}
				}
			}
		}
	}

	return op;
}

void Deliminator::FindCandidates(unique_ptr<LogicalOperator> &op, vector<DelimCandidate> &candidates) {
	// Search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(child, candidates);
	}

	if (op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}

	candidates.emplace_back(op, op->Cast<LogicalComparisonJoin>());
	auto &candidate = candidates.back();

	// DelimGets are in the RHS
	FindJoinWithDelimGet(op->children[1], candidate);
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

void Deliminator::FindJoinWithDelimGet(unique_ptr<LogicalOperator> &op, DelimCandidate &candidate) {
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		FindJoinWithDelimGet(op->children[0], candidate);
	} else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidate.delim_get_count++;
	} else {
		for (auto &child : op->children) {
			FindJoinWithDelimGet(child, candidate);
		}
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    (OperatorIsDelimGet(*op->children[0]) || OperatorIsDelimGet(*op->children[1]))) {
		candidate.joins.emplace_back(op);
	}
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

bool Deliminator::RemoveJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
                                         unique_ptr<LogicalOperator> &join, bool &all_equality_conditions) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (!ChildJoinTypeCanBeDeliminated(comparison_join.join_type)) {
		return false;
	}

	// Get the index (left or right) of the DelimGet side of the join
	const idx_t delim_idx = OperatorIsDelimGet(*join->children[0]) ? 0 : 1;

	// Get the filter (if any)
	optional_ptr<LogicalFilter> filter;
	vector<unique_ptr<Expression>> filter_expressions;
	if (join->children[delim_idx]->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter = &join->children[delim_idx]->Cast<LogicalFilter>();
		for (auto &expr : filter->expressions) {
			filter_expressions.emplace_back(expr->Copy());
		}
	}

	auto &delim_get = (filter ? filter->children[0] : join->children[delim_idx])->Cast<LogicalDelimGet>();
	if (comparison_join.conditions.size() != delim_get.chunk_types.size()) {
		return false; // Joining with DelimGet adds new information
	}

	// Check if joining with the DelimGet is redundant, and collect relevant column information
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (auto &cond : comparison_join.conditions) {
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(cond);
		auto &delim_side = delim_idx == 0 ? *cond.left : *cond.right;
		auto &other_side = delim_idx == 0 ? *cond.right : *cond.left;
		if (delim_side.type != ExpressionType::BOUND_COLUMN_REF ||
		    other_side.type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &delim_colref = delim_side.Cast<BoundColumnRefExpression>();
		auto &other_colref = other_side.Cast<BoundColumnRefExpression>();
		replacement_bindings.emplace_back(delim_colref.binding, other_colref.binding);

		if (cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			auto is_not_null_expr =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null_expr->children.push_back(other_side.Copy());
			filter_expressions.push_back(std::move(is_not_null_expr));
		}
	}

	if (!all_equality_conditions &&
	    !RemoveInequalityJoinWithDelimGet(delim_join, delim_get_count, join, replacement_bindings)) {
		return false;
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[1 - delim_idx]);
	if (!filter_expressions.empty()) { // Create filter if necessary
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}

	join = std::move(replacement_op);

	// TODO: Maybe go from delim join instead to save work
	replacer.VisitOperator(*root);
	return true;
}

static bool InequalityDelimJoinCanBeEliminated(JoinType &join_type) {
	return join_type == JoinType::ANTI || join_type == JoinType::MARK || join_type == JoinType::SEMI ||
	       join_type == JoinType::SINGLE;
}

bool FindAndReplaceBindings(vector<ColumnBinding> &traced_bindings, const vector<unique_ptr<Expression>> &expressions,
                            const vector<ColumnBinding> &current_bindings) {
	for (auto &binding : traced_bindings) {
		idx_t current_idx;
		for (current_idx = 0; current_idx < expressions.size(); current_idx++) {
			if (binding == current_bindings[current_idx]) {
				break;
			}
		}

		if (current_idx == expressions.size() || expressions[current_idx]->type != ExpressionType::BOUND_COLUMN_REF) {
			return false; // Didn't find / can't deal with non-colref
		}

		auto &colref = expressions[current_idx]->Cast<BoundColumnRefExpression>();
		binding = colref.binding;
	}
	return true;
}

bool Deliminator::RemoveInequalityJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
                                                   unique_ptr<LogicalOperator> &join,
                                                   const vector<ReplacementBinding> &replacement_bindings) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	auto &delim_conditions = delim_join.conditions;
	const auto &join_conditions = comparison_join.conditions;
	if (delim_get_count != 1 || !InequalityDelimJoinCanBeEliminated(delim_join.join_type) ||
	    delim_conditions.size() != join_conditions.size()) {
		return false;
	}

	// TODO: we cannot perform the optimization here because our pure inequality joins don't implement
	//  JoinType::SINGLE yet
	if (delim_join.join_type == JoinType::SINGLE) {
		bool has_one_equality = false;
		for (auto &cond : join_conditions) {
			has_one_equality = has_one_equality || IsEqualityJoinCondition(cond);
		}
		if (!has_one_equality) {
			return false;
		}
	}

	// We only support colref's
	vector<ColumnBinding> traced_bindings;
	for (const auto &cond : delim_conditions) {
		if (cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = cond.right->Cast<BoundColumnRefExpression>();
		traced_bindings.emplace_back(colref.binding);
	}

	// Now we trace down the bindings to the join (for now, we only trace it through a few operators)
	reference<LogicalOperator> current_op = *delim_join.children[1];
	while (&current_op.get() != join.get()) {
		if (current_op.get().children.size() != 1) {
			return false;
		}

		switch (current_op.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			FindAndReplaceBindings(traced_bindings, current_op.get().expressions, current_op.get().GetColumnBindings());
			break;
		case LogicalOperatorType::LOGICAL_FILTER:
			break; // Doesn't change bindings
		default:
			return false;
		}
		current_op = *current_op.get().children[0];
	}

	// Get the index (left or right) of the DelimGet side of the join
	const idx_t delim_idx = OperatorIsDelimGet(*join->children[0]) ? 0 : 1;

	bool found_all = true;
	for (idx_t cond_idx = 0; cond_idx < delim_conditions.size(); cond_idx++) {
		auto &delim_condition = delim_conditions[cond_idx];
		const auto &traced_binding = traced_bindings[cond_idx];

		bool found = false;
		for (auto &join_condition : join_conditions) {
			auto &delim_side = delim_idx == 0 ? *join_condition.left : *join_condition.right;
			auto &colref = delim_side.Cast<BoundColumnRefExpression>();
			if (colref.binding == traced_binding) {
				delim_condition.comparison = FlipComparisonExpression(join_condition.comparison);
				found = true;
				break;
			}
		}
		found_all = found_all && found;
	}

	return found_all;
}

} // namespace duckdb
