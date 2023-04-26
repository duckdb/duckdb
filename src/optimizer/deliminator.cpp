#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/remove_duplicate_groups.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

struct DelimCandidate {
public:
	explicit DelimCandidate(LogicalDelimJoin &delim_join) : delim_join(delim_join), delim_get_count(0) {
	}

public:
	LogicalDelimJoin &delim_join;
	vector<reference<unique_ptr<LogicalOperator>>> joins;
	idx_t delim_get_count;
};

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	root = op;

	vector<DelimCandidate> candidates;
	FindCandidates(op, candidates);

	for (auto &candidate : candidates) {
		auto &delim_join = candidate.delim_join;

		bool all_removed = true;
		for (auto &join : candidate.joins) {
			all_removed = RemoveJoinWithDelimGet(join) && all_removed;
		}

		// TODO: special handling for inequality joins!

		// Change type if there are no more duplicate-eliminated columns
		if (candidate.joins.size() == candidate.delim_get_count && all_removed) {
			delim_join.duplicate_eliminated_columns.clear();
			for (auto &cond : delim_join.conditions) {
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			}
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
			// Sub-plans with DelimGets are not re-orderable (yet), however, we removed all DelimGet of this DelimJoin
			// The DelimGets are on the RHS of the DelimJoin, so we can call the JoinOrderOptimizer on the RHS now
			JoinOrderOptimizer optimizer(context);
			delim_join.children[1] = optimizer.Optimize(std::move(delim_join.children[1]));
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

	candidates.emplace_back(op->Cast<LogicalDelimJoin>());
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

bool Deliminator::RemoveJoinWithDelimGet(unique_ptr<LogicalOperator> &join) {
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
		filter = (LogicalFilter *)join->children[delim_idx].get();
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
		if (!IsEqualityJoinCondition(cond)) {
			return false;
		}
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

} // namespace duckdb
