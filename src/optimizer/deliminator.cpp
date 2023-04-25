#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
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
	auto &replace_bindings = replacer.replace_bindings;
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
		replace_bindings.emplace_back(delim_colref.binding, other_colref.binding);

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

	// We might be grouping by duplicate groups now that we've replaced column bindings
	RemoveDuplicateGroups(*root, replacer);

	// TODO: Maybe go from delim join instead to save work
	replacer.VisitOperator(*root);
	return true;
}

void Deliminator::RemoveDuplicateGroups(LogicalOperator &op, const ColumnBindingReplacer &replacer) {
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		for (auto &child : op.children) {
			RemoveDuplicateGroups(*child, replacer);
		}
		return;
	}

	bool found_corresponding_agg = false;
	auto &agg = op.Cast<LogicalAggregate>();
	auto &groups = agg.groups;
	for (const auto &group : groups) {
		if (group->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}

		auto &colref = group->Cast<BoundColumnRefExpression>();
		for (const auto &replace_binding : replacer.replace_bindings) {
			if (colref.binding == replace_binding.old_binding || colref.binding == replace_binding.new_binding) {
				found_corresponding_agg = true;
				break;
			}
		}

		if (found_corresponding_agg) {
			break;
		}
	}

	if (!found_corresponding_agg) { // We didn't find the corresponding aggregate, recurse
		for (auto &child : op.children) {
			RemoveDuplicateGroups(*child, replacer);
		}
	}

	// We found the corresponding aggregate, check if it has duplicates (both the old and new binding)
	vector<pair<idx_t, idx_t>> duplicates;
	for (const auto &replace_binding : replacer.replace_bindings) {
		D_ASSERT(replace_binding.old_binding != replace_binding.new_binding);
		idx_t old_idx = DConstants::INVALID_INDEX;
		idx_t new_idx = DConstants::INVALID_INDEX;
		for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
			const auto &group = groups[group_idx];
			if (group->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			auto &colref = group->Cast<BoundColumnRefExpression>();
			if (colref.binding == replace_binding.old_binding) {
				old_idx = group_idx;
			} else if (colref.binding == replace_binding.new_binding) {
				new_idx = group_idx;
			}
		}

		if (old_idx == DConstants::INVALID_INDEX || new_idx == DConstants::INVALID_INDEX) {
			continue;
		}

		// Groups contain both the old and new binding - duplicate!
		duplicates.emplace_back(old_idx, new_idx);
	}

	if (duplicates.empty()) {
		return;
	}

	// Sort duplicates by max of the old/new group indices first, because we want to remove stuff from the back
	sort(duplicates.begin(), duplicates.end(), [](const pair<idx_t, idx_t> &lhs, const pair<idx_t, idx_t> &rhs) {
		return MaxValue(lhs.first, lhs.second) > MaxValue(rhs.first, rhs.second);
	});

	// Now we want to remove the duplicates, but this alters the column bindings coming out of the aggregate,
	// so we keep track of how they shift and do another round of column binding replacements
	column_binding_map_t<ColumnBinding> group_binding_map;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		group_binding_map.emplace(ColumnBinding(agg.group_index, group_idx), ColumnBinding(agg.group_index, group_idx));
	}

	for (idx_t duplicate_idx = 0; duplicate_idx < duplicates.size(); duplicate_idx++) {
		const auto &duplicate = duplicates[duplicate_idx];
		const auto &old_idx = duplicate.first;
		const auto &new_idx = duplicate.second;

		auto remaining_idx = MinValue(old_idx, new_idx);
		auto removed_idx = MaxValue(old_idx, new_idx);
		groups.erase(groups.begin() + removed_idx);

		// Update mapping
		auto it = group_binding_map.find(ColumnBinding(agg.group_index, removed_idx));
		D_ASSERT(it != group_binding_map.end());
		it->second.column_index = remaining_idx;

		for (auto &map_entry : group_binding_map) {
			auto &new_binding = map_entry.second;
			if (new_binding.column_index > removed_idx) {
				new_binding.column_index--;
			}
		}
	}

	ColumnBindingReplacer agg_replacer;
	auto &replace_bindings = agg_replacer.replace_bindings;
	for (auto &map_entry : group_binding_map) {
		const auto &old_binding = map_entry.first;
		const auto &new_binding = map_entry.second;
		if (old_binding != new_binding) {
			replace_bindings.emplace_back(old_binding, new_binding);
		}
	}

	if (!replace_bindings.empty()) {
		agg_replacer.VisitOperator(*root);
	}
}

} // namespace duckdb
