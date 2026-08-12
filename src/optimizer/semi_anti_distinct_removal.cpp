#include "duckdb/optimizer/semi_anti_distinct_removal.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

//! Returns whether this join type only probes one of its children for existence,
//! and if so which child that is.
static bool GetExistenceOnlyChild(JoinType type, idx_t &result) {
	switch (type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// these emit rows from the left and only ask whether the right matches
		result = 1;
		return true;
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		// the optimizer swapped the children, so the left side is the one that
		// is only probed for existence
		result = 0;
		return true;
	default:
		return false;
	}
}

//! Walks down from an existence-only join child looking for a DISTINCT that can be
//! removed. A projection maps every input row to exactly one output row, so it
//! cannot turn a duplicate-insensitive consumer into a sensitive one and we can
//! keep descending through it.
static optional_ptr<unique_ptr<LogicalOperator>> FindRemovableDistinct(unique_ptr<LogicalOperator> &start) {
	auto current = &start;
	while (true) {
		auto &op = **current;
		if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			auto &distinct = op.Cast<LogicalDistinct>();
			// DISTINCT ON keeps one row per group and an ORDER BY decides which one,
			// so removing it can change which values reach the join condition. Only a
			// plain DISTINCT is a pure de-duplication.
			if (distinct.distinct_type != DistinctType::DISTINCT) {
				return nullptr;
			}
			return current;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && op.children.size() == 1) {
			current = &op.children[0];
			continue;
		}
		return nullptr;
	}
}

bool SemiAntiDistinctRemoval::CanOptimize(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	idx_t child_idx;
	if (!GetExistenceOnlyChild(join.join_type, child_idx)) {
		return false;
	}
	return FindRemovableDistinct(op.children[child_idx]) != nullptr;
}

unique_ptr<LogicalOperator> SemiAntiDistinctRemoval::Optimize(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		idx_t child_idx;
		if (GetExistenceOnlyChild(join.join_type, child_idx)) {
			auto slot = FindRemovableDistinct(op->children[child_idx]);
			if (slot) {
				// LogicalDistinct forwards both the column bindings and the types of
				// its child, so it can be spliced out without rebinding anything above
				auto distinct = std::move(*slot);
				*slot = std::move(distinct->children[0]);
			}
		}
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
