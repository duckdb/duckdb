#include "duckdb/optimizer/redundant_distinct_remover.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

RedundantDistinctRemover::RedundantDistinctRemover(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> RedundantDistinctRemover::Optimize(unique_ptr<LogicalOperator> op) {
	return Visit(std::move(op), false);
}

//! A projection emits one row per input row, so when its expressions are deterministic the set of values
//! reaching an ancestor that removes duplicates does not depend on duplicates below it. A volatile
//! expression breaks that, because it is evaluated once per row and feeding it fewer rows changes what it
//! produces.
static bool ProjectionPreservesDeduplication(const LogicalOperator &op) {
	for (auto &expr : op.expressions) {
		if (expr->IsVolatile()) {
			return false;
		}
	}
	return true;
}

unique_ptr<LogicalOperator> RedundantDistinctRemover::Visit(unique_ptr<LogicalOperator> op, bool deduplicated) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op->Cast<LogicalDistinct>();
		// DISTINCT ON keeps one particular row per group rather than collapsing exact duplicates, so it
		// neither is redundant nor makes what is below it redundant.
		if (distinct.distinct_type != DistinctType::DISTINCT) {
			break;
		}
		if (deduplicated) {
			return Visit(std::move(op->children[0]), true);
		}
		// this operator discards the duplicates of everything below it
		op->children[0] = Visit(std::move(op->children[0]), true);
		return op;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		op->children[0] = Visit(std::move(op->children[0]), deduplicated && ProjectionPreservesDeduplication(*op));
		return op;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = op->Cast<LogicalSetOperation>();
		// The ALL variants are defined on multisets, so a duplicate coming out of a branch is part of the
		// result and the branch DISTINCT is doing real work.
		auto propagate = deduplicated && !setop.setop_all;
		for (auto &child : op->children) {
			child = Visit(std::move(child), propagate);
		}
		return op;
	}
	default:
		break;
	}
	// Any other operator may turn the rows below it into something whose duplicates do survive, so stop
	// carrying the property across it.
	for (auto &child : op->children) {
		child = Visit(std::move(child), false);
	}
	return op;
}

} // namespace duckdb
